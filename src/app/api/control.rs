use super::responses::{encode_error, encode_success};
use crate::api::schema::{
    JobCondition, ManagedExit, ManagedJobParams, ManagedJobPhase, ManagedLaunchParams,
    ManagedTerminalInfo, PaneTarget, ResponseResult,
};
use crate::app::App;

impl App {
    pub(super) fn handle_managed_job_launch(
        &mut self,
        id: String,
        params: ManagedLaunchParams,
    ) -> String {
        if self.managed_jobs.jobs.len() >= 256 {
            return encode_error(id, "job_limit", "managed job receipt capacity reached");
        }
        if !crate::platform::cooperative_control_supported() {
            return encode_error(
                id,
                "unsupported_platform",
                "managed job peer authentication is unavailable on this platform",
            );
        }
        if let Err(code) = self.validate_terminal_condition(&params.parent) {
            return encode_error(
                id,
                code,
                "parent runtime or membership changed; no job launched",
            );
        }
        if params.argv.is_empty()
            || params.argv.len() > 64
            || params.argv.iter().any(|arg| arg.contains('\0'))
            || params.argv.iter().map(String::len).sum::<usize>() > 8192
            || !std::path::Path::new(&params.argv[0]).is_absolute()
        {
            return encode_error(
                id,
                "invalid_managed_argv",
                "managed argv requires an absolute executable and at most 64 arguments/8192 bytes",
            );
        }
        let cwd = std::path::PathBuf::from(params.cwd);
        if !cwd.is_absolute() || !cwd.is_dir() {
            return encode_error(
                id,
                "invalid_managed_cwd",
                "managed cwd must be an existing absolute directory",
            );
        }
        if params.env.len() > 128
            || params
                .env
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 65536
        {
            return encode_error(
                id,
                "managed_env_too_large",
                "managed environment exceeds limit",
            );
        }
        let mut extra = match super::env::normalize_launch_env(params.env) {
            Ok(env) => env,
            Err((code, message)) => return encode_error(id, &code, message),
        };
        let Some((parent_ws, parent_pane)) =
            self.parse_current_public_pane_id(&params.parent.pane_id)
        else {
            return encode_error(id, "terminal_not_found", "parent pane missing");
        };
        if self.state.workspaces[parent_ws]
            .find_tab_index_for_pane(parent_pane)
            .is_some_and(|tab| self.state.workspaces[parent_ws].tabs[tab].zoomed)
        {
            return encode_error(
                id,
                "managed_parent_zoomed",
                "managed split requires an unzoomed parent; no launch or focus change performed",
            );
        }
        let job_id = match crate::app::control::random_identity("job_") {
            Ok(id) => id,
            Err(_) => {
                return encode_error(
                    id,
                    "terminal_identity_unavailable",
                    "OS entropy unavailable; no job launched",
                )
            }
        };
        let launch_id = match crate::app::control::random_identity("runtime_") {
            Ok(id) => id,
            Err(_) => {
                return encode_error(
                    id,
                    "terminal_identity_unavailable",
                    "OS entropy unavailable; no job launched",
                )
            }
        };
        let capability = match crate::app::control::random_identity("enroll_") {
            Ok(id) => id,
            Err(_) => {
                return encode_error(
                    id,
                    "terminal_identity_unavailable",
                    "OS entropy unavailable; no job launched",
                )
            }
        };
        extra.retain(|(key, _)| !key.starts_with("HERDR_CONTROL_"));
        extra.extend([
            ("HERDR_CONTROL_CAPABILITY".into(), capability.clone()),
            ("HERDR_CONTROL_VERSION".into(), "1".into()),
            (
                "HERDR_CONTROL_SOCKET".into(),
                crate::api::socket_path().display().to_string(),
            ),
        ]);
        let Some((ws_idx, parent)) = self.parse_current_public_pane_id(&params.parent.pane_id)
        else {
            return encode_error(id, "terminal_not_found", "parent pane is missing");
        };
        let direction = match params.direction {
            crate::api::schema::SplitDirection::Right => ratatui::layout::Direction::Horizontal,
            crate::api::schema::SplitDirection::Down => ratatui::layout::Direction::Vertical,
        };
        let (rows, cols) = self
            .state
            .new_pane_size(crate::ui::NewPanePlacement::Split {
                ws_idx,
                target: parent,
                direction,
                ratio: 0.5,
            });
        let result = self.state.workspaces[ws_idx].split_pane_managed_argv_command(
            parent,
            direction,
            rows,
            cols,
            cwd,
            &params.argv,
            extra,
            self.state.pane_scrollback_limit_bytes,
            self.state.host_terminal_theme,
            self.state.host_terminal_appearance,
            &job_id,
            &launch_id,
        );
        let new_pane = match result {
            Some(Ok((_, pane))) => pane,
            Some(Err(_)) => {
                return encode_error(
                    id,
                    "managed_launch_failed",
                    "direct managed command could not be launched",
                )
            }
            None => return encode_error(id, "terminal_not_found", "parent pane is missing"),
        };
        let pane_id = new_pane.pane_id;
        let mut terminal = new_pane.terminal;
        if let Some(label) = params.label {
            terminal.set_manual_label(label);
        }
        let anchor = new_pane.runtime.managed_process_anchor();
        self.terminal_runtimes
            .insert(terminal.id.clone(), new_pane.runtime);
        self.state.terminals.insert(terminal.id.clone(), terminal);
        self.state.remove_alias_shadowed_by_new_pane(pane_id);
        let Some(public) = self.public_pane_id(ws_idx, pane_id) else {
            return encode_error(
                id,
                "managed_launch_failed",
                "owned pane membership unavailable",
            );
        };
        let terminal = match self.current_terminal_condition(&public) {
            Ok(condition) => condition,
            Err(code) => return encode_error(id, code, "owned runtime identity unavailable"),
        };
        let info = ManagedTerminalInfo {
            condition: JobCondition {
                terminal,
                job_id: job_id.clone(),
            },
            phase: if anchor.is_some() {
                ManagedJobPhase::AwaitingEnrollment
            } else {
                ManagedJobPhase::EnrollmentFailed
            },
            agent: None,
            exit: None,
            diagnostic: anchor.is_none().then(|| {
                "original_wrapper_identity_unavailable; release or close the owned resource".into()
            }),
            native_scope_id: None,
            native_request_ids: Vec::new(),
            busy: false,
        };
        self.managed_jobs.jobs.insert(
            job_id,
            crate::app::control::ManagedJob {
                info: info.clone(),
                enrollment: anchor.map(|anchor| crate::app::control::Enrollment {
                    capability,
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
                    anchor: Some(anchor),
                }),
                route: None,
                anchor: None,
                operations: std::collections::HashMap::new(),
                absent_since: None,
            },
        );
        self.schedule_session_save();
        encode_success(
            id,
            ResponseResult::ManagedTerminal {
                job: Box::new(info),
            },
        )
    }

    pub(crate) fn has_managed_job_runtime(&self) -> bool {
        self.terminal_runtimes
            .values()
            .any(|runtime| self.managed_jobs.has_runtime(runtime.launch_id()))
    }

    pub(super) fn refresh_managed_jobs(&mut self) {
        if self.managed_jobs.jobs.is_empty() {
            return;
        }
        let live = self
            .terminal_runtimes
            .values()
            .filter_map(|runtime| runtime.launch_id().map(str::to_string))
            .collect();
        self.managed_jobs
            .retire_absent(&live, std::time::Instant::now());
        let states: Vec<_> = self
            .managed_jobs
            .jobs
            .iter()
            .map(|(id, job)| {
                (
                    id.clone(),
                    job.info.condition.clone(),
                    job.info.phase.clone(),
                )
            })
            .collect();
        for (id, condition, phase) in states {
            if matches!(
                phase,
                ManagedJobPhase::Released
                    | ManagedJobPhase::Moved
                    | ManagedJobPhase::Replaced
                    | ManagedJobPhase::Exited
                    | ManagedJobPhase::Closed
            ) {
                continue;
            }
            let identity = self.validate_terminal_condition(&condition.terminal);
            let job = self
                .managed_jobs
                .jobs
                .get_mut(&id)
                .expect("existing managed job");
            job.expire();
            if identity.is_err() {
                job.revoke("managed runtime or membership changed");
                job.info.phase = if live.contains(&condition.terminal.launch_id) {
                    ManagedJobPhase::Moved
                } else {
                    ManagedJobPhase::Replaced
                };
                job.info.diagnostic = Some(
                    "managed runtime or membership changed; original authority revoked".into(),
                );
            } else if job.route.as_ref().is_some_and(|route| {
                !route
                    .connection
                    .alive
                    .load(std::sync::atomic::Ordering::Acquire)
            }) {
                let diagnostic = job
                    .route
                    .as_ref()
                    .and_then(|route| {
                        route
                            .connection
                            .diagnostic
                            .lock()
                            .ok()
                            .and_then(|diagnostic| diagnostic.clone())
                    })
                    .unwrap_or_else(|| "control stream disconnected".into());
                job.revoke("control stream disconnected");
                job.info.diagnostic = Some(format!("{diagnostic}; release or relaunch required"));
            } else if let Some(enrollment) = job.enrollment.as_ref() {
                let diagnostic = if enrollment.anchor.is_none() {
                    Some("original_wrapper_identity_unavailable; release or close the owned resource")
                } else if enrollment.deadline <= std::time::Instant::now() {
                    Some("enrollment_expired; release or close the owned resource")
                } else {
                    None
                };
                if let Some(diagnostic) = diagnostic {
                    job.revoke(diagnostic);
                    job.info.phase = ManagedJobPhase::EnrollmentFailed;
                    job.info.diagnostic = Some(diagnostic.into());
                }
            }
        }
    }

    pub(super) fn validate_managed_job(
        &self,
        condition: &JobCondition,
    ) -> Result<(), &'static str> {
        let info = self.managed_jobs.receipt(condition)?;
        match info.phase {
            ManagedJobPhase::Released => return Err("job_released"),
            ManagedJobPhase::Moved => return Err("job_moved"),
            ManagedJobPhase::Replaced => return Err("job_replaced"),
            ManagedJobPhase::Closed => return Err("job_closed"),
            _ => {}
        }
        self.validate_terminal_condition(&condition.terminal)
    }

    pub(super) fn handle_managed_job_release(
        &mut self,
        id: String,
        params: ManagedJobParams,
    ) -> String {
        if let Err(code) = self.validate_managed_job(&params.condition) {
            return encode_error(
                id,
                code,
                "managed ownership or runtime changed; no release performed",
            );
        }
        let job = self
            .managed_jobs
            .jobs
            .get_mut(&params.condition.job_id)
            .expect("validated job");
        job.revoke("job released; already admitted native work may still complete");
        job.info.phase = ManagedJobPhase::Released;
        job.info.native_scope_id = None;
        job.info.native_request_ids.clear();
        job.info.busy = false;
        encode_success(
            id,
            ResponseResult::ManagedTerminal {
                job: Box::new(job.info.clone()),
            },
        )
    }

    pub(super) fn handle_managed_job_close(
        &mut self,
        id: String,
        params: ManagedJobParams,
    ) -> String {
        if let Err(code) = self.validate_managed_job(&params.condition) {
            return encode_error(
                id,
                code,
                "managed ownership or runtime changed; no close performed",
            );
        }
        if let Err(response) = self.close_pane(
            id.clone(),
            &PaneTarget {
                pane_id: params.condition.terminal.pane_id,
            },
        ) {
            if let Some(job) = self.managed_jobs.jobs.get_mut(&params.condition.job_id) {
                job.info.diagnostic = Some(response.clone());
            }
            return response;
        }
        let job = self
            .managed_jobs
            .jobs
            .get_mut(&params.condition.job_id)
            .expect("validated job");
        job.revoke("job closed");
        job.info.phase = ManagedJobPhase::Closed;
        job.info.diagnostic = None;
        encode_success(
            id,
            ResponseResult::ManagedTerminal {
                job: Box::new(job.info.clone()),
            },
        )
    }

    pub(super) fn handle_managed_job_rename(
        &mut self,
        id: String,
        params: crate::api::schema::ManagedJobRenameParams,
    ) -> String {
        if let Err(code) = self.validate_managed_job(&params.condition) {
            return encode_error(
                id,
                code,
                "managed ownership or runtime changed; no rename performed",
            );
        }
        self.handle_pane_rename(
            id,
            crate::api::schema::PaneRenameParams {
                pane_id: params.condition.terminal.pane_id,
                label: params.label,
            },
        )
    }

    pub(super) fn handle_managed_job_capabilities(&self, id: String) -> String {
        let supported = crate::platform::cooperative_control_supported();
        encode_success(
            id,
            ResponseResult::ManagedTerminalCapabilities {
                job_ownership: supported,
                cooperative_control: supported,
                version: 1,
                max_frame_bytes: 16384,
                required_sdk_features: vec![
                    "input_request_lifecycle_v1".into(),
                    "message_admission_v1".into(),
                    "external_request_correlation_v1".into(),
                ],
            },
        )
    }

    pub(super) fn handle_managed_job_get(
        &mut self,
        id: String,
        params: ManagedJobParams,
    ) -> String {
        match self.managed_jobs.receipt(&params.condition) {
            Ok(info) => encode_success(
                id,
                ResponseResult::ManagedTerminal {
                    job: Box::new(info),
                },
            ),
            Err(code) => encode_error(id, code, "managed job condition is unknown or mismatched"),
        }
    }

    pub(super) fn handle_managed_job_exit(
        &mut self,
        job_id: String,
        _pane_id: crate::layout::PaneId,
        code: Option<u32>,
        exit_reason: crate::platform::ChildExitReason,
    ) {
        let Some(job) = self.managed_jobs.jobs.get(&job_id) else {
            return;
        };
        if job.info.exit.is_some() {
            return;
        }
        let condition = job.info.condition.clone();
        let retained = matches!(
            job.info.phase,
            ManagedJobPhase::Released
                | ManagedJobPhase::Closed
                | ManagedJobPhase::Moved
                | ManagedJobPhase::Replaced
        );
        let identity = self.validate_terminal_condition(&condition.terminal);
        let Some(job) = self.managed_jobs.jobs.get_mut(&job_id) else {
            return;
        };
        job.info.exit = Some(ManagedExit {
            code,
            reason: format!("{exit_reason:?}").to_lowercase(),
        });
        job.revoke("wrapper exited");
        if retained {
            job.info.diagnostic =
                Some("ownership revoked; old wrapper exit did not close a resource".into());
            return;
        }
        if let Err(code) = identity {
            job.info.phase = ManagedJobPhase::Replaced;
            job.info.diagnostic = Some(format!(
                "{code}; old wrapper exit did not close a changed resource"
            ));
            return;
        }
        job.info.phase = ManagedJobPhase::Exited;
        if let Err(response) = self.close_pane(
            "managed-exit".into(),
            &PaneTarget {
                pane_id: condition.terminal.pane_id,
            },
        ) {
            if let Some(job) = self.managed_jobs.jobs.get_mut(&job_id) {
                job.info.diagnostic = Some(response);
            }
        } else if let Some(job) = self.managed_jobs.jobs.get_mut(&job_id) {
            job.info.diagnostic = None;
        }
    }
}

#[cfg(test)]
mod tests;
