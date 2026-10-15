use crate::api::schema::{JobCondition, ManagedTerminalInfo};
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct ManagedJobs {
    pub jobs: HashMap<String, ManagedJob>,
}
pub(crate) struct ManagedJob {
    pub info: ManagedTerminalInfo,
    pub enrollment: Option<Enrollment>,
    pub route: Option<Route>,
    pub anchor: Option<crate::platform::ControlProcessAnchor>,
    pub operations: HashMap<String, Operation>,
    pub absent_since: Option<std::time::Instant>,
}
pub(crate) struct Enrollment {
    pub capability: String,
    pub deadline: std::time::Instant,
    pub anchor: Option<crate::platform::ControlProcessAnchor>,
}
pub(crate) struct Route {
    pub connection: crate::api::ControlConnection,
    pub sequence: u64,
    pub observed: bool,
    pub admission_pending: bool,
}
pub(crate) struct Operation {
    pub info: crate::api::schema::ControlOperationInfo,
    pub deadline: std::time::Instant,
    pub prompt: bool,
    pub delivery_phase: crate::api::schema::ControlPhase,
    pub condition: crate::api::schema::AgentCondition,
}
impl ManagedJob {
    pub fn revoke(&mut self, reason: &str) {
        self.enrollment = None;
        if let Some(route) = self.route.take() {
            route
                .connection
                .alive
                .store(false, std::sync::atomic::Ordering::Release);
        }
        self.info.agent = None;
        self.info.native_scope_id = None;
        self.info.native_request_ids.clear();
        self.info.busy = false;
        for operation in self.operations.values_mut() {
            if !matches!(
                operation.info.phase,
                crate::api::schema::ControlPhase::Settled
                    | crate::api::schema::ControlPhase::Rejected
            ) {
                operation.info.phase = crate::api::schema::ControlPhase::OutcomeUnknown;
                operation.info.error = Some(reason.into());
            }
        }
    }
    pub fn expire(&mut self) {
        let now = std::time::Instant::now();
        for operation in self.operations.values_mut() {
            if operation.deadline <= now
                && !matches!(
                    operation.info.phase,
                    crate::api::schema::ControlPhase::Settled
                        | crate::api::schema::ControlPhase::Rejected
                        | crate::api::schema::ControlPhase::OutcomeUnknown
                )
            {
                operation.info.phase = crate::api::schema::ControlPhase::OutcomeUnknown;
                operation.info.error =
                    Some("deadline expired; do not retry an uncertain operation".into());
            }
        }
    }
}
impl ManagedJobs {
    pub fn retire_absent(
        &mut self,
        live: &std::collections::HashSet<String>,
        now: std::time::Instant,
    ) {
        self.jobs.retain(|_, job| {
            if live.contains(&job.info.condition.terminal.launch_id) {
                job.absent_since = None;
                return true;
            }
            let absent = job.absent_since.get_or_insert(now);
            now.saturating_duration_since(*absent) < std::time::Duration::from_secs(30 * 60)
        });
    }
    pub fn has_runtime(&self, launch: Option<&str>) -> bool {
        self.jobs
            .values()
            .any(|job| Some(job.info.condition.terminal.launch_id.as_str()) == launch)
    }

    pub fn receipt(&self, condition: &JobCondition) -> Result<ManagedTerminalInfo, &'static str> {
        let job = self.jobs.get(&condition.job_id).ok_or("job_not_found")?;
        if job.info.condition != *condition {
            return Err("stale_job_condition");
        }
        Ok(job.info.clone())
    }
    pub fn is_runtime(&self, terminal: &str, launch: Option<&str>) -> bool {
        self.jobs.values().any(|job| {
            job.info.condition.terminal.terminal_id == terminal
                && Some(job.info.condition.terminal.launch_id.as_str()) == launch
        })
    }
}
pub(crate) fn random_identity(prefix: &str) -> Result<String, getrandom::Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut id = String::with_capacity(prefix.len() + 64);
    id.push_str(prefix);
    for byte in bytes {
        id.push(char::from(HEX[usize::from(byte >> 4)]));
        id.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok(id)
}
