use super::responses::{encode_error, encode_success};
use crate::api::schema::*;
use crate::app::App;
use std::sync::atomic::Ordering;

fn native_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
}
fn operation_response(id: String, info: ControlOperationInfo) -> String {
    encode_success(
        id,
        ResponseResult::AgentControlOperation { operation: info },
    )
}
impl App {
    fn validate_agent(&self, condition: &AgentCondition) -> Result<(), &'static str> {
        self.validate_managed_job(&condition.job)?;
        let job = self
            .managed_jobs
            .jobs
            .get(&condition.job.job_id)
            .ok_or("job_not_found")?;
        if job.info.agent.as_ref() != Some(condition) {
            return Err("stale_agent_condition");
        }
        let route = job.route.as_ref().ok_or("control_disconnected")?;
        if !route.connection.alive.load(Ordering::Acquire) {
            return Err("control_disconnected");
        }
        let anchor = job
            .enrollment
            .as_ref()
            .and_then(|enrollment| enrollment.anchor.as_ref())
            .or(job.anchor.as_ref())
            .ok_or("control_identity_unavailable")?;
        crate::platform::validate_control_peer(&route.connection.peer, anchor)
            .map_err(|_| "control_peer_changed")
    }

    pub(super) fn handle_agent_control(
        &mut self,
        request: Request,
        connection: Option<crate::api::ControlConnection>,
    ) -> String {
        let id = request.id;
        match request.method {
            Method::AgentControlRegister(params) => {
                let Some(connection) = connection else {
                    return encode_error(
                        id,
                        "connection_local_only",
                        "registration requires the original local socket",
                    );
                };
                if params.version != 1
                    || params.epoch.len() != 64
                    || !params.epoch.bytes().all(|b| b.is_ascii_hexdigit())
                    || params.session.is_empty()
                    || params.session.len() > 512
                    || [
                        "input_request_lifecycle_v1",
                        "message_admission_v1",
                        "external_request_correlation_v1",
                    ]
                    .iter()
                    .any(|feature| !params.sdk_features.iter().any(|f| f == feature))
                {
                    return encode_error(
                        id,
                        "unsupported_control_client",
                        "required public SDK features and immutable epoch/session are missing",
                    );
                }
                let job_id = self.managed_jobs.jobs.iter().find_map(|(job_id, job)| {
                    job.enrollment
                        .as_ref()
                        .filter(|enrollment| enrollment.capability == params.capability)
                        .map(|_| job_id.clone())
                });
                let Some(job_id) = job_id else {
                    return encode_error(
                        id,
                        "invalid_enrollment",
                        "enrollment capability unknown, expired or consumed",
                    );
                };
                let condition = self.managed_jobs.jobs[&job_id].info.condition.clone();
                if let Err(code) = self.validate_managed_job(&condition) {
                    return encode_error(id, code, "job ownership changed before enrollment");
                }
                let job = self
                    .managed_jobs
                    .jobs
                    .get_mut(&job_id)
                    .expect("known enrollment");
                let enrollment = job.enrollment.as_ref().expect("known enrollment");
                if enrollment.deadline <= std::time::Instant::now() {
                    job.enrollment = None;
                    return encode_error(id, "enrollment_expired", "enrollment expired");
                }
                let Some(anchor) = enrollment.anchor.as_ref() else {
                    return encode_error(
                        id,
                        "control_identity_unavailable",
                        "original wrapper identity unavailable",
                    );
                };
                if crate::platform::validate_control_peer(&connection.peer, anchor).is_err() {
                    return encode_error(
                        id,
                        "invalid_control_peer",
                        "socket peer is not a live member of the original managed job",
                    );
                }
                let condition = AgentCondition {
                    job: condition,
                    connection: connection.connection.clone(),
                    epoch: params.epoch,
                    session: params.session,
                };
                job.anchor = job
                    .enrollment
                    .take()
                    .and_then(|enrollment| enrollment.anchor);
                job.info.agent = Some(condition.clone());
                job.info.phase = ManagedJobPhase::Owned;
                job.info.busy = true;
                job.route = Some(crate::app::control::Route {
                    connection,
                    sequence: 0,
                    observed: false,
                    admission_pending: false,
                });
                encode_success(id, ResponseResult::AgentControlRegistered { condition })
            }
            Method::AgentControlPrompt(params) => {
                if id != params.request_id {
                    return encode_error(
                        id,
                        "invalid_correlation",
                        "request id must equal operation id",
                    );
                }
                if params.text.is_empty() {
                    return encode_error(id, "invalid_prompt", "prompt must not be empty");
                }
                let condition = params.condition.clone();
                let request_id = params.request_id.clone();
                let deadline = params.deadline_ms;
                self.queue_control(
                    id,
                    condition,
                    request_id,
                    deadline,
                    Method::AgentControlPrompt(params),
                    true,
                    None,
                )
            }
            Method::AgentControlAbort(params) => {
                if id != params.request_id {
                    return encode_error(
                        id,
                        "invalid_correlation",
                        "request id must equal operation id",
                    );
                }
                if !native_id(&params.native_request_id) {
                    return encode_error(
                        id,
                        "invalid_native_request",
                        "abort requires an exact native scope UUID",
                    );
                }
                let condition = params.condition.clone();
                let request_id = params.request_id.clone();
                let deadline = params.deadline_ms;
                let native = params.native_request_id.clone();
                self.queue_control(
                    id,
                    condition,
                    request_id,
                    deadline,
                    Method::AgentControlAbort(params),
                    false,
                    Some(native),
                )
            }
            Method::AgentControlStatus(params) => {
                // A revoked stream may still query its immutable outcome, never dispatch more work.
                let job = match self.managed_jobs.receipt(&params.condition.job) {
                    Ok(_) => self
                        .managed_jobs
                        .jobs
                        .get_mut(&params.condition.job.job_id)
                        .expect("existing receipt"),
                    Err(code) => return encode_error(id, code, "job condition unknown"),
                };
                job.expire();
                match job.operations.get(&params.request_id) {
                    Some(operation) if operation.condition == params.condition => {
                        operation_response(id, operation.info.clone())
                    }
                    _ => encode_error(
                        id,
                        "operation_not_found",
                        "operation does not belong to this original route",
                    ),
                }
            }
            Method::AgentControlObserve(params) => {
                if let Err(code) = self.validate_agent(&params.condition) {
                    return encode_error(id, code, "control identity changed");
                }
                let Some(connection) = connection else {
                    return encode_error(
                        id,
                        "connection_local_only",
                        "observation requires enrolled stream",
                    );
                };
                let job = self
                    .managed_jobs
                    .jobs
                    .get_mut(&params.condition.job.job_id)
                    .expect("validated route");
                let route = job.route.as_mut().expect("validated route");
                if connection.connection != params.condition.connection
                    || params.sequence <= route.sequence
                    || params.native_request_ids.len() > 64
                    || params
                        .native_request_ids
                        .iter()
                        .any(|value| !native_id(value))
                    || params
                        .native_scope_id
                        .as_ref()
                        .is_some_and(|value| !native_id(value))
                    || (!params.busy && params.native_scope_id.is_some())
                {
                    return encode_error(
                        id,
                        "invalid_observation",
                        "wrong stream, replayed sequence or invalid native scope",
                    );
                }
                route.sequence = params.sequence;
                route.observed = true;
                job.info.native_scope_id = params.native_scope_id;
                job.info.native_request_ids = params.native_request_ids;
                job.info.busy = params.busy;
                encode_success(id, ResponseResult::Ok {})
            }
            Method::AgentControlAck(params) => self.control_ack(id, params, connection),
            _ => unreachable!("cooperative methods only"),
        }
    }

    fn queue_control(
        &mut self,
        id: String,
        condition: AgentCondition,
        request_id: String,
        deadline_ms: u32,
        method: Method,
        prompt: bool,
        native: Option<String>,
    ) -> String {
        if let Err(code) = self.validate_agent(&condition) {
            return encode_error(id, code, "original control route unavailable");
        }
        if request_id.is_empty()
            || request_id.len() > 128
            || deadline_ms == 0
            || deadline_ms > 30000
        {
            return encode_error(
                id,
                "invalid_operation",
                "bounded unique request id and 1..30000ms deadline required",
            );
        }
        let frame = match serde_json::to_string(&Request {
            id: id.clone(),
            method,
        }) {
            Ok(frame) if frame.len() < 16384 => frame,
            _ => return encode_error(id, "control_frame_too_large", "control frame exceeds 16KiB"),
        };
        let job = self
            .managed_jobs
            .jobs
            .get_mut(&condition.job.job_id)
            .expect("validated job");
        job.expire();
        if job.operations.contains_key(&request_id) {
            return encode_error(
                id,
                "operation_replayed",
                "operation id already used; do not retry",
            );
        }
        if job.operations.len() >= 256 {
            return encode_error(
                id,
                "operation_limit",
                "route operation ledger full; release or relaunch required",
            );
        }
        let route = job.route.as_mut().expect("validated route");
        if !route.observed {
            return encode_error(
                id,
                "control_not_observed",
                "first native observation required before dispatch",
            );
        }
        if prompt && (job.info.busy || route.admission_pending) {
            return encode_error(
                id,
                "agent_busy",
                "native turn or uncertain prompt admission is outstanding",
            );
        }
        if !prompt && (!job.info.busy || job.info.native_scope_id != native) {
            return encode_error(
                id,
                "native_scope_changed",
                "target scope is no longer current; no abort sent",
            );
        }
        let info = ControlOperationInfo {
            request_id: request_id.clone(),
            phase: ControlPhase::Queued,
            native_request_id: native,
            aborted: None,
            error: None,
        };
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(u64::from(deadline_ms));
        if route
            .connection
            .outbound
            .try_send(crate::api::ControlOutbound { frame, deadline })
            .is_err()
        {
            job.revoke("control stream unavailable or queue full");
            return encode_error(
                id,
                "control_disconnected",
                "control stream unavailable; delivery was not accepted",
            );
        }
        if prompt {
            route.admission_pending = true;
        }
        job.operations.insert(
            request_id,
            crate::app::control::Operation {
                info: info.clone(),
                deadline,
                prompt,
                delivery_phase: ControlPhase::Queued,
                condition,
            },
        );
        operation_response(id, info)
    }

    fn control_ack(
        &mut self,
        id: String,
        params: AgentControlAckParams,
        connection: Option<crate::api::ControlConnection>,
    ) -> String {
        if id != params.request_id {
            return encode_error(id, "invalid_correlation", "ack id must equal operation id");
        }
        if let Err(code) = self.validate_agent(&params.condition) {
            return encode_error(id, code, "original control identity changed");
        }
        let Some(connection) = connection else {
            return encode_error(id, "connection_local_only", "ack requires enrolled stream");
        };
        if connection.connection != params.condition.connection {
            return encode_error(id, "stale_control_stream", "ack stream changed");
        }
        let job = self
            .managed_jobs
            .jobs
            .get_mut(&params.condition.job.job_id)
            .expect("validated job");
        let Some(operation) = job.operations.get_mut(&params.request_id) else {
            return encode_error(id, "operation_not_found", "unknown operation");
        };
        let old = &operation.delivery_phase;
        let valid = match params.phase {
            ControlPhase::Received => {
                *old == ControlPhase::Queued && params.native_request_id.is_none()
            }
            ControlPhase::Accepted => {
                *old == ControlPhase::Received
                    && params
                        .native_request_id
                        .as_ref()
                        .is_some_and(|value| native_id(value))
                    && (operation.prompt
                        || params.native_request_id == operation.info.native_request_id)
            }
            ControlPhase::Settled => {
                *old == ControlPhase::Accepted
                    && params.native_request_id == operation.info.native_request_id
            }
            ControlPhase::Rejected => {
                matches!(old, ControlPhase::Queued | ControlPhase::Received)
                    && params.error.as_ref().is_some_and(|error| !error.is_empty())
            }
            _ => false,
        };
        if !valid
            || params
                .error
                .as_ref()
                .is_some_and(|error| error.len() > 1024)
        {
            return encode_error(
                id,
                "invalid_ack",
                "out of order acknowledgment or native correlation mismatch",
            );
        }
        operation.delivery_phase = params.phase.clone();
        operation.info.phase = params.phase;
        operation.info.native_request_id = params
            .native_request_id
            .or(operation.info.native_request_id.clone());
        operation.info.aborted = params.aborted;
        operation.info.error = params.error;
        if operation.prompt
            && matches!(
                operation.info.phase,
                ControlPhase::Settled | ControlPhase::Rejected
            )
        {
            job.route
                .as_mut()
                .expect("validated route")
                .admission_pending = false;
        }
        operation_response(id, operation.info.clone())
    }
}
