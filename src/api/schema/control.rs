use super::{SplitDirection, TerminalCondition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobCondition {
    pub terminal: TerminalCondition,
    pub job_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentCondition {
    pub job: JobCondition,
    pub connection: String,
    pub epoch: String,
    pub session: String,
}
#[derive(Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedLaunchParams {
    pub parent: TerminalCondition,
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub direction: SplitDirection,
    #[serde(default)]
    pub label: Option<String>,
}
impl std::fmt::Debug for ManagedLaunchParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedLaunchParams")
            .field("parent", &self.parent)
            .field("argv_count", &self.argv.len())
            .field("cwd", &self.cwd)
            .field("env", &"[redacted]")
            .field("direction", &self.direction)
            .field("label", &self.label)
            .finish()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedJobParams {
    pub condition: JobCondition,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ManagedJobPhase {
    AwaitingEnrollment,
    EnrollmentFailed,
    Owned,
    Released,
    Moved,
    Replaced,
    Exited,
    Closed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ManagedExit {
    pub code: Option<u32>,
    pub reason: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ManagedTerminalInfo {
    pub condition: JobCondition,
    pub phase: ManagedJobPhase,
    pub agent: Option<AgentCondition>,
    pub exit: Option<ManagedExit>,
    pub diagnostic: Option<String>,
    pub native_scope_id: Option<String>,
    pub native_request_ids: Vec<String>,
    pub busy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedJobRenameParams {
    pub condition: JobCondition,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentControlRegisterParams {
    pub capability: String,
    pub version: u32,
    pub epoch: String,
    pub session: String,
    pub sdk_features: Vec<String>,
}
impl std::fmt::Debug for AgentControlRegisterParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentControlRegisterParams")
            .field("capability", &"[redacted]")
            .field("version", &self.version)
            .field("epoch", &self.epoch)
            .field("session", &self.session)
            .field("sdk_features", &self.sdk_features)
            .finish()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentControlPromptParams {
    pub condition: AgentCondition,
    pub request_id: String,
    pub text: String,
    pub deadline_ms: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentControlAbortParams {
    pub condition: AgentCondition,
    pub request_id: String,
    pub native_request_id: String,
    pub deadline_ms: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentControlStatusParams {
    pub condition: AgentCondition,
    pub request_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentControlAckParams {
    pub condition: AgentCondition,
    pub request_id: String,
    pub phase: ControlPhase,
    #[serde(default)]
    pub native_request_id: Option<String>,
    #[serde(default)]
    pub aborted: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentControlObserveParams {
    pub condition: AgentCondition,
    pub sequence: u64,
    #[serde(default)]
    pub native_scope_id: Option<String>,
    pub native_request_ids: Vec<String>,
    pub busy: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlPhase {
    Queued,
    Received,
    Accepted,
    Settled,
    Rejected,
    OutcomeUnknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ControlOperationInfo {
    pub request_id: String,
    pub phase: ControlPhase,
    pub native_request_id: Option<String>,
    pub aborted: Option<bool>,
    pub error: Option<String>,
}

pub(crate) const MANAGED_CONTROL_MAX_FRAME_BYTES: usize = 16 * 1024;
pub(crate) fn is_managed_control_method_name(method: &str) -> bool {
    method.starts_with("terminal.job.") || method.starts_with("agent.control.")
}
