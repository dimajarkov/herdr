use serde::{Deserialize, Serialize};

/// Identifies one live terminal runtime and its current layout membership.
/// This condition does not identify an agent process inside a persistent shell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TerminalCondition {
    pub pane_id: String,
    pub tab_id: String,
    pub terminal_id: String,
    pub launch_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TerminalRenameIfParams {
    pub condition: TerminalCondition,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TerminalCloseIfParams {
    pub condition: TerminalCondition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TerminalInputIfParams {
    pub condition: TerminalCondition,
    pub text: String,
}
