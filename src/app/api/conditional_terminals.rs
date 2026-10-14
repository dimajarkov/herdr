use super::responses::{encode_error, encode_success};
use crate::api::schema::{
    PaneRenameParams, PaneTarget, ResponseResult, TerminalCloseIfParams, TerminalCondition,
    TerminalInputIfParams, TerminalRenameIfParams,
};
use crate::app::App;

impl App {
    fn current_terminal_condition(&self, pane: &str) -> Option<TerminalCondition> {
        // Never follow a stale pane alias to its new location.
        let (ws_idx, pane_id) = self.parse_current_public_pane_id(pane)?;
        let ws = self.state.workspaces.get(ws_idx)?;
        let terminal_id = ws.terminal_id(pane_id)?;
        let runtime = self.terminal_runtimes.get(terminal_id)?;
        let tab_idx = ws.find_tab_index_for_pane(pane_id)?;
        Some(TerminalCondition {
            pane_id: self.public_pane_id(ws_idx, pane_id)?,
            tab_id: self.public_tab_id(ws_idx, tab_idx)?,
            terminal_id: terminal_id.to_string(),
            launch_id: runtime.launch_id().to_string(),
        })
    }

    fn validate_terminal_condition(
        &self,
        condition: &TerminalCondition,
    ) -> Result<(), &'static str> {
        match self.current_terminal_condition(&condition.pane_id) {
            None => Err("terminal_not_found"),
            Some(current) if current == *condition => Ok(()),
            Some(_) => Err("stale_terminal_condition"),
        }
    }

    pub(super) fn handle_terminal_condition(&self, id: String, target: PaneTarget) -> String {
        let Some(condition) = self.current_terminal_condition(&target.pane_id) else {
            return encode_error(
                id,
                "terminal_not_found",
                "current pane has no live terminal runtime",
            );
        };
        encode_success(
            id,
            ResponseResult::TerminalCondition {
                condition,
                scope: "terminal_runtime".into(),
                input_supported: false,
            },
        )
    }

    pub(super) fn handle_terminal_input_if(
        &self,
        id: String,
        params: TerminalInputIfParams,
    ) -> String {
        if let Err(code) = self.validate_terminal_condition(&params.condition) {
            return encode_error(
                id,
                code,
                "terminal runtime or layout membership changed; no input performed",
            );
        }
        encode_error(id, "unsupported_transport", "terminal_runtime conditions cannot isolate an original agent reader; no PTY input was queued")
    }

    pub(super) fn handle_terminal_close_if(
        &mut self,
        id: String,
        params: TerminalCloseIfParams,
    ) -> String {
        if let Err(code) = self.validate_terminal_condition(&params.condition) {
            return encode_error(
                id,
                code,
                "terminal runtime or layout membership changed; no close performed",
            );
        }
        // Existing close protections remain authoritative. Never force or fall back.
        self.handle_pane_close(
            id,
            PaneTarget {
                pane_id: params.condition.pane_id,
            },
        )
    }

    pub(super) fn handle_terminal_rename_if(
        &mut self,
        id: String,
        params: TerminalRenameIfParams,
    ) -> String {
        if let Err(code) = self.validate_terminal_condition(&params.condition) {
            return encode_error(
                id,
                code,
                "terminal runtime or layout membership changed; no rename performed",
            );
        }
        self.handle_pane_rename(
            id,
            PaneRenameParams {
                pane_id: params.condition.pane_id,
                label: params.label,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::app::{App, AppPolicy};
    use serde_json::{json, Value};

    fn app() -> (App, String) {
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            AppPolicy::TEST,
            None,
            rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![crate::workspace::Workspace::test_new("conditional")];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        let pane = app.state.workspaces[0].tabs[0].root_pane;
        let terminal = app.state.workspaces[0].terminal_id(pane).unwrap().clone();
        let (runtime, _) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.terminal_runtimes.insert(terminal, runtime);
        let public = app.public_pane_id(0, pane).unwrap();
        (app, public)
    }

    fn request(app: &mut App, method: &str, params: Value) -> Value {
        let request =
            serde_json::from_value(json!({"id":"guard", "method":method, "params":params}))
                .unwrap();
        serde_json::from_str(&app.handle_api_request(request)).unwrap()
    }

    #[tokio::test]
    async fn conditional_terminal_rename_matches_live_runtime() {
        let (mut app, pane) = app();
        let result = request(&mut app, "terminal.condition", json!({"pane_id":pane}));
        assert_eq!(result["result"]["scope"], "terminal_runtime");
        assert_eq!(result["result"]["input_supported"], false);
        let condition = result["result"]["condition"].clone();
        let renamed = request(
            &mut app,
            "terminal.rename_if",
            json!({"condition":condition,"label":"owned child"}),
        );
        assert_eq!(renamed["result"]["pane"]["label"], "owned child");
        assert_eq!(
            renamed["result"]["pane"]["terminal_id"],
            condition["terminal_id"]
        );
        app.state.assert_invariants_for_test();
    }
    #[tokio::test]
    async fn conditional_terminal_close_removes_only_matching_runtime() {
        let (mut app, pane) = app();
        let condition = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        let closed = request(
            &mut app,
            "terminal.close_if",
            json!({"condition":condition}),
        );
        assert_eq!(closed["result"]["type"], "ok");
        assert_eq!(
            request(&mut app, "terminal.condition", json!({"pane_id":pane}))["error"]["code"],
            "terminal_not_found"
        );
        app.state.assert_invariants_for_test();
    }

    #[tokio::test]
    async fn conditional_terminal_input_rejects_raw_pty_transport() {
        let (mut app, pane) = app();
        let condition = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        let rejected = request(
            &mut app,
            "terminal.input_if",
            json!({"condition":condition,"text":"do not deliver"}),
        );
        assert_eq!(rejected["error"]["code"], "unsupported_transport");
        assert_eq!(
            request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]["condition"],
            condition
        );
    }

    #[tokio::test]
    async fn conditional_terminal_stale_condition_never_mutates_target() {
        let (mut app, pane) = app();
        let condition = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        for field in ["launch_id", "terminal_id", "tab_id"] {
            let mut stale = condition.clone();
            stale[field] = json!("stale");
            assert_eq!(
                request(
                    &mut app,
                    "terminal.rename_if",
                    json!({"condition":stale,"label":"wrong"})
                )["error"]["code"],
                "stale_terminal_condition"
            );
            assert_eq!(
                request(&mut app, "terminal.close_if", json!({"condition":stale}))["error"]["code"],
                "stale_terminal_condition"
            );
            assert_eq!(
                request(
                    &mut app,
                    "terminal.input_if",
                    json!({"condition":stale,"text":"wrong"})
                )["error"]["code"],
                "stale_terminal_condition"
            );
        }
        let current = request(&mut app, "pane.get", json!({"pane_id":pane}));
        assert_ne!(current["result"]["pane"]["label"], "wrong");
        assert_eq!(
            request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]["condition"],
            condition
        );
        app.state.assert_invariants_for_test();
    }

    #[tokio::test]
    async fn conditional_terminal_restart_invalidates_old_launch_condition() {
        let (mut app, pane) = app();
        let old = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal = app.state.workspaces[0]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        let (replacement, _) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.terminal_runtimes.insert(terminal, replacement);
        let current = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        assert_ne!(current["launch_id"], old["launch_id"]);
        assert_eq!(current["terminal_id"], old["terminal_id"]);
        assert_eq!(
            request(&mut app, "terminal.close_if", json!({"condition":old}))["error"]["code"],
            "stale_terminal_condition"
        );
        assert_eq!(
            request(
                &mut app,
                "terminal.rename_if",
                json!({"condition":current,"label":"replacement preserved"})
            )["result"]["pane"]["label"],
            "replacement preserved"
        );
    }

    #[tokio::test]
    async fn conditional_terminal_movement_does_not_follow_old_pane_alias() {
        let (mut app, pane) = app();
        let old = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        app.state.workspaces[0].test_split(ratatui::layout::Direction::Horizontal);
        app.state.ensure_test_terminals();
        let moved = request(
            &mut app,
            "pane.move",
            json!({"pane_id":pane,"destination":{"type":"new_workspace","label":"moved"},"focus":false}),
        );
        let new_pane = moved["result"]["move_result"]["pane"]["pane_id"].clone();
        assert!(new_pane.is_string(), "{moved}");
        assert_ne!(new_pane, old["pane_id"]);
        assert_eq!(
            request(&mut app, "terminal.close_if", json!({"condition":old}))["error"]["code"],
            "terminal_not_found"
        );
        let current = request(&mut app, "terminal.condition", json!({"pane_id":new_pane}));
        assert_eq!(
            current["result"]["condition"]["launch_id"],
            old["launch_id"]
        );
        app.state.assert_invariants_for_test();
    }

    #[tokio::test]
    async fn conditional_terminal_missing_target_fails_explicitly() {
        let (mut app, pane) = app();
        let mut condition = request(&mut app, "terminal.condition", json!({"pane_id":pane}))
            ["result"]["condition"]
            .clone();
        condition["pane_id"] = json!("w999:p999");
        assert_eq!(
            request(
                &mut app,
                "terminal.rename_if",
                json!({"condition":condition,"label":"wrong"})
            )["error"]["code"],
            "terminal_not_found"
        );
        assert_eq!(
            request(
                &mut app,
                "terminal.close_if",
                json!({"condition":condition})
            )["error"]["code"],
            "terminal_not_found"
        );
        assert_eq!(
            request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]["type"],
            "terminal_condition"
        );
    }

    #[tokio::test]
    async fn conditional_terminal_close_preserves_group_protection() {
        let (mut app, pane) = app();
        app.state.confirm_close = true;
        let membership = crate::workspace::WorktreeSpaceMembership {
            key: "guarded-group".into(),
            label: "group".into(),
            repo_root: "/test".into(),
            checkout_path: "/test".into(),
            is_linked_worktree: false,
        };
        app.state.workspaces[0].worktree_space = Some(membership.clone());
        let mut other = crate::workspace::Workspace::test_new("independent");
        other.worktree_space = Some(crate::workspace::WorktreeSpaceMembership {
            is_linked_worktree: true,
            checkout_path: "/test/child".into(),
            ..membership
        });
        app.state.workspaces.push(other);
        app.state.ensure_test_terminals();
        let condition = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        assert_eq!(
            request(
                &mut app,
                "terminal.close_if",
                json!({"condition":condition})
            )["error"]["code"],
            "confirmation_required"
        );
        assert_eq!(
            request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]["condition"],
            condition
        );
        assert_eq!(
            request(&mut app, "workspace.list", json!({}))["result"]["workspaces"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        app.state.assert_invariants_for_test();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn conditional_terminal_real_pty_input_rejection_delivers_no_bytes() {
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        let (mut app, pane) = app();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        let (events, _events_rx) = tokio::sync::mpsc::channel(64);
        let runtime = crate::terminal::TerminalRuntime::spawn_argv_command(
            pane_id, 24, 80, std::env::temp_dir(),
            &["/bin/sh".into(), "-c".into(), "printf 'OWNED_READER_READY\\n'; IFS= read -r line; printf 'RECEIVED:%s\\n' \"$line\"; sleep 2".into()],
            &crate::pane::PaneLaunchEnv::from_extra(Vec::new()), crate::pane::AgentDetection::Disabled,
            4096, crate::terminal_theme::TerminalTheme::default(), None, events,
            Arc::new(tokio::sync::Notify::new()), Arc::new(crate::render_signal::RenderSignal::new())
        ).unwrap();
        app.terminal_runtimes.insert(terminal_id, runtime);
        async fn wait_text(app: &mut App, pane: &str, needle: &str) -> String {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let result = request(
                    app,
                    "pane.read",
                    json!({"pane_id":pane,"format":"text","source":"visible"}),
                );
                let text = result["result"]["read"]["text"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                if text.contains(needle) {
                    return text;
                }
                assert!(Instant::now() < deadline, "{result}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        wait_text(&mut app, &pane, "OWNED_READER_READY").await;
        let condition = request(&mut app, "terminal.condition", json!({"pane_id":pane}))["result"]
            ["condition"]
            .clone();
        assert_eq!(
            request(
                &mut app,
                "terminal.input_if",
                json!({"condition":condition,"text":"FORBIDDEN\n"})
            )["error"]["code"],
            "unsupported_transport"
        );
        assert_eq!(
            request(
                &mut app,
                "pane.send_text",
                json!({"pane_id":pane,"text":"CONTROL\n"})
            )["result"]["type"],
            "ok"
        );
        let text = wait_text(&mut app, &pane, "RECEIVED:CONTROL").await;
        assert!(!text.contains("FORBIDDEN"), "{text}");
        request(
            &mut app,
            "terminal.close_if",
            json!({"condition":condition}),
        );
    }
}
