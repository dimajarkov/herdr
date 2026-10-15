use super::*;
use serde_json::{json, Value};
fn request(server: &mut HeadlessServer, method: &str, params: Value) -> Value {
    let (respond_to, rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: serde_json::from_value(
            json!({"id":"managed-handoff","method":method,"params":params}),
        )
        .unwrap(),
        respond_to,
        response_write_complete: None,
        control: None,
    });
    serde_json::from_str(&rx.recv_timeout(Duration::from_secs(2)).unwrap()).unwrap()
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_live_handoff_rejects_owned_and_released_runtime_but_not_tombstone() {
    let mut server = test_headless_server();
    server.app.state.workspaces = vec![crate::workspace::Workspace::test_new("managed handoff")];
    server.app.state.ensure_test_terminals();
    server.app.state.active = Some(0);
    let pane = server.app.state.workspaces[0].tabs[0].root_pane;
    let terminal = server.app.state.workspaces[0]
        .terminal_id(pane)
        .unwrap()
        .clone();
    let (runtime, _) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
    server.app.terminal_runtimes.insert(terminal, runtime);
    let parent = headless_pane_list(&mut server).pop().unwrap().pane_id;
    let root = std::env::temp_dir().join(format!("herdr-managed-handoff-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(&script, "sleep 30\n").unwrap();
    let parent_condition = request(&mut server, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launch = request(
        &mut server,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launch["result"]["condition"].clone();
    assert!(condition.is_object(), "{launch}");
    let before = request(&mut server, "pane.list", json!({}));
    let handoff = json!({"import_exe":"/private/tmp/herdr46-not-an-installed-candidate"});
    assert_eq!(
        request(&mut server, "server.live_handoff", handoff.clone())["error"]["code"],
        "managed_handoff_unsupported"
    );
    assert_eq!(request(&mut server, "pane.list", json!({})), before);
    assert!(!server.app.state.should_quit);
    request(
        &mut server,
        "terminal.job.release",
        json!({"condition":condition}),
    );
    assert_eq!(
        request(&mut server, "server.live_handoff", handoff.clone())["error"]["code"],
        "managed_handoff_unsupported"
    );
    request(
        &mut server,
        "pane.close",
        json!({"pane_id":condition["terminal"]["pane_id"]}),
    );
    assert_eq!(
        request(&mut server, "server.live_handoff", handoff)["error"]["code"],
        "handoff_failed"
    );
    drop(server);
    std::fs::remove_dir_all(root).unwrap();
}
