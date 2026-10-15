use crate::app::{App, AppPolicy};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn app() -> (App, String) {
    let (_, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut app = App::new(
        &crate::config::Config::default(),
        AppPolicy::TEST,
        None,
        rx,
        crate::api::EventHub::default(),
    );
    app.state.workspaces = vec![crate::workspace::Workspace::test_new("owned jobs")];
    app.state.ensure_test_terminals();
    app.state.active = Some(0);
    app.state.workspaces[0].tabs[0].events = app.event_tx.clone();
    let pane = app.state.workspaces[0].tabs[0].root_pane;
    let terminal = app.state.workspaces[0].terminal_id(pane).unwrap().clone();
    let (runtime, _) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
    app.terminal_runtimes.insert(terminal, runtime);
    let public = app.public_pane_id(0, pane).unwrap();
    (app, public)
}
fn request(app: &mut App, method: &str, params: Value) -> Value {
    let request =
        serde_json::from_value(json!({"id":"managed-test","method":method,"params":params}))
            .unwrap();
    serde_json::from_str(&app.handle_api_request(request)).unwrap()
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_launch_preserves_direct_script_env_cwd_and_exit_receipt() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-launch-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("long-launch.sh");
    std::fs::write(
        &script,
        "printf '%s|%s' \"$PWD\" \"$TEST_VALUE\" > result.sidecar; exit 7\n",
    )
    .unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launched = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{"TEST_VALUE":"synthetic-value"},"direction":"right","label":"owned job"}),
    );
    assert_eq!(launched["result"]["type"], "managed_terminal", "{launched}");
    let condition = launched["result"]["condition"].clone();
    assert_ne!(condition["terminal"]["pane_id"], parent);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = request(&mut app, "terminal.job.get", json!({"condition":condition}));
        if status["result"]["phase"] == "exited" {
            assert_eq!(status["result"]["exit"]["code"], 7, "{status}");
            break;
        }
        assert!(Instant::now() < deadline, "{status}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        std::fs::read_to_string(root.join("result.sidecar")).unwrap(),
        format!("{}|synthetic-value", root.display())
    );
    assert_eq!(
        request(&mut app, "pane.get", json!({"pane_id":parent}))["result"]["pane"]["focused"],
        true
    );
    assert_eq!(
        request(
            &mut app,
            "terminal.condition",
            json!({"pane_id":condition["terminal"]["pane_id"]})
        )["error"]["code"],
        "terminal_not_found"
    );
    app.state.assert_invariants_for_test();
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_release_keeps_resource_through_late_wrapper_exit() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-release-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("release-launch.sh");
    std::fs::write(
        &script,
        "sleep 0.2; printf finished > completed.sidecar; exit 0\n",
    )
    .unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launched = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launched["result"]["condition"].clone();
    assert_eq!(
        request(
            &mut app,
            "terminal.job.release",
            json!({"condition":condition})
        )["result"]["phase"],
        "released"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = request(&mut app, "terminal.job.get", json!({"condition":condition}));
        assert_eq!(status["result"]["phase"], "released");
        if status["result"]["exit"]["code"] == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "{status}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        std::fs::read_to_string(root.join("completed.sidecar")).unwrap(),
        "finished"
    );
    assert_eq!(
        request(
            &mut app,
            "pane.get",
            json!({"pane_id":condition["terminal"]["pane_id"]})
        )["result"]["pane"]["terminal_id"],
        condition["terminal"]["terminal_id"]
    );
    assert_eq!(
        request(
            &mut app,
            "terminal.job.close",
            json!({"condition":condition})
        )["error"]["code"],
        "job_released"
    );
    request(
        &mut app,
        "pane.close",
        json!({"pane_id":condition["terminal"]["pane_id"]}),
    );
    app.state.assert_invariants_for_test();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn managed_control_registration_without_socket_context_fails_closed() {
    let (mut app, _) = app();
    let response = request(
        &mut app,
        "agent.control.register",
        json!({"capability":"synthetic","version":1,"epoch":"a".repeat(64),"session":"native-session","sdk_features":["input_request_lifecycle_v1","message_admission_v1","external_request_correlation_v1"]}),
    );
    assert_eq!(response["error"]["code"], "connection_local_only");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_moved_resource_revokes_old_authority_without_following_alias() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-move-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("move-launch.sh");
    std::fs::write(&script, "sleep 30\n").unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launch = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launch["result"]["condition"].clone();
    let moved = request(
        &mut app,
        "pane.move",
        json!({"pane_id":condition["terminal"]["pane_id"],"destination":{"type":"new_workspace","label":"moved job"},"focus":false}),
    );
    let new_pane = moved["result"]["move_result"]["pane"]["pane_id"].clone();
    assert!(new_pane.is_string(), "{moved}");
    assert_eq!(
        request(
            &mut app,
            "terminal.job.close",
            json!({"condition":condition})
        )["error"]["code"],
        "job_moved"
    );
    assert_eq!(
        request(&mut app, "terminal.job.get", json!({"condition":condition}))["result"]["phase"],
        "moved"
    );
    assert_eq!(
        request(&mut app, "terminal.condition", json!({"pane_id":new_pane}))["result"]["condition"]
            ["terminal_id"],
        condition["terminal"]["terminal_id"]
    );
    request(&mut app, "pane.close", json!({"pane_id":new_pane}));
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
fn peer_request(
    app: &mut App,
    connection: &crate::api::ControlConnection,
    id: &str,
    method: &str,
    params: Value,
) -> Value {
    let request = serde_json::from_value(json!({"id":id,"method":method,"params":params})).unwrap();
    serde_json::from_str(&app.handle_api_request_with_control(request, Some(connection.clone())))
        .unwrap()
}
#[cfg(target_os = "macos")]
fn enrolled_fixture(
    name: &str,
) -> (
    App,
    Value,
    crate::api::ControlConnection,
    std::sync::mpsc::Receiver<crate::api::ControlOutbound>,
    std::path::PathBuf,
    crate::ipc::LocalStream,
) {
    use interprocess::local_socket::traits::Listener as _;
    let (mut app, parent) = app();
    let root =
        std::env::temp_dir().join(format!("herdr-managed-peer-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let socket = root.join("peer.sock");
    let listener = crate::ipc::bind_local_listener(&socket).unwrap();
    let child = root.join("child.py");
    std::fs::write(&child,"import os,socket,time\nopen(os.environ['CAP_FILE'],'w').write(os.environ['HERDR_CONTROL_CAPABILITY'])\ns=socket.socket(socket.AF_UNIX)\ns.connect(os.environ['TEST_SOCKET'])\ntime.sleep(30)\n").unwrap();
    let script = root.join("launch.sh");
    std::fs::write(
        &script,
        format!("exec /usr/bin/python3 '{}'\n", child.display()),
    )
    .unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launched = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{"TEST_SOCKET":socket,"CAP_FILE":root.join("capability")},"direction":"right"}),
    );
    assert_eq!(launched["result"]["type"], "managed_terminal", "{launched}");
    let stream = listener.accept().unwrap();
    let peer = crate::platform::control_peer_identity(&stream).unwrap();
    let (outbound, rx) = std::sync::mpsc::sync_channel(8);
    let connection = crate::api::ControlConnection {
        peer,
        connection: crate::app::control::random_identity("connection_").unwrap(),
        outbound,
        alive: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        diagnostic: std::sync::Arc::new(std::sync::Mutex::new(None)),
    };
    let capability = std::fs::read_to_string(root.join("capability")).unwrap();
    let params = json!({"capability":capability,"version":1,"epoch":"e".repeat(64),"session":"native-fixture","sdk_features":["input_request_lifecycle_v1","message_admission_v1","external_request_correlation_v1"]});
    let outsider_path = root.join("outside.sock");
    let outsider_listener = crate::ipc::bind_local_listener(&outsider_path).unwrap();
    let _outsider_client = crate::ipc::connect_local_stream(&outsider_path).unwrap();
    let outsider_stream = outsider_listener.accept().unwrap();
    let mut outsider = connection.clone();
    outsider.peer = crate::platform::control_peer_identity(&outsider_stream).unwrap();
    outsider.connection = crate::app::control::random_identity("outside_").unwrap();
    assert_eq!(
        peer_request(
            &mut app,
            &outsider,
            "outside-register",
            "agent.control.register",
            params.clone()
        )["error"]["code"],
        "invalid_control_peer"
    );
    let registered = peer_request(
        &mut app,
        &connection,
        "register",
        "agent.control.register",
        params.clone(),
    );
    assert_eq!(
        registered["result"]["type"], "agent_control_registered",
        "{registered}"
    );
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "replay",
            "agent.control.register",
            params
        )["error"]["code"],
        "invalid_enrollment"
    );
    let condition = registered["result"]["condition"].clone();
    (app, condition, connection, rx, root, stream)
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_control_real_peer_prompt_ack_scope_abort_and_replay() {
    let (mut app, condition, connection, rx, root, _stream) = enrolled_fixture("operations");
    let prompt = json!({"condition":condition,"request_id":"prompt-1","text":"synthetic prompt","deadline_ms":30000});
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "prompt-1",
            "agent.control.prompt",
            prompt.clone()
        )["error"]["code"],
        "control_not_observed"
    );
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "observe-prep",
            "agent.control.observe",
            json!({"condition":condition,"sequence":1,"native_scope_id":null,"native_request_ids":[],"busy":true})
        )["result"]["type"],
        "ok"
    );
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "prompt-1",
            "agent.control.prompt",
            prompt.clone()
        )["error"]["code"],
        "agent_busy"
    );
    peer_request(
        &mut app,
        &connection,
        "observe-idle",
        "agent.control.observe",
        json!({"condition":condition,"sequence":2,"native_scope_id":null,"native_request_ids":[],"busy":false}),
    );
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "prompt-1",
            "agent.control.prompt",
            prompt.clone()
        )["result"]["phase"],
        "queued"
    );
    let delivered: Value =
        serde_json::from_str(&rx.recv_timeout(Duration::from_secs(1)).unwrap().frame).unwrap();
    assert_eq!(delivered["params"]["condition"], condition);
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "prompt-1",
            "agent.control.prompt",
            prompt
        )["error"]["code"],
        "operation_replayed"
    );
    let member = "01234567-89ab-4cde-8fab-0123456789ab";
    for (phase, native) in [
        ("received", None),
        ("accepted", Some(member)),
        ("settled", Some(member)),
    ] {
        let ack = peer_request(
            &mut app,
            &connection,
            "prompt-1",
            "agent.control.ack",
            json!({"condition":condition,"request_id":"prompt-1","phase":phase,"native_request_id":native}),
        );
        assert_eq!(ack["result"]["phase"], phase, "{ack}");
    }
    let scope = "11234567-89ab-4cde-8fab-0123456789ab";
    peer_request(
        &mut app,
        &connection,
        "observe-human",
        "agent.control.observe",
        json!({"condition":condition,"sequence":3,"native_scope_id":scope,"native_request_ids":[member],"busy":true}),
    );
    let abort = peer_request(
        &mut app,
        &connection,
        "abort-human",
        "agent.control.abort",
        json!({"condition":condition,"request_id":"abort-human","native_request_id":scope,"deadline_ms":30000}),
    );
    assert_eq!(abort["result"]["phase"], "queued", "{abort}");
    let sent: Value =
        serde_json::from_str(&rx.recv_timeout(Duration::from_secs(1)).unwrap().frame).unwrap();
    assert_eq!(sent["params"]["native_request_id"], scope);
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "stale-abort",
            "agent.control.abort",
            json!({"condition":condition,"request_id":"stale-abort","native_request_id":member,"deadline_ms":100})
        )["error"]["code"],
        "native_scope_changed"
    );
    request(
        &mut app,
        "terminal.job.close",
        json!({"condition":condition["job"]}),
    );
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_control_uncertain_admission_stays_fenced_until_exact_late_settlement() {
    let (mut app, condition, connection, rx, root, _stream) = enrolled_fixture("deadline");
    peer_request(
        &mut app,
        &connection,
        "observe",
        "agent.control.observe",
        json!({"condition":condition,"sequence":1,"native_request_ids":[],"busy":false}),
    );
    let response = peer_request(
        &mut app,
        &connection,
        "deadline-prompt",
        "agent.control.prompt",
        json!({"condition":condition,"request_id":"deadline-prompt","text":"synthetic deadline","deadline_ms":20}),
    );
    assert_eq!(response["result"]["phase"], "queued");
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "status",
            "agent.control.status",
            json!({"condition":condition,"request_id":"deadline-prompt"})
        )["result"]["phase"],
        "outcome_unknown"
    );
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "successor",
            "agent.control.prompt",
            json!({"condition":condition,"request_id":"successor","text":"should be fenced","deadline_ms":100})
        )["error"]["code"],
        "agent_busy"
    );
    let member = "01234567-89ab-4cde-8fab-0123456789ab";
    for (phase, native) in [
        ("received", None),
        ("accepted", Some(member)),
        ("settled", Some(member)),
    ] {
        assert_eq!(
            peer_request(
                &mut app,
                &connection,
                "deadline-prompt",
                "agent.control.ack",
                json!({"condition":condition,"request_id":"deadline-prompt","phase":phase,"native_request_id":native})
            )["result"]["phase"],
            phase
        );
    }
    request(
        &mut app,
        "terminal.job.release",
        json!({"condition":condition["job"]}),
    );
    assert!(!connection.alive.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        peer_request(
            &mut app,
            &connection,
            "released",
            "agent.control.prompt",
            json!({"condition":condition,"request_id":"released","text":"blocked","deadline_ms":100})
        )["error"]["code"],
        "job_released"
    );
    request(
        &mut app,
        "pane.close",
        json!({"pane_id":condition["job"]["terminal"]["pane_id"]}),
    );
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_protected_close_retains_ownership_and_diagnostic() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-protected-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(&script, "sleep 30\n").unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launched = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launched["result"]["condition"].clone();
    app.state.confirm_close = true;
    let membership = crate::workspace::WorktreeSpaceMembership {
        key: "owned-protected".into(),
        label: "group".into(),
        repo_root: "/test".into(),
        checkout_path: "/test".into(),
        is_linked_worktree: false,
    };
    app.state.workspaces[0].worktree_space = Some(membership.clone());
    let mut other = crate::workspace::Workspace::test_new("protected sibling");
    other.worktree_space = Some(crate::workspace::WorktreeSpaceMembership {
        is_linked_worktree: true,
        checkout_path: "/test/child".into(),
        ..membership
    });
    app.state.workspaces.push(other);
    app.state.ensure_test_terminals();
    request(&mut app, "pane.close", json!({"pane_id":parent}));
    let refused = request(
        &mut app,
        "terminal.job.close",
        json!({"condition":condition}),
    );
    assert_eq!(
        refused["error"]["code"], "confirmation_required",
        "{refused}"
    );
    let receipt = request(&mut app, "terminal.job.get", json!({"condition":condition}));
    assert_eq!(receipt["result"]["phase"], "awaiting_enrollment");
    assert!(receipt["result"]["diagnostic"]
        .as_str()
        .unwrap()
        .contains("confirmation_required"));
    assert_eq!(
        request(
            &mut app,
            "terminal.condition",
            json!({"pane_id":condition["terminal"]["pane_id"]})
        )["result"]["condition"],
        condition["terminal"]
    );
    app.state.confirm_close = false;
    request(
        &mut app,
        "terminal.job.close",
        json!({"condition":condition}),
    );
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_zoomed_parent_rejects_before_launch_without_focus_change() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-zoom-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(&script, "printf launched > sidecar; sleep 30\n").unwrap();
    app.state.workspaces[0].tabs[0].zoomed = true;
    let before = request(&mut app, "pane.list", json!({}));
    let condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))["result"]
        ["condition"]
        .clone();
    let result = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    assert_eq!(result["error"]["code"], "managed_parent_zoomed", "{result}");
    assert_eq!(request(&mut app, "pane.list", json!({})), before);
    assert!(app.state.workspaces[0].tabs[0].zoomed);
    assert!(!root.join("sidecar").exists());
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_receipt_expiry_never_evicts_a_released_live_runtime() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-retention-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(&script, "sleep 30\n").unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launch = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launch["result"]["condition"].clone();
    request(
        &mut app,
        "terminal.job.release",
        json!({"condition":condition}),
    );
    let live = std::collections::HashSet::from([condition["terminal"]["launch_id"]
        .as_str()
        .unwrap()
        .to_string()]);
    app.managed_jobs
        .retire_absent(&live, Instant::now() + Duration::from_secs(3600));
    assert_eq!(
        request(&mut app, "terminal.job.get", json!({"condition":condition}))["result"]["phase"],
        "released"
    );
    request(
        &mut app,
        "pane.close",
        json!({"pane_id":condition["terminal"]["pane_id"]}),
    );
    let id = condition["job_id"].as_str().unwrap();
    let absent = app.managed_jobs.jobs[id].absent_since.unwrap();
    app.managed_jobs.retire_absent(
        &std::collections::HashSet::new(),
        absent + Duration::from_secs(1799),
    );
    assert_eq!(
        request(&mut app, "terminal.job.get", json!({"condition":condition}))["result"]["phase"],
        "released"
    );
    app.managed_jobs.retire_absent(
        &std::collections::HashSet::new(),
        absent + Duration::from_secs(1800),
    );
    assert_eq!(
        request(&mut app, "terminal.job.get", json!({"condition":condition}))["error"]["code"],
        "job_not_found"
    );
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_resolved_protection_clears_cleanup_diagnostic_and_exit_is_immutable() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-resolved-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(
        &script,
        "while [ ! -f release ]; do sleep 0.01; done; exit 7\n",
    )
    .unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launch = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launch["result"]["condition"].clone();
    let pane = app
        .parse_current_public_pane_id(condition["terminal"]["pane_id"].as_str().unwrap())
        .unwrap()
        .1;
    app.state.confirm_close = true;
    let membership = crate::workspace::WorktreeSpaceMembership {
        key: "resolved-group".into(),
        label: "group".into(),
        repo_root: "/test".into(),
        checkout_path: "/test".into(),
        is_linked_worktree: false,
    };
    app.state.workspaces[0].worktree_space = Some(membership.clone());
    let mut other = crate::workspace::Workspace::test_new("sibling");
    other.worktree_space = Some(crate::workspace::WorktreeSpaceMembership {
        is_linked_worktree: true,
        checkout_path: "/test/child".into(),
        ..membership
    });
    app.state.workspaces.push(other);
    app.state.ensure_test_terminals();
    request(&mut app, "pane.close", json!({"pane_id":parent}));
    assert_eq!(
        request(
            &mut app,
            "terminal.job.close",
            json!({"condition":condition})
        )["error"]["code"],
        "confirmation_required"
    );
    app.state.confirm_close = false;
    std::fs::write(root.join("release"), "ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = request(&mut app, "terminal.job.get", json!({"condition":condition}));
        if status["result"]["phase"] == "exited" {
            assert_eq!(status["result"]["exit"]["code"], 7);
            assert!(status["result"]["diagnostic"].is_null(), "{status}");
            break;
        }
        assert!(Instant::now() < deadline, "{status}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        request(
            &mut app,
            "terminal.condition",
            json!({"pane_id":condition["terminal"]["pane_id"]})
        )["error"]["code"],
        "terminal_not_found"
    );
    app.event_tx
        .send(crate::events::AppEvent::ManagedJobExited {
            job_id: condition["job_id"].as_str().unwrap().into(),
            pane_id: pane,
            code: Some(42),
            exit_reason: crate::platform::ChildExitReason::Exited,
        })
        .await
        .unwrap();
    assert_eq!(
        request(&mut app, "terminal.job.get", json!({"condition":condition}))["result"]["exit"]
            ["code"],
        7
    );
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_oversized_public_request_rejects_before_allocation() {
    let (mut app, parent) = app();
    let condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))["result"]
        ["condition"]
        .clone();
    let before = request(&mut app, "pane.list", json!({}));
    let response = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":condition,"argv":["/bin/bash","/private/tmp/should-not-launch"],"cwd":"/private/tmp","env":{"OVERSIZED":"x".repeat(17000)},"direction":"right"}),
    );
    assert_eq!(response["error"]["code"], "control_frame_too_large");
    assert_eq!(request(&mut app, "pane.list", json!({})), before);
    assert_eq!(app.managed_jobs.jobs.len(), 0);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_known_enrollment_failures_are_visible_and_keep_resource_ownership() {
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!(
        "herdr-managed-enrollment-failure-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(&script, "sleep 30\n").unwrap();
    for absent_anchor in [false, true] {
        let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
            ["result"]["condition"]
            .clone();
        let launch = request(
            &mut app,
            "terminal.job.launch",
            json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
        );
        let condition = launch["result"]["condition"].clone();
        let enrollment = app
            .managed_jobs
            .jobs
            .get_mut(condition["job_id"].as_str().unwrap())
            .unwrap()
            .enrollment
            .as_mut()
            .unwrap();
        // Deterministic unavailable-proof/clock injection stays in this test fixture.
        if absent_anchor {
            enrollment.anchor = None;
        } else {
            enrollment.deadline = Instant::now() - Duration::from_secs(1);
        }
        let status = request(&mut app, "terminal.job.get", json!({"condition":condition}));
        assert_eq!(status["result"]["phase"], "enrollment_failed", "{status}");
        assert!(status["result"]["diagnostic"].is_string());
        assert!(status["result"]["agent"].is_null());
        assert_eq!(
            request(
                &mut app,
                "terminal.condition",
                json!({"pane_id":condition["terminal"]["pane_id"]})
            )["result"]["condition"],
            condition["terminal"]
        );
        assert_eq!(
            request(
                &mut app,
                "terminal.job.rename",
                json!({"condition":condition,"label":"failed but still owned"})
            )["result"]["pane"]["label"],
            "failed but still owned"
        );
        assert_eq!(
            request(
                &mut app,
                "terminal.job.close",
                json!({"condition":condition})
            )["result"]["phase"],
            "closed"
        );
    }
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn managed_job_close_hangups_only_its_pty_despite_stale_numeric_pid() {
    use std::os::unix::process::CommandExt as _;
    struct OwnedFixtureProcess(std::process::Child);
    impl Drop for OwnedFixtureProcess {
        fn drop(&mut self) {
            if self.0.try_wait().is_ok_and(|status| status.is_none()) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let mut unrelated = std::process::Command::new("/bin/sleep");
    unrelated
        .arg("30")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // SAFETY: this private fixture creates a separate session using one async-signal-safe syscall.
    unsafe {
        unrelated.pre_exec(|| {
            if libc::setsid() < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut unrelated = OwnedFixtureProcess(unrelated.spawn().unwrap());
    let (mut app, parent) = app();
    let root = std::env::temp_dir().join(format!("herdr-managed-teardown-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let script = root.join("launch.sh");
    std::fs::write(&script,"trap 'printf hangup > hangup.sidecar; exit 0' HUP; printf ready > ready.sidecar; while :; do sleep 0.01; done\n").unwrap();
    let parent_condition = request(&mut app, "terminal.condition", json!({"pane_id":parent}))
        ["result"]["condition"]
        .clone();
    let launch = request(
        &mut app,
        "terminal.job.launch",
        json!({"parent":parent_condition,"argv":["/bin/bash",script],"cwd":root,"env":{},"direction":"right"}),
    );
    let condition = launch["result"]["condition"].clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !root.join("ready.sidecar").exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (ws, pane) = app
        .parse_current_public_pane_id(condition["terminal"]["pane_id"].as_str().unwrap())
        .unwrap();
    let terminal = app.state.workspaces[ws].terminal_id(pane).unwrap();
    let probe = app
        .terminal_runtimes
        .get(terminal)
        .unwrap()
        .test_pid_shutdown_probe();
    app.terminal_runtimes
        .get(terminal)
        .unwrap()
        .test_inject_reaped_child_pid(unrelated.0.id());
    let closed = request(
        &mut app,
        "terminal.job.close",
        json!({"condition":condition}),
    );
    assert_eq!(closed["result"]["phase"], "closed", "{closed}");
    assert_eq!(
        probe.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "managed teardown called the PID enumeration/signaling path"
    );
    assert!(unrelated.0.try_wait().unwrap().is_none(),"managed teardown signaled a recycled numeric PID belonging to an unrelated private session");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = request(&mut app, "terminal.job.get", json!({"condition":condition}));
        if root.join("hangup.sidecar").exists() && status["result"]["exit"]["code"] == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "{status}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        std::fs::read_to_string(root.join("hangup.sidecar")).unwrap(),
        "hangup"
    );
    assert_eq!(
        request(
            &mut app,
            "terminal.condition",
            json!({"pane_id":condition["terminal"]["pane_id"]})
        )["error"]["code"],
        "terminal_not_found"
    );
    assert!(unrelated.0.try_wait().unwrap().is_none());
    drop(app);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn managed_capabilities_accepts_trusted_endpoint_id_prefix_for_external_128_byte_id() {
    let (mut app, _) = app();
    let id = format!("endpoint:boot:42:{}", "x".repeat(128));
    assert!(id.len() > 128);
    let request =
        serde_json::from_value(json!({"id":id,"method":"terminal.job.capabilities","params":{}}))
            .unwrap();
    let response: Value = serde_json::from_str(&app.handle_api_request(request)).unwrap();
    assert_eq!(response["id"], id);
    assert_eq!(response["result"]["type"], "managed_terminal_capabilities");
}
