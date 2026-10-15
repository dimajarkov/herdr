#![cfg(unix)]

pub mod support;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn guarded_terminal_cli_old_server_rejection_never_retries_unguarded_close() {
    assert_old_server_rejects(
        &["terminal", "close-if"],
        "terminal.close_if",
        json!({"pane_id":"w1:p1","tab_id":"w1:t1","terminal_id":"original","launch_id":"original-launch"}),
        "runtime",
    );
}

#[test]
fn managed_job_cli_old_server_rejection_never_downgrades_release_or_close() {
    for action in ["release", "close"] {
        assert_old_server_rejects(
            &["terminal", "job", action],
            &format!("terminal.job.{action}"),
            json!({"terminal":{"pane_id":"w1:p1","tab_id":"w1:t1","terminal_id":"original","launch_id":"original-launch"},"job_id":"original-job"}),
            action,
        );
    }
}

fn assert_old_server_rejects(args: &[&str], method: &str, condition: Value, fixture: &str) {
    assert_cli_reply(args, method, condition, fixture, None);
}

#[test]
fn managed_job_cli_capabilities_and_get_round_trip_public_socket() {
    assert_cli_reply(
        &["terminal", "job", "capabilities"],
        "terminal.job.capabilities",
        json!(null),
        "caps",
        Some(
            json!({"type":"managed_terminal_capabilities","job_ownership":true,"cooperative_control":true,"version":1,"max_frame_bytes":16384,"required_sdk_features":["input_request_lifecycle_v1","message_admission_v1","external_request_correlation_v1"]}),
        ),
    );
    let condition = json!({"terminal":{"pane_id":"w1:p2","tab_id":"w1:t1","terminal_id":"job-terminal","launch_id":"job-runtime"},"job_id":"job"});
    assert_cli_reply(
        &["terminal", "job", "get"],
        "terminal.job.get",
        condition.clone(),
        "get",
        Some(
            json!({"type":"managed_terminal","condition":condition,"phase":"released","agent":null,"exit":null,"diagnostic":null,"native_scope_id":null,"native_request_ids":[],"busy":false}),
        ),
    );
}

fn assert_cli_reply(
    args: &[&str],
    method: &str,
    condition: Value,
    fixture: &str,
    result: Option<Value>,
) {
    let base = std::env::temp_dir().join(format!(
        "herdr-guarded-cli-{fixture}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&base).unwrap();
    let socket = base.join("old.sock");
    let condition_file = base.join("condition.json");
    std::fs::write(&condition_file, condition.to_string()).unwrap();
    let listener = UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr"));
    command.args(args);
    if method != "terminal.job.capabilities" {
        command.args(["--condition", condition_file.to_str().unwrap()]);
    }
    let mut child = command
        .env("HERDR_SOCKET_PATH", &socket)
        .env("XDG_CONFIG_HOME", &base)
        .env("XDG_RUNTIME_DIR", &base)
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_ENV")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut requests = Vec::new();
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut line = String::new();
                BufReader::new(stream.try_clone().unwrap())
                    .read_line(&mut line)
                    .unwrap();
                let req: Value = serde_json::from_str(&line).unwrap();
                let response = if req["method"] == "ping" {
                    json!({"id":req["id"],"result":{"type":"pong","version":"0.9.4","protocol":support::CURRENT_PROTOCOL}})
                } else if let Some(result) = &result {
                    json!({"id":req["id"],"result":result})
                } else {
                    json!({"id":req["id"],"error":{"code":"unknown_method","message":"old server has no guarded lifecycle methods"}})
                };
                writeln!(stream, "{response}").unwrap();
                stream.flush().unwrap();
                requests.push(req);
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                assert!(Instant::now() < deadline, "CLI timed out");
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("{err}"),
        }
    }
    let output = child.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(if result.is_some() { 0 } else { 1 }),
        "{output:?}"
    );
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[0]["method"], "ping");
    assert_eq!(requests[1]["method"], method);
    if method != "terminal.job.capabilities" {
        assert_eq!(requests[1]["params"]["condition"], condition);
    }
    if result.is_none() {
        assert!(String::from_utf8_lossy(&output.stderr).contains("unknown_method"));
    }
    drop(listener);
    std::fs::remove_dir_all(base).unwrap();
}
