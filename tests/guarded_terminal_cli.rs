#![cfg(unix)]

pub mod support;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn guarded_terminal_cli_old_server_rejection_never_retries_unguarded_close() {
    let base = std::env::temp_dir().join(format!("herdr-guarded-cli-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    let socket = base.join("old.sock");
    let condition_file = base.join("condition.json");
    let condition = json!({"pane_id":"w1:p1","tab_id":"w1:t1","terminal_id":"original","launch_id":"original-launch"});
    std::fs::write(&condition_file, condition.to_string()).unwrap();
    let listener = UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args([
            "terminal",
            "close-if",
            "--condition",
            condition_file.to_str().unwrap(),
        ])
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
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[0]["method"], "ping");
    assert_eq!(requests[1]["method"], "terminal.close_if");
    assert_eq!(requests[1]["params"]["condition"], condition);
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown_method"));
    drop(listener);
    std::fs::remove_dir_all(base).unwrap();
}
