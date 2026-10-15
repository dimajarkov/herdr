//! Persistent, connection-local cooperative traffic on the public JSON socket.
use crate::api::schema::{Method, Request};
use crate::api::{ApiRequestMessage, ApiRequestSender, ControlConnection, ControlOutbound};
use crate::ipc::{
    poll_local_stream_read_count, set_local_stream_polling, LocalStream, LocalStreamReadCount,
};
use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, TryRecvError},
    Arc,
};
use std::time::{Duration, Instant};

const MAX_FRAME: usize = 16 * 1024;
const QUEUE_LIMIT: usize = 16;
const WRITE_BOUND: Duration = Duration::from_secs(5);
const FRAME_BOUND: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(5);
struct ConnectionLease {
    alive: Arc<AtomicBool>,
    diagnostic: Arc<std::sync::Mutex<Option<String>>>,
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        self.diagnostic
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_or_insert_with(|| "control peer disconnected or original route revoked".into());
    }
}
fn diagnose(context: &ControlConnection, message: impl Into<String>) {
    let mut cause = context
        .diagnostic
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if cause.is_none() {
        *cause = Some(message.into());
    }
}
fn rejected_response(context: &ControlConnection, response: &str) {
    let code = serde_json::from_str::<serde_json::Value>(response)
        .ok()
        .and_then(|v| v.get("error")?.get("code")?.as_str().map(str::to_string))
        .filter(|s| s.len() <= 64 && s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'));
    diagnose(
        context,
        code.map_or_else(
            || "control request rejected".into(),
            |code| format!("control request rejected: {code}"),
        ),
    );
}

fn connection_id() -> io::Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| io::Error::other("control identity entropy unavailable"))?;
    let mut id = String::from("connection-");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut id, "{byte:02x}").expect("String formatting");
    }
    Ok(id)
}
fn write_frame(
    stream: &mut LocalStream,
    frame: &str,
    deadline: Instant,
    alive: Option<&AtomicBool>,
) -> io::Result<()> {
    if frame.len() + 1 > MAX_FRAME || frame.contains(['\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control output frame invalid or too large",
        ));
    }
    let mut bytes = frame.as_bytes();
    let newline = [b'\n'];
    for part in [bytes, &newline[..]] {
        bytes = part;
        while !bytes.is_empty() {
            if Instant::now() >= deadline || alive.is_some_and(|flag| !flag.load(Ordering::Acquire))
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "control output deadline or route revocation",
                ));
            }
            match stream.write(bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) => bytes = &bytes[written..],
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => std::thread::sleep(POLL),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}
fn error(stream: &mut LocalStream, id: String, code: &str, message: &str) -> io::Result<()> {
    write_frame(
        stream,
        &super::error_response_json(id, code, message.into()),
        Instant::now() + WRITE_BOUND,
        None,
    )
}
fn dispatch(
    request: Request,
    context: &ControlConnection,
    api_tx: &ApiRequestSender,
) -> io::Result<String> {
    let (respond_to, response) = mpsc::channel();
    api_tx
        .send(ApiRequestMessage {
            request,
            respond_to,
            response_write_complete: None,
            control: Some(context.clone()),
        })
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "control app unavailable"))?;
    response
        .recv_timeout(super::APP_RESPONSE_TIMEOUT)
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control app response unavailable"))
}
fn flush_outbound(
    stream: &mut LocalStream,
    outbound: &Receiver<ControlOutbound>,
    context: &ControlConnection,
) -> io::Result<()> {
    // Work accepted into an OS/native queue cannot be unsent; only omit envelopes
    // whose monotonic deadline expired before this transport begins writing.
    for _ in 0..QUEUE_LIMIT {
        match outbound.try_recv() {
            Ok(message) if message.deadline <= Instant::now() => continue,
            Ok(message) => write_frame(
                stream,
                &message.frame,
                message.deadline.min(Instant::now() + WRITE_BOUND),
                Some(&context.alive),
            )?,
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    Ok(())
}
pub(super) fn handle_control_connection(
    mut stream: LocalStream,
    first: Request,
    first_bytes: usize,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
) -> io::Result<()> {
    set_local_stream_polling(&mut stream, true)?;
    if first_bytes > MAX_FRAME {
        return error(
            &mut stream,
            first.id,
            "control_frame_too_large",
            "control registration exceeds 16KiB",
        );
    }
    let peer = match crate::platform::control_peer_identity(&stream) {
        Ok(peer) => peer,
        Err(_) => {
            return error(
                &mut stream,
                first.id,
                "control_peer_unavailable",
                "native local peer identity unavailable",
            )
        }
    };
    let (outbound, receiver) = mpsc::sync_channel(QUEUE_LIMIT);
    let context = ControlConnection {
        peer,
        connection: connection_id()?,
        outbound,
        alive: Arc::new(AtomicBool::new(true)),
        diagnostic: Arc::new(std::sync::Mutex::new(None)),
    };
    let _lease = ConnectionLease {
        alive: Arc::clone(&context.alive),
        diagnostic: Arc::clone(&context.diagnostic),
    };
    let result = serve(&mut stream, first, api_tx, running, &context, &receiver);
    if let Err(error) = &result {
        diagnose(
            &context,
            format!("control transport failed: {:?}", error.kind()),
        );
    }
    result
}
fn partial_frame_expired(
    stream: &mut LocalStream,
    context: &ControlConnection,
    since: Option<Instant>,
) -> io::Result<bool> {
    if since.is_some_and(|start| start.elapsed() >= FRAME_BOUND) {
        diagnose(context, "partial control frame exceeded 5s");
        error(
            stream,
            String::new(),
            "control_frame_timeout",
            "partial control frame exceeded deadline",
        )?;
        return Ok(true);
    }
    Ok(false)
}
fn serve(
    stream: &mut LocalStream,
    first: Request,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
    context: &ControlConnection,
    receiver: &Receiver<ControlOutbound>,
) -> io::Result<()> {
    let response = dispatch(first, context, api_tx)?;
    write_frame(
        stream,
        &response,
        Instant::now() + WRITE_BOUND,
        Some(&context.alive),
    )?;
    if super::api_response_outcome(&response) != "ok" {
        rejected_response(context, &response);
        return Ok(());
    }
    let mut frame = Vec::with_capacity(MAX_FRAME);
    let mut partial_since: Option<Instant> = None;
    let mut chunk = [0u8; 4096];
    while running.load(Ordering::Acquire) && context.alive.load(Ordering::Acquire) {
        flush_outbound(stream, receiver, context)?;
        if partial_frame_expired(stream, context, partial_since)? {
            return Ok(());
        }
        match poll_local_stream_read_count(stream, &mut chunk)? {
            LocalStreamReadCount::Closed => return Ok(()),
            LocalStreamReadCount::Pending => {
                std::thread::sleep(POLL);
            }
            LocalStreamReadCount::Data(count) => {
                for byte in &chunk[..count] {
                    // App dispatch/output flushing can await across a chunk. Recheck
                    // before accepting more bytes, including a final newline.
                    if partial_frame_expired(stream, context, partial_since)? {
                        return Ok(());
                    }
                    partial_since.get_or_insert_with(Instant::now);
                    frame.push(*byte);
                    if frame.len() > MAX_FRAME {
                        diagnose(context, "incoming control frame exceeds 16KiB");
                        error(
                            stream,
                            String::new(),
                            "control_frame_too_large",
                            "control frame exceeds 16KiB",
                        )?;
                        return Ok(());
                    }
                    if *byte != b'\n' {
                        continue;
                    }
                    let request = std::str::from_utf8(&frame[..frame.len() - 1])
                        .ok()
                        .and_then(|text| serde_json::from_str::<Request>(text).ok());
                    frame.clear();
                    partial_since = None;
                    let Some(request) = request else {
                        diagnose(context, "invalid control request envelope");
                        error(
                            stream,
                            String::new(),
                            "invalid_control_request",
                            "invalid bounded control request",
                        )?;
                        return Ok(());
                    };
                    if request.id.is_empty()
                        || request.id.len() > 128
                        || !matches!(
                            &request.method,
                            Method::AgentControlAck(_) | Method::AgentControlObserve(_)
                        )
                    {
                        diagnose(context, "invalid connection-local control method");
                        error(
                            stream,
                            request.id,
                            "invalid_control_method",
                            "enrolled stream accepts only acknowledgment and observation",
                        )?;
                        return Ok(());
                    }
                    let response = dispatch(request, context, api_tx)?;
                    write_frame(
                        stream,
                        &response,
                        Instant::now() + WRITE_BOUND,
                        Some(&context.alive),
                    )?;
                    // Backend replay/binding errors terminate exactly this route.
                    if super::api_response_outcome(&response) != "ok" {
                        rejected_response(context, &response);
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use crate::api::{ApiRequestMessage, EventHub};
    use crate::ipc::{bind_local_listener, connect_local_stream};
    use interprocess::local_socket::traits::Listener as _;
    use std::io::{BufRead, BufReader, Write};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    type RegisteredSocket = (
        crate::ipc::LocalStream,
        std::thread::JoinHandle<std::io::Result<()>>,
        crate::api::ControlConnection,
        tokio::sync::mpsc::UnboundedReceiver<ApiRequestMessage>,
        std::path::PathBuf,
    );
    fn registered_socket() -> Option<RegisteredSocket> {
        if !crate::platform::cooperative_control_supported() {
            return None;
        }
        let dir = std::env::temp_dir().join(format!(
            "hc46-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let listener = bind_local_listener(&dir.join("socket")).unwrap();
        let mut client = connect_local_stream(&dir.join("socket")).unwrap();
        let stream = listener.accept().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let running = Arc::new(AtomicBool::new(true));
        let worker = std::thread::spawn(move || {
            super::super::handle_connection(stream, &tx, &EventHub::default(), &running, None)
        });
        writeln!(client,r#"{{"id":"register","method":"agent.control.register","params":{{"capability":"synthetic","version":1,"epoch":"epoch","session":"session","sdk_features":[]}}}}"#).unwrap();
        let request = rx.blocking_recv().unwrap();
        let context = request.control.unwrap();
        request
            .respond_to
            .send(r#"{"id":"register","result":{"type":"agent_control_registered"}}"#.into())
            .unwrap();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        assert!(response.contains("agent_control_registered"));
        Some((client, worker, context, rx, dir))
    }
    fn discard_directory(path: std::path::PathBuf) {
        let _ = std::fs::remove_file(path.join("socket"));
        let _ = std::fs::remove_dir(path);
    }

    fn initial_exchange(frame: &str) -> (String, usize) {
        let path = std::env::temp_dir().join(format!(
            "hc46-first-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = bind_local_listener(&path).unwrap();
        let mut client = connect_local_stream(&path).unwrap();
        let stream = listener.accept().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let app = std::thread::spawn(move || {
            let mut count = 0;
            while let Some(request) = rx.blocking_recv() {
                count += 1;
                request
                    .respond_to
                    .send(r#"{"id":"first","result":{"type":"ok"}}"#.into())
                    .unwrap();
            }
            count
        });
        let worker = std::thread::spawn(move || {
            super::super::handle_connection(
                stream,
                &tx,
                &EventHub::default(),
                &Arc::new(AtomicBool::new(true)),
                None,
            )
        });
        writeln!(client, "{frame}").unwrap();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        drop(client);
        worker.join().unwrap().unwrap();
        let count = app.join().unwrap();
        let _ = std::fs::remove_file(path);
        (response, count)
    }

    #[test]
    fn oversized_managed_initial_requests_are_refused_before_app_dispatch() {
        let condition = serde_json::json!({"pane_id":"w1:p1","tab_id":"w1:t1","terminal_id":"terminal","launch_id":"launch"});
        for frame in [
            serde_json::json!({"id":"first","method":"terminal.job.launch","params":{"parent":condition,"argv":["/bin/bash"],"cwd":"/private/tmp","env":{"TEST":"x".repeat(17000)},"direction":"right"}}).to_string(),
            serde_json::json!({"id":"first","method":"agent.control.register","params":{"capability":"x".repeat(17000),"version":1,"epoch":"epoch","session":"session","sdk_features":[]}}).to_string(),
        ] {
            let (response, dispatched) = initial_exchange(&frame);
            assert!(response.contains("control_frame_too_large"), "{response}");
            assert_eq!(dispatched, 0);
        }
        let legacy = serde_json::json!({"id":"first","method":"pane.get","params":{"pane_id":"w1:p1"},"legacy_padding":"x".repeat(17000)}).to_string();
        let (response, dispatched) = initial_exchange(&legacy);
        assert!(response.contains("result"));
        assert_eq!(
            dispatched, 1,
            "legacy generic ingress keeps its existing bound"
        );
    }

    #[test]
    fn oversized_managed_request_id_is_uncorrelated_and_refused_before_dispatch() {
        let frame = serde_json::json!({"id":"x".repeat(129),"method":"terminal.job.capabilities","params":{}}).to_string();
        let (response, dispatched) = initial_exchange(&frame);
        let reply: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(reply["id"], "");
        assert_eq!(reply["error"]["code"], "invalid_control_id");
        assert_eq!(dispatched, 0);
        assert!(response.len() < 512);
        let legacy = serde_json::json!({"id":"x".repeat(129),"method":"pane.get","params":{"pane_id":"w1:p1"}}).to_string();
        let (_, dispatched) = initial_exchange(&legacy);
        assert_eq!(dispatched, 1);
    }

    #[test]
    fn malformed_managed_registration_never_echoes_capability_or_unknown_fields() {
        for frame in [
            r#"{"id":"first","method":"agent.control.register","params":{"synthetic-private-capability":true}}"#,
            r#"{"id":"first","method":"agent.control.register","params":{"capability":"synthetic-private-capability","version":"synthetic-private-capability"}}"#,
            r#"{"id":"first","method":"agent.control.register","params":{"capability":"synthetic-private-capability" BROKEN}}"#,
        ] {
            let (response, dispatched) = initial_exchange(frame);
            assert_eq!(dispatched, 0);
            assert!(response.contains("invalid_request"));
            assert!(
                !response.contains("synthetic-private-capability"),
                "{response}"
            );
            assert!(response.len() <= 16384);
        }
    }

    #[test]
    fn malformed_and_oversized_control_frames_revoke_only_their_route_without_app_dispatch() {
        for frame in [
            "null\n".to_string(),
            "[]\n".into(),
            "{malformed}\n".into(),
            "x".repeat(16385),
        ] {
            let Some((mut client, worker, context, mut requests, dir)) = registered_socket() else {
                return;
            };
            client.write_all(frame.as_bytes()).unwrap();
            let mut response = String::new();
            BufReader::new(&mut client)
                .read_line(&mut response)
                .unwrap();
            assert!(
                response.contains("invalid_control_request")
                    || response.contains("control_frame_too_large")
            );
            worker.join().unwrap().unwrap();
            assert!(!context.alive.load(Ordering::Acquire));
            assert!(requests.try_recv().is_err());
            assert!(context
                .diagnostic
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|reason| reason.contains("frame") || reason.contains("request")));
            drop(client);
            discard_directory(dir);
        }
    }

    #[test]
    fn expired_outbound_is_dropped_before_delivery_and_live_output_stays_on_the_enrolled_socket() {
        let Some((mut client, worker, context, _requests, dir)) = registered_socket() else {
            return;
        };
        context
            .outbound
            .try_send(crate::api::ControlOutbound {
                frame: r#"{"id":"expired"}"#.into(),
                deadline: std::time::Instant::now() - std::time::Duration::from_millis(1),
            })
            .unwrap();
        context
            .outbound
            .try_send(crate::api::ControlOutbound {
                frame: r#"{"id":"live"}"#.into(),
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
            })
            .unwrap();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        assert_eq!(response.trim(), r#"{"id":"live"}"#);
        drop(client);
        worker.join().unwrap().unwrap();
        assert!(!context.alive.load(Ordering::Acquire));
        discard_directory(dir);
    }

    #[test]
    fn backend_replay_error_closes_the_original_control_route_and_preserves_other_api_requests() {
        let Some((mut client, worker, context, mut requests, dir)) = registered_socket() else {
            return;
        };
        let observe = r#"{"id":"replay","method":"agent.control.observe","params":{"condition":{"job":{"terminal":{"pane_id":"w1:p1","tab_id":"w1:t1","terminal_id":"terminal","launch_id":"launch"},"job_id":"job"},"connection":"connection","epoch":"epoch","session":"session"},"sequence":1,"native_request_ids":[],"busy":false}}"#;
        writeln!(client, "{observe}").unwrap();
        let request = requests.blocking_recv().unwrap();
        assert_eq!(request.control.unwrap().connection, context.connection);
        request.respond_to.send(r#"{"id":"replay","error":{"code":"invalid_observation","message":"replayed sequence"}}"#.into()).unwrap();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        assert!(response.contains("invalid_observation"));
        worker.join().unwrap().unwrap();
        assert!(!context.alive.load(Ordering::Acquire));
        assert_eq!(
            context.diagnostic.lock().unwrap().as_deref(),
            Some("control request rejected: invalid_observation")
        );
        drop(client);
        discard_directory(dir);
        // The generic transport retains its original one-response lifecycle and carries no control authority.
        let path = std::env::temp_dir().join(format!(
            "hc46-generic-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = bind_local_listener(&path).unwrap();
        let mut client = connect_local_stream(&path).unwrap();
        let stream = listener.accept().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let running = Arc::new(AtomicBool::new(true));
        let worker = std::thread::spawn(move || {
            super::super::handle_connection(stream, &tx, &EventHub::default(), &running, None)
        });
        writeln!(
            client,
            r#"{{"id":"generic","method":"pane.get","params":{{"pane_id":"w1:p1"}}}}"#
        )
        .unwrap();
        let request = rx.blocking_recv().unwrap();
        assert!(request.control.is_none());
        request
            .respond_to
            .send(r#"{"id":"generic","result":{"type":"ok"}}"#.into())
            .unwrap();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        assert!(response.contains("generic"));
        drop(client);
        worker.join().unwrap().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn incomplete_control_frame_expires_without_dispatch_and_idle_route_remains_usable() {
        let Some((mut client, worker, context, mut requests, dir)) = registered_socket() else {
            return;
        };
        // An empty idle route has no frame deadline. A received prefix does.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(context.alive.load(Ordering::Acquire));
        client.write_all(b"{\"id\":").unwrap();
        let started = std::time::Instant::now();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        assert!(response.contains("control_frame_timeout"));
        assert!(started.elapsed() >= std::time::Duration::from_secs(5));
        assert!(started.elapsed() < std::time::Duration::from_secs(7));
        worker.join().unwrap().unwrap();
        assert!(requests.try_recv().is_err());
        assert!(!context.alive.load(Ordering::Acquire));
        assert_eq!(
            context.diagnostic.lock().unwrap().as_deref(),
            Some("partial control frame exceeded 5s")
        );
        drop(client);
        discard_directory(dir);
    }

    #[test]
    fn unread_control_peer_has_bounded_writes_and_actionable_route_local_failure() {
        let Some((client, worker, context, _requests, dir)) = registered_socket() else {
            return;
        };
        let start = std::time::Instant::now();
        let mut sent = 0;
        let frame = serde_json::json!({"id":"pressure","text":"x".repeat(16000)}).to_string();
        while context.alive.load(Ordering::Acquire)
            && start.elapsed() < std::time::Duration::from_secs(3)
        {
            if context
                .outbound
                .try_send(crate::api::ControlOutbound {
                    frame: frame.clone(),
                    deadline: std::time::Instant::now() + std::time::Duration::from_millis(100),
                })
                .is_ok()
            {
                sent += 1;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(sent > 0);
        assert!(
            !context.alive.load(Ordering::Acquire),
            "unread socket must not pin a writer indefinitely"
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(3));
        let error = worker.join().unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(
            context.diagnostic.lock().unwrap().as_deref(),
            Some("control transport failed: TimedOut")
        );
        drop(client);
        discard_directory(dir);
    }

    #[test]
    fn control_socket_stays_open_and_preserves_exact_connection_context_until_disconnect() {
        if !crate::platform::cooperative_control_supported() {
            return;
        }
        let dir = std::env::temp_dir().join(format!(
            "herdr-control-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.sock");
        let listener = bind_local_listener(&path).unwrap();
        let mut client = connect_local_stream(&path).unwrap();
        let server = listener.accept().unwrap();
        let running = Arc::new(AtomicBool::new(true));
        let (api_tx, mut api_rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let responder = std::thread::spawn(move || {
            let first = api_rx.blocking_recv().unwrap();
            let context = first.control.clone().expect("trusted control context");
            assert_eq!(context.peer.pid, std::process::id());
            first
                .respond_to
                .send(
                    r#"{"id":"register","result":{"type":"agent_control_registered"}}"#.to_string(),
                )
                .unwrap();
            context
                .outbound
                .try_send(crate::api::ControlOutbound {
                    frame: r#"{"id":"operation","method":"agent.control.prompt","params":{}}"#
                        .into(),
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
                })
                .unwrap();
            let second = api_rx.blocking_recv().unwrap();
            let next = second.control.unwrap();
            assert_eq!(next.connection, context.connection);
            assert!(Arc::ptr_eq(&next.alive, &context.alive));
            second
                .respond_to
                .send(r#"{"id":"observe","result":{"type":"ok"}}"#.into())
                .unwrap();
            context
        });
        let worker = std::thread::spawn(move || {
            super::super::handle_connection(server, &api_tx, &EventHub::default(), &running, None)
        });
        let register = r#"{"id":"register","method":"agent.control.register","params":{"capability":"synthetic-private","version":1,"epoch":"epoch","session":"session","sdk_features":[]}}"#;
        writeln!(client, "{register}").unwrap();
        let mut reader = BufReader::new(&mut client);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("agent_control_registered"));
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("agent.control.prompt"));
        drop(reader);
        let observe = r#"{"id":"observe","method":"agent.control.observe","params":{"condition":{"job":{"terminal":{"pane_id":"w1:p1","tab_id":"w1:t1","terminal_id":"terminal","launch_id":"launch"},"job_id":"job"},"connection":"connection","epoch":"epoch","session":"session"},"sequence":1,"native_request_ids":[],"busy":false}}"#;
        writeln!(client, "{observe}").unwrap();
        let mut line = String::new();
        BufReader::new(&mut client).read_line(&mut line).unwrap();
        assert!(line.contains("observe"));
        let context = responder.join().unwrap();
        drop(client);
        worker.join().unwrap().unwrap();
        assert!(!context.alive.load(Ordering::Acquire));
    }
}
