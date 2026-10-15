use super::{capture_control_process_anchor, control_peer_identity, validate_control_peer};
use interprocess::local_socket::{
    prelude::*, GenericFilePath, ListenerNonblockingMode, ListenerOptions,
};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::io::{Read, Write};
use std::time::{Duration, Instant};

#[test]
fn control_peer_actual_socket_accepts_original_pty_wrapper() {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).expect("random test identity");
    let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let directory = std::path::Path::new("/private/tmp").join(format!("hc46-{suffix}"));
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .expect("private test directory");
    let path = directory.join("peer.sock");
    let name = path
        .as_path()
        .to_fs_name::<GenericFilePath>()
        .expect("socket name");
    let listener = ListenerOptions::new()
        .name(name)
        .nonblocking(ListenerNonblockingMode::Accept)
        .create_sync()
        .expect("listener");
    let pair = native_pty_system()
        .openpty(PtySize::default())
        .expect("native PTY");
    let mut command = CommandBuilder::new("/usr/bin/python3");
    command.args(["-c", "import socket,sys; s=socket.socket(socket.AF_UNIX); s.settimeout(5); s.connect(sys.argv[1]); s.sendall(b'x'); s.recv(1)"]);
    command.arg(&path);
    let mut child = pair.slave.spawn_command(command).expect("PTY child");
    let pid = child.process_id().expect("original wrapper pid");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match listener.accept() {
            Ok(stream) => break stream,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10))
            }
            Err(error) => panic!("native accept failed: {error}"),
        }
    };
    let mut ready = [0];
    stream.read_exact(&mut ready).expect("child ready");
    let proof = (|| {
        let peer = control_peer_identity(&stream)?;
        let anchor = capture_control_process_anchor(pid)?;
        assert_eq!(peer.pid, pid);
        validate_control_peer(&peer, &anchor)
    })();
    stream.write_all(b"x").expect("release only test child");
    child.wait().expect("reap only test child");
    drop(listener);
    drop(pair);
    std::fs::remove_dir_all(directory).expect("remove only test directory");
    proof.expect("accepted stream belongs to original PTY wrapper");
}

struct NativePeerFixture {
    directory: std::path::PathBuf,
    // Retain the original terminal until the owned wrapper has been reaped.
    _pair: portable_pty::PtyPair,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    stream: interprocess::local_socket::Stream,
    wrapper: u32,
}

impl NativePeerFixture {
    fn start(mode: &str) -> Self {
        use std::os::unix::fs::DirBuilderExt;
        let mut random = [0_u8; 8];
        getrandom::fill(&mut random).expect("private native fixture name");
        let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let directory = std::path::Path::new("/private/tmp").join(format!("hc46-{suffix}"));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .expect("private native fixture");
        let path = directory.join("peer.sock");
        let listener = ListenerOptions::new()
            .name(
                path.as_path()
                    .to_fs_name::<GenericFilePath>()
                    .expect("socket name"),
            )
            .nonblocking(ListenerNonblockingMode::Accept)
            .create_sync()
            .expect("private socket listener");
        let pair = native_pty_system()
            .openpty(PtySize::default())
            .expect("original PTY");
        let mut command = CommandBuilder::new("/usr/bin/python3");
        command.args([
            "-c",
            r#"
import os,socket,sys,signal,time
mode=sys.argv[2]
candidate=None
if mode == 'sibling_anchor':
    candidate=os.fork()
    if candidate == 0:
        time.sleep(5)
        sys.exit(0)
if mode in ('descendant','detached'):
    child=os.fork()
    if child:
        os.waitpid(child,0)
        sys.exit(0)
    if mode == 'detached':
        os.setsid()
s=socket.socket(socket.AF_UNIX)
s.settimeout(5)
s.connect(sys.argv[1])
s.sendall(b'x')
if candidate is not None:
    s.sendall(candidate.to_bytes(4,'big'))
s.recv(1)
if candidate is not None:
    os.kill(candidate,signal.SIGTERM)
    os.waitpid(candidate,0)
"#,
        ]);
        command.arg(&path);
        command.arg(mode);
        let mut child = pair
            .slave
            .spawn_command(command)
            .expect("original PTY wrapper");
        let wrapper = child.process_id().expect("original wrapper PID");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok(stream) => break stream,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("native fixture accept failed: {error}");
                }
            }
        };
        stream
            .set_recv_timeout(Some(Duration::from_secs(5)))
            .expect("bounded fixture read");
        let mut ready = [0];
        stream
            .read_exact(&mut ready)
            .expect("native peer handshake");
        drop(listener);
        Self {
            directory,
            _pair: pair,
            child,
            stream,
            wrapper,
        }
    }
}

impl Drop for NativePeerFixture {
    fn drop(&mut self) {
        let _ = self.stream.write_all(b"x");
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn control_peer_accepts_live_descendant_on_original_pty() {
    let fixture = NativePeerFixture::start("descendant");
    let peer = control_peer_identity(&fixture.stream).expect("actual child socket identity");
    let anchor = capture_control_process_anchor(fixture.wrapper).expect("original wrapper anchor");
    assert_ne!(
        peer.pid, anchor.pid,
        "socket actor really is a distinct child process"
    );
    validate_control_peer(&peer, &anchor)
        .expect("native live descendant shares original job terminal");
}

#[test]
fn control_peer_refuses_detached_descendant_and_forged_identity_or_anchor() {
    let detached = NativePeerFixture::start("detached");
    let peer = control_peer_identity(&detached.stream)
        .expect("same-UID detached socket still has kernel identity");
    let anchor = capture_control_process_anchor(detached.wrapper).expect("original parent PTY");
    assert!(
        validate_control_peer(&peer, &anchor).is_err(),
        "ancestry alone never authorizes a different terminal/session"
    );

    let fixture = NativePeerFixture::start("self");
    let peer = control_peer_identity(&fixture.stream).expect("accepted peer");
    let anchor = capture_control_process_anchor(fixture.wrapper).expect("launch anchor");
    let wrong_birth = super::super::ControlPeerIdentity {
        birth: peer.birth + 1,
        ..peer
    };
    let wrong_uid = super::super::ControlPeerIdentity {
        uid: peer.uid.wrapping_add(1),
        ..peer
    };
    let wrong_wrapper = super::super::ControlProcessAnchor {
        birth: anchor.birth + 1,
        ..anchor
    };
    let wrong_tty = super::super::ControlProcessAnchor {
        tty: anchor.tty.wrapping_add(1),
        ..anchor
    };
    let wrong_session = super::super::ControlProcessAnchor {
        session: anchor.session + 1,
        ..anchor
    };
    assert!(validate_control_peer(&wrong_birth, &anchor).is_err());
    assert!(validate_control_peer(&wrong_uid, &anchor).is_err());
    assert!(validate_control_peer(&peer, &wrong_wrapper).is_err());
    assert!(validate_control_peer(&peer, &wrong_tty).is_err());
    assert!(validate_control_peer(&peer, &wrong_session).is_err());
}

#[test]
fn control_peer_refuses_unrelated_original_job_and_dead_wrapper() {
    let mut fixture = NativePeerFixture::start("self");
    let sibling = NativePeerFixture::start("self");
    let peer = control_peer_identity(&fixture.stream).expect("actual peer");
    let anchor = capture_control_process_anchor(fixture.wrapper).expect("original anchor");
    let foreign =
        capture_control_process_anchor(sibling.wrapper).expect("separate real PTY anchor");
    assert!(
        validate_control_peer(&peer, &foreign).is_err(),
        "same user on another real PTY is not this job"
    );
    fixture.stream.write_all(b"x").expect("finish own wrapper");
    fixture.child.wait().expect("reap own wrapper");
    assert!(
        validate_control_peer(&peer, &anchor).is_err(),
        "cached accepted identity never adopts a later PID"
    );
    assert!(
        control_peer_identity(&fixture.stream).is_err(),
        "closed stream's vanished peer cannot be recaptured as live"
    );
}

#[test]
fn control_peer_refuses_same_terminal_non_descendant() {
    let mut fixture = NativePeerFixture::start("sibling_anchor");
    let peer = control_peer_identity(&fixture.stream).expect("actual original socket actor");
    let original = capture_control_process_anchor(fixture.wrapper).expect("original test PTY");
    let mut bytes = [0; 4];
    fixture
        .stream
        .read_exact(&mut bytes)
        .expect("owned test child's fixture identity");
    let child = u32::from_be_bytes(bytes);
    let different_anchor =
        capture_control_process_anchor(child).expect("actual live child on same PTY");
    assert_eq!(different_anchor.tty, original.tty);
    assert_eq!(different_anchor.session, original.session);
    assert_ne!(different_anchor.pid, peer.pid);
    assert!(
        validate_control_peer(&peer, &different_anchor).is_err(),
        "same UID, terminal and session cannot substitute for descendant membership"
    );
}
