//! Darwin-only proof for a cooperative stream and its original managed PTY job.
//!
//! Process observations are rechecked, not an atomic snapshot of the process tree.
use std::io;
use std::os::fd::{AsFd, AsRawFd, RawFd};

use super::super::{ControlPeerIdentity, ControlProcessAnchor};
use super::{bsdinfo_start_token, process_bsdinfo};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProcessObservation {
    pid: u32,
    uid: u32,
    birth: u64,
    parent: u32,
    tty: u32,
    session: i32,
}

fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn server_uid() -> io::Result<u32> {
    // SAFETY: uid getters have no pointer arguments or side effects.
    let (uid, real_uid) = unsafe { (libc::geteuid(), libc::getuid()) };
    if uid != real_uid {
        return Err(denied(
            "cooperative control requires an unambiguous server UID",
        ));
    }
    Ok(uid)
}

fn observe_process(pid: u32, uid: u32) -> io::Result<ProcessObservation> {
    let native_pid = libc::pid_t::try_from(pid)
        .map_err(|_| denied("cooperative control process PID is invalid"))?;
    if native_pid <= 0 {
        return Err(denied("cooperative control process PID is invalid"));
    }
    let info =
        process_bsdinfo(pid).ok_or_else(|| denied("cooperative control process is unavailable"))?;
    if info.pbi_pid != pid
        || info.pbi_status == libc::SZOMB
        || info.pbi_uid != uid
        || info.pbi_ruid != uid
    {
        return Err(denied(
            "cooperative control process identity is foreign or dead",
        ));
    }
    // SAFETY: getsid queries the live PID already obtained through typed proc_bsdinfo.
    let session = unsafe { libc::getsid(native_pid) };
    let birth = bsdinfo_start_token(&info);
    if session <= 0 || birth == 0 {
        return Err(denied("cooperative control process identity is incomplete"));
    }
    Ok(ProcessObservation {
        pid,
        uid,
        birth,
        parent: info.pbi_ppid,
        tty: info.e_tdev,
        session,
    })
}

fn socket_peer(fd: RawFd) -> io::Result<(u32, u32)> {
    let mut pid: libc::pid_t = 0;
    let mut length = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: pointers refer to initialized typed storage with its exact byte size.
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            std::ptr::from_mut(&mut pid).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    if length != std::mem::size_of::<libc::pid_t>() as libc::socklen_t || pid <= 0 {
        return Err(denied("accepted stream has no exact native peer PID"));
    }
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    // SAFETY: the accepted socket remains borrowed; both outputs use Darwin libc types.
    if unsafe { libc::getpeereid(fd, &mut uid, &mut gid) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((pid as u32, uid))
}

/// No caller-supplied PID is consulted. This is only the accepted stream's identity.
pub(crate) fn control_peer_identity(
    stream: &interprocess::local_socket::Stream,
) -> io::Result<ControlPeerIdentity> {
    let interprocess::local_socket::Stream::UdSocket(socket) = stream;
    let fd = socket.as_fd().as_raw_fd();
    let (pid, uid) = socket_peer(fd)?;
    if uid != server_uid()? {
        return Err(denied(
            "accepted control stream UID differs from server UID",
        ));
    }
    let first = observe_process(pid, uid)?;
    if socket_peer(fd)? != (pid, uid) || observe_process(pid, uid)? != first {
        return Err(denied(
            "accepted control stream process changed during proof",
        ));
    }
    Ok(ControlPeerIdentity {
        pid,
        uid,
        birth: first.birth,
    })
}

/// Capture once while the original direct-command PTY wrapper is still alive.
pub(crate) fn capture_control_process_anchor(pid: u32) -> io::Result<ControlProcessAnchor> {
    let uid = server_uid()?;
    let process = observe_process(pid, uid)?;
    if process.tty == 0 || process.tty == u32::MAX {
        return Err(denied(
            "managed launch has no positive original terminal identity",
        ));
    }
    if observe_process(pid, uid)? != process {
        return Err(denied(
            "managed launch process changed during anchor capture",
        ));
    }
    Ok(ControlProcessAnchor {
        pid,
        birth: process.birth,
        tty: process.tty,
        session: process.session,
    })
}

fn matches_anchor(process: ProcessObservation, anchor: &ControlProcessAnchor) -> bool {
    process.pid == anchor.pid
        && process.birth == anchor.birth
        && process.tty == anchor.tty
        && process.session == anchor.session
        && anchor.tty != 0
        && anchor.tty != u32::MAX
        && anchor.session > 0
}

/// Validate a captured accepted peer against immutable launch-time ownership.
/// Backend runtime condition validation remains a separate required check.
pub(crate) fn validate_control_peer(
    peer: &ControlPeerIdentity,
    anchor: &ControlProcessAnchor,
) -> io::Result<()> {
    let uid = server_uid()?;
    if peer.uid != uid {
        return Err(denied("control peer UID is foreign"));
    }
    let wrapper = observe_process(anchor.pid, uid)?;
    if !matches_anchor(wrapper, anchor) {
        return Err(denied(
            "original managed wrapper or terminal identity changed",
        ));
    }
    let mut pid = peer.pid;
    let mut chain = Vec::new();
    // Bound proof cost and fail closed rather than infer membership beyond the limit.
    for _ in 0..256 {
        let process = observe_process(pid, uid)?;
        if process.tty != anchor.tty
            || process.session != anchor.session
            || (pid == peer.pid && process.birth != peer.birth)
        {
            return Err(denied("control peer left its original process or terminal"));
        }
        if chain
            .iter()
            .any(|node: &ProcessObservation| node.pid == pid)
        {
            return Err(denied("control ancestry changed or is cyclic"));
        }
        chain.push(process);
        if pid == anchor.pid {
            if !matches_anchor(process, anchor) {
                return Err(denied("control ancestry reached a successor wrapper"));
            }
            for original in &chain {
                if observe_process(original.pid, uid)? != *original {
                    return Err(denied("control ancestry changed during proof"));
                }
            }
            if !matches_anchor(observe_process(anchor.pid, uid)?, anchor) {
                return Err(denied("original managed wrapper changed during proof"));
            }
            return Ok(());
        }
        pid = process.parent;
        if pid == 0 {
            break;
        }
    }
    Err(denied(
        "control peer is not a live original-wrapper descendant",
    ))
}
