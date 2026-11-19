use std::{
    io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
};
use tokio::{io::unix::AsyncFd, net::UnixStream};

pub struct Listener(AsyncFd<OwnedFd>);

fn fd(raw: RawFd) -> io::Result<OwnedFd> {
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: callers provide the newly owned result of socket or accept4.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

fn address() -> libc::sockaddr_vm {
    // SAFETY: sockaddr_vm contains integer and padding fields, all valid when zero.
    unsafe { mem::zeroed() }
}

impl Listener {
    pub fn bind(port: u32) -> io::Result<Self> {
        if port == 0 || port == libc::VMADDR_PORT_ANY {
            return Err(io::Error::other("invalid vsock port"));
        }
        // SAFETY: socket takes only integer arguments. The returned fd is owned by socket.
        let socket = fd(unsafe {
            libc::socket(
                libc::AF_VSOCK,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        })?;
        let mut addr = address();
        addr.svm_family = libc::AF_VSOCK as _;
        addr.svm_cid = libc::VMADDR_CID_ANY;
        addr.svm_port = port;
        // SAFETY: the address and length identify a fully initialized AF_VSOCK address.
        if unsafe { libc::bind(socket.as_raw_fd(), (&raw const addr).cast(), size()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the socket is a live bound stream descriptor.
        if unsafe { libc::listen(socket.as_raw_fd(), 128) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the OwnedFd stays in this registration until drop, and its
        // descriptor is never closed, replaced or independently registered.
        Ok(Self(unsafe { AsyncFd::register(socket) }?))
    }

    pub async fn accept(&self) -> io::Result<UnixStream> {
        loop {
            let mut ready = self.0.readable().await?;
            let accepted = ready.try_io(|listener| {
                let mut peer = address();
                let mut len = size();
                // SAFETY: accept4 writes at most len bytes into peer and returns a fresh nonblocking descriptor.
                let client = fd(unsafe {
                    libc::accept4(
                        listener.as_raw_fd(),
                        (&raw mut peer).cast(),
                        &mut len,
                        libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                    )
                })?;
                Ok((
                    client,
                    len == size()
                        && peer.svm_family as i32 == libc::AF_VSOCK
                        && allowed_peer(peer.svm_cid),
                ))
            });
            if let Ok(result) = accepted {
                let (client, allowed) = result?;
                if allowed {
                    // Tokio uses stream read/write/shutdown only. Peer addresses are decoded above as sockaddr_vm.
                    return UnixStream::from_std(std::os::unix::net::UnixStream::from(client));
                }
            }
        }
    }
}
fn size() -> libc::socklen_t {
    mem::size_of::<libc::sockaddr_vm>() as _
}
fn allowed_peer(cid: u32) -> bool {
    cid == libc::VMADDR_CID_HOST || (cfg!(test) && cid == libc::VMADDR_CID_LOCAL)
}
#[cfg(test)]
mod tests {
    #[test]
    fn carrier_peer_identity() {
        assert!(super::allowed_peer(libc::VMADDR_CID_HOST));
        assert!(super::allowed_peer(libc::VMADDR_CID_LOCAL));
        assert!(!super::allowed_peer(3));
        assert!(!super::allowed_peer(libc::VMADDR_CID_ANY));
    }
}
