use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream, UnixStream},
    sync::{Mutex, watch},
    task::JoinHandle,
};
use vmm_core::{Error, Result};

pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}

pub type DialFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = std::io::Result<Box<dyn AsyncStream>>> + Send>,
>;

pub trait UpstreamDialer: Send + Sync {
    fn connect(&self) -> DialFuture;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalServiceRoute {
    pub id: String,
    pub source: vmm_core::BootIdentity,
    pub vip: Ipv4Addr,
    pub port: u16,
    pub virtual_service_name: String,
    pub revision: i64,
}

impl From<&vmm_core::ServiceRoute> for ExternalServiceRoute {
    fn from(r: &vmm_core::ServiceRoute) -> Self {
        let b = &r.binding;
        Self {
            id: b.id.clone(),
            source: b.source.clone(),
            vip: b.vip,
            port: b.export.port,
            virtual_service_name: b.virtual_service_name.clone(),
            revision: r.revision,
        }
    }
}

#[derive(Clone)]
pub enum Target {
    Tcp(SocketAddrV4),
    Unix(PathBuf),
    Dialer(Arc<dyn UpstreamDialer>),
}

impl PartialEq for Target {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tcp(a), Self::Tcp(b)) => a == b,
            (Self::Unix(a), Self::Unix(b)) => a == b,
            (Self::Dialer(a), Self::Dialer(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Eq for Target {}
impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => f.debug_tuple("Tcp").field(addr).finish(),
            Self::Unix(path) => f.debug_tuple("Unix").field(path).finish(),
            Self::Dialer(_) => f.write_str("Dialer"),
        }
    }
}

struct Grant {
    target: Target,
    revision: i64,
    cancel: watch::Sender<()>,
    expires: Option<std::time::SystemTime>,
}

struct Listener {
    grants: Arc<Mutex<BTreeMap<Ipv4Addr, Grant>>>,
    task: JoinHandle<()>,
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Default)]
pub struct Proxies {
    listeners: Mutex<BTreeMap<SocketAddrV4, Listener>>,
}

impl Proxies {
    pub async fn bind(
        &self,
        vip: SocketAddrV4,
        source: Ipv4Addr,
        target: Target,
        revision: i64,
    ) -> Result<()> {
        self.bind_until(vip, source, target, revision, None).await
    }
    pub(crate) async fn bind_until(
        &self,
        vip: SocketAddrV4,
        source: Ipv4Addr,
        target: Target,
        revision: i64,
        expires: Option<std::time::SystemTime>,
    ) -> Result<()> {
        let mut listeners = self.listeners.lock().await;
        if let std::collections::btree_map::Entry::Vacant(entry) = listeners.entry(vip) {
            let socket = TcpListener::bind(vip).await?;
            let grants = Arc::new(Mutex::new(BTreeMap::<Ipv4Addr, Grant>::new()));
            let map = Arc::clone(&grants);
            let task = tokio::spawn(async move {
                while let Ok((stream, peer)) = socket.accept().await {
                    let IpAddr::V4(ip) = peer.ip() else {
                        continue;
                    };
                    if let Some(grant) = map.lock().await.get(&ip) {
                        let mut cancel = grant.cancel.subscribe();
                        let target = grant.target.clone();
                        let expires = grant.expires;
                        tokio::spawn(async move {
                            let expiry = async {
                                match expires {
                                    Some(time) => {
                                        if let Ok(remaining) =
                                            time.duration_since(std::time::SystemTime::now())
                                        {
                                            tokio::time::sleep(remaining).await;
                                        }
                                    }
                                    None => std::future::pending().await,
                                }
                            };
                            tokio::select! { biased; _ = cancel.changed() => {}, _ = expiry => {}, _ = forward(stream, target) => {} }
                        });
                    }
                }
            });
            entry.insert(Listener { grants, task });
        }
        let mut grants = listeners[&vip].grants.lock().await;
        if let Some(old) = grants.get(&source) {
            return if old.revision == revision && old.target == target && old.expires == expires {
                Ok(())
            } else {
                Err(Error::Conflict("proxy grant occupied"))
            };
        }
        grants.insert(
            source,
            Grant {
                target,
                revision,
                cancel: watch::channel(()).0,
                expires,
            },
        );
        Ok(())
    }

    pub async fn revoke(&self, vip: SocketAddrV4, source: Ipv4Addr, revision: i64) -> Result<()> {
        let mut listeners = self.listeners.lock().await;
        let Some(listener) = listeners.get(&vip) else {
            return Ok(());
        };
        let mut grants = listener.grants.lock().await;
        if grants.get(&source).is_some_and(|g| g.revision != revision) {
            return Err(Error::Fenced);
        }
        if let Some(grant) = grants.remove(&source) {
            grant.cancel.send_replace(());
            grant.cancel.closed().await;
        }
        let empty = grants.is_empty();
        drop(grants);
        if empty && let Some(mut listener) = listeners.remove(&vip) {
            listener.task.abort();
            // Close the listening socket before a replacement reuses the port.
            let _ = (&mut listener.task).await;
        }
        Ok(())
    }
}

async fn forward(mut client: TcpStream, target: Target) -> std::io::Result<()> {
    match target {
        Target::Tcp(addr) => {
            copy_bidirectional(&mut client, &mut TcpStream::connect(addr).await?).await?;
        }
        Target::Dialer(dialer) => {
            copy_bidirectional(&mut client, &mut dialer.connect().await?).await?;
        }
        Target::Unix(path) => {
            copy_bidirectional(&mut client, &mut UnixStream::connect(path).await?).await?;
        }
    }
    Ok(())
}
