mod build;
pub use build::RemoteBuild;
mod image;
pub use image::RemoteImageStage;
mod wire;
mod workspace;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, UnixStream},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, pki_types::ServerName},
};
use vmm_core::{backend::*, *};
use wire::{Fault, Request, Start};
pub use workspace::RemoteWorkspace;
use zeroize::Zeroizing;

/// Credentials for one framed Guest RPC handshake, returned only to the host.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct GuestEndpoint {
    pub address: std::net::SocketAddrV4,
    pub token: vmm_guest_proto::Secret,
}

const MAX_FRAME: usize = 32 << 20;

async fn read<S: AsyncRead + Unpin, T: DeserializeOwned>(s: &mut S) -> Result<T> {
    let n = s.read_u32().await? as usize;
    if n == 0 || n > MAX_FRAME {
        return Err(Error::Invalid("dom0 frame size"));
    }
    let mut bytes = Zeroizing::new(vec![0; n]);
    s.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn write<S: AsyncWrite + Unpin, T: Serialize>(s: &mut S, t: &T) -> Result<()> {
    let bytes = Zeroizing::new(serde_json::to_vec(t)?);
    if bytes.len() > MAX_FRAME {
        return Err(Error::Invalid("dom0 frame size"));
    }
    s.write_u32(bytes.len() as u32).await?;
    s.write_all(&bytes).await?;
    s.flush().await?;
    Ok(())
}

/// One request on an authenticated stream (Unix socket, mutually authenticated
/// TLS, or a carrier transport that enforces equivalent peer identity).
pub async fn serve_connection<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    backend: &dyn Dom0Backend,
) -> Result<()> {
    serve(stream, backend, None).await
}

/// Serve a connection accepted from VMADDR_CID_HOST on the carrier control listener.
pub async fn serve_host_connection<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    backend: &Arc<crate::backend::LocalDom0Backend>,
) -> Result<()> {
    serve(stream, backend.as_ref(), Some(backend)).await
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    backend: &dyn Dom0Backend,
    host: Option<&Arc<crate::backend::LocalDom0Backend>>,
) -> Result<()> {
    let (version, r): (u32, Request) =
        tokio::time::timeout(Duration::from_secs(30), read(&mut stream))
            .await
            .map_err(|_| Error::Interrupted)??;
    if version != 1 {
        return Err(Error::Invalid("dom0 protocol version"));
    }
    if let Request::AcquireBuild(scope, network, quota, id) = r {
        return build::serve(stream, host, *scope, network, quota, &id).await;
    }
    if let Request::SettleBuild(id) = r {
        let result = match host {
            Some(local) => local.settle_build(&id).await.map_err(Fault::from),
            None => Err(Fault::Fenced),
        };
        return write(&mut stream, &result).await;
    }
    if let Request::StageImage(digest, size) = r {
        return image::serve(stream, host, digest, size).await;
    }
    if let Request::StageErofs {
        manifest,
        physical_bytes,
        sha256,
        defaults,
    } = r
    {
        return image::serve_erofs(stream, host, manifest, physical_bytes, sha256, defaults).await;
    }
    if let Request::AcquireWorkspace(scope, volume, access) = r {
        return workspace::serve(stream, host, scope, volume, access).await;
    }
    if let Request::OpenGuestRuntime(boot) = r {
        let response = host
            .ok_or(Fault::Fenced)
            .and_then(|local| local.open_guest_runtime(&boot).map_err(Fault::from));
        return write(&mut stream, &response).await;
    }
    if let Request::GuestAddress(boot) = r {
        let response = host
            .ok_or(Fault::Fenced)
            .and_then(|local| local.guest_address(&boot).map_err(Fault::from));
        return write(&mut stream, &response).await;
    }
    let response = wire::dispatch(backend, r).await.map_err(Fault::from);
    write(&mut stream, &response).await
}

/// Opens a fresh authenticated stream for a single dom0 request.
#[async_trait]
pub trait Connect: Send + Sync {
    async fn connect(&self) -> std::io::Result<UnixStream>;
}

#[derive(Clone)]
enum Endpoint {
    Unix(PathBuf),
    Transport(Arc<dyn Connect>),
    Tls(SocketAddr, ServerName<'static>, TlsConnector),
}
#[derive(Clone)]
pub struct RemoteDom0Client(Endpoint);
impl RemoteDom0Client {
    pub async fn settle_build(&self, id: &str) -> Result<()> {
        self.rpc(Request::SettleBuild(id.into())).await
    }
    pub async fn open_guest_runtime(&self, boot: &BootIdentity) -> Result<GuestEndpoint> {
        self.rpc(Request::OpenGuestRuntime(boot.clone())).await
    }
    pub async fn guest_address(&self, boot: &BootIdentity) -> Result<std::net::SocketAddrV4> {
        self.rpc(Request::GuestAddress(boot.clone())).await
    }
    pub fn transport(connector: Arc<dyn Connect>) -> Self {
        Self(Endpoint::Transport(connector))
    }
    pub fn unix(path: impl AsRef<Path>) -> Self {
        Self(Endpoint::Unix(path.as_ref().into()))
    }
    pub fn tls(
        address: SocketAddr,
        server: ServerName<'static>,
        config: Arc<ClientConfig>,
    ) -> Self {
        Self(Endpoint::Tls(address, server, TlsConnector::from(config)))
    }
    async fn rpc<T: DeserializeOwned>(&self, r: Request) -> Result<T> {
        match &self.0 {
            Endpoint::Transport(c) => exchange(c.connect().await?, r).await,
            Endpoint::Unix(path) => exchange(UnixStream::connect(path).await?, r).await,
            Endpoint::Tls(address, name, tls) => {
                exchange(
                    tls.connect(name.clone(), TcpStream::connect(address).await?)
                        .await?,
                    r,
                )
                .await
            }
        }
    }
}
async fn exchange<S: AsyncRead + AsyncWrite + Unpin, T: DeserializeOwned>(
    mut stream: S,
    r: Request,
) -> Result<T> {
    write(&mut stream, &(1_u32, r)).await?;
    read::<_, std::result::Result<T, Fault>>(&mut stream)
        .await?
        .map_err(Into::into)
}

#[async_trait]
impl Dom0Backend for RemoteDom0Client {
    async fn image_installed(&self, digest: Digest) -> Result<bool> {
        self.rpc(Request::ImageInstalled(digest)).await
    }
    async fn install_image_artifact(&self, s: &ImageSource) -> Result<ImageArtifact> {
        self.rpc(Request::Install(
            s.manifest_digest,
            s.staged_extent.clone(),
            s.physical_bytes,
        ))
        .await
    }
    async fn discard_image_artifact(&self, d: Digest) -> Result<u64> {
        self.rpc(Request::DiscardImage(d)).await
    }
    async fn allocate_volume(&self, v: &Volume) -> Result<()> {
        self.rpc(Request::Volume(v.clone())).await
    }
    async fn discard_volume(&self, s: &str, v: &str) -> Result<()> {
        self.rpc(Request::DiscardVolume(s.into(), v.into())).await
    }
    async fn allocate_overlay(&self, k: &SandboxKey, o: &RootOverlay) -> Result<()> {
        self.rpc(Request::Overlay(k.clone(), o.clone())).await
    }
    async fn discard_overlay(&self, k: &SandboxKey, o: &RootOverlay) -> Result<()> {
        self.rpc(Request::DiscardOverlay(k.clone(), o.clone()))
            .await
    }
    async fn start_vm(&self, l: Launch<'_>) -> Result<VMIncarnation> {
        self.rpc(Request::Start(Box::new(Start {
            definition: l.definition.clone(),
            scope: l.scope.clone(),
            image: l.image.clone(),
            volumes: l.volumes.to_vec(),
            boot: l.boot.clone(),
            token: Zeroizing::new(l.boot_token.to_vec()),
        })))
        .await
    }
    async fn list_live_vms(&self) -> Result<Vec<LiveVm>> {
        let list: Vec<(VMIncarnation, bool)> = self.rpc(Request::List).await?;
        Ok(list
            .into_iter()
            .map(|(incarnation, quiesced)| LiveVm {
                incarnation,
                quiesced,
            })
            .collect())
    }
    async fn quiesce_vm(&self, b: &BootIdentity) -> Result<()> {
        self.rpc(Request::Quiesce(b.clone())).await
    }
    async fn shutdown_vm(&self, b: &BootIdentity) -> Result<()> {
        self.rpc(Request::Shutdown(b.clone())).await
    }
    async fn destroy_vm(&self, b: &BootIdentity) -> Result<()> {
        self.rpc(Request::Destroy(b.clone())).await
    }
    async fn set_network_policy(&self, b: &BootIdentity, s: &Scope) -> Result<()> {
        self.rpc(Request::Policy(b.clone(), s.clone())).await
    }
    async fn bind_route(&self, r: &ServiceRoute) -> Result<()> {
        self.rpc(Request::Bind(r.clone())).await
    }
    async fn revoke_route(&self, id: &str, rev: i64) -> Result<()> {
        self.rpc(Request::Revoke(id.into(), rev)).await
    }
    async fn exec(&self, b: &BootIdentity, a: &[String]) -> Result<String> {
        self.rpc(Request::Exec(b.clone(), a.to_vec())).await
    }
    async fn process(&self, b: &BootIdentity, id: &str) -> Result<ProcessState> {
        let p: Option<i32> = self.rpc(Request::Process(b.clone(), id.into())).await?;
        Ok(p.map_or(ProcessState::Running, ProcessState::Exited))
    }
    async fn logs(&self, b: &BootIdentity, id: &str) -> Result<Vec<u8>> {
        self.rpc(Request::Logs(b.clone(), id.into())).await
    }
    async fn workspace(&self, b: &BootIdentity, q: WorkspaceRequest) -> Result<Vec<u8>> {
        let (path, bytes) = match q {
            WorkspaceRequest::Read(p) => (p, None),
            WorkspaceRequest::Write { path, bytes } => (path, Some(bytes)),
        };
        self.rpc(Request::Workspace(b.clone(), path, bytes)).await
    }
}
