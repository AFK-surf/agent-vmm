//! A host control connection owns the dom0 mount lease through the last Guest RPC.
use super::{Connect, Fault, Request, read, write};
use crate::backend::LocalDom0Backend;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use vmm_core::{AccessMode, BootIdentity, Error, Result};

pub struct RemoteWorkspace {
    pub boot: BootIdentity,
    pub alias: String,
    _control: tokio::net::UnixStream,
}

impl RemoteWorkspace {
    pub async fn acquire(
        connector: &dyn Connect,
        scope: &str,
        volume: Option<&str>,
        access: AccessMode,
    ) -> Result<Self> {
        let mut stream = connector.connect().await?;
        let request = Request::AcquireWorkspace(scope.into(), volume.map(String::from), access);
        write(&mut stream, &(1u32, request)).await?;
        let (boot, alias) = read::<_, std::result::Result<_, Fault>>(&mut stream)
            .await?
            .map_err(Error::from)?;
        Ok(Self {
            boot,
            alias,
            _control: stream,
        })
    }
}

pub(super) async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    backend: Option<&Arc<LocalDom0Backend>>,
    scope: String,
    volume: Option<String>,
    access: AccessMode,
) -> Result<()> {
    use std::result::Result as StdResult;
    let result = async {
        let backend = backend.ok_or(Error::Fenced)?;
        let permit = backend
            .remote_mounts
            .try_acquire()
            .map_err(|_| Error::Conflict("workspace lease limit"))?;
        let workspace = backend
            .resolve_workspace(&scope, volume.as_deref(), access)
            .await?;
        Ok::<_, Error>((permit, workspace))
    }
    .await;
    let (_permit, lease) = match result {
        Ok(pair) => pair,
        Err(err) => return write(&mut stream, &StdResult::<(), Fault>::Err(err.into())).await,
    };
    write(&mut stream, &StdResult::<_, Fault>::Ok(lease.binding())).await?;
    // EOF from cancellation, normal completion, or host process exit releases the same lease.
    let _ = stream.read_u8().await;
    lease.close().await
}
