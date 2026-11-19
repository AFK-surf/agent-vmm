//! The host control connection owns one disposable build VM and its disks.
use super::{Connect, Fault, Request, read, write};
use crate::backend::LocalDom0Backend;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use vmm_core::{BootIdentity, EgressMode, Error, Result, Scope};

pub struct RemoteBuild {
    pub boot: BootIdentity,
    stream: tokio::net::UnixStream,
}

impl RemoteBuild {
    pub async fn acquire(
        connector: &dyn Connect,
        scope: &Scope,
        network: EgressMode,
        quota: u64,
    ) -> Result<Self> {
        let id = hex::encode(crate::keys::random::<16>()?.as_ref());
        Self::acquire_owned(connector, scope, network, quota, &id).await
    }

    pub async fn acquire_owned(
        connector: &dyn Connect,
        scope: &Scope,
        network: EgressMode,
        quota: u64,
        id: &str,
    ) -> Result<Self> {
        let mut stream = connector.connect().await?;
        write(
            &mut stream,
            &(
                1u32,
                Request::AcquireBuild(Box::new(scope.clone()), network, quota, id.into()),
            ),
        )
        .await?;
        let boot = read::<_, std::result::Result<_, Fault>>(&mut stream)
            .await?
            .map_err(Error::from)?;
        Ok(Self { boot, stream })
    }

    pub async fn close(mut self) -> Result<()> {
        self.stream.write_u8(1).await?;
        self.stream.flush().await?;
        read::<_, std::result::Result<(), Fault>>(&mut self.stream)
            .await?
            .map_err(Into::into)
    }
}

pub(super) async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    local: Option<&Arc<LocalDom0Backend>>,
    scope: Scope,
    network: EgressMode,
    quota: u64,
    id: &str,
) -> Result<()> {
    let result = async {
        let backend = local.ok_or(Error::Fenced)?;
        let build = backend
            .begin_owned_build(&scope, network, quota, id)
            .await?;
        Ok::<_, Error>(build)
    }
    .await;
    let build = match result {
        Ok(build) => build,
        Err(err) => {
            return write(
                &mut stream,
                &std::result::Result::<(), Fault>::Err(err.into()),
            )
            .await;
        }
    };
    write(
        &mut stream,
        &std::result::Result::<_, Fault>::Ok(&build.boot),
    )
    .await?;
    let close = stream.read_u8().await;
    let result = build.close().await;
    if matches!(close, Ok(1)) {
        write(&mut stream, &result.map_err(Fault::from)).await
    } else {
        result
    }
}
