//! The carrier sends OCI bytes over one host-only connection and holds it through installation.
use super::{Connect, Fault, Request, read, write};
use crate::backend::LocalDom0Backend;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use vmm_core::{Digest, Error, ImageSource, Result};

const MAX_ARCHIVE: u64 = 8 << 30;

pub struct RemoteImageStage {
    pub source: ImageSource,
    _control: tokio::net::UnixStream,
}
impl RemoteImageStage {
    pub async fn upload(connector: &dyn Connect, path: &Path, manifest: Digest) -> Result<Self> {
        tokio::time::timeout(
            Duration::from_secs(600),
            Self::transfer(connector, path, manifest),
        )
        .await
        .map_err(|_| Error::Interrupted)?
    }
    async fn transfer(connector: &dyn Connect, path: &Path, manifest: Digest) -> Result<Self> {
        let mut archive = tokio::fs::File::open(path).await?;
        let size = archive.metadata().await?.len();
        check_size(size)?;
        let mut stream = connector.connect().await?;
        write(&mut stream, &(1_u32, Request::StageImage(manifest, size))).await?;
        read::<_, std::result::Result<(), Fault>>(&mut stream)
            .await?
            .map_err(Error::from)?;
        if tokio::io::copy(&mut (&mut archive).take(size), &mut stream).await? != size {
            return Err(Error::Invalid("incomplete OCI upload"));
        }
        stream.flush().await?;
        let (staged_extent, physical_bytes) = read::<_, std::result::Result<_, Fault>>(&mut stream)
            .await?
            .map_err(Error::from)?;
        Ok(Self {
            source: ImageSource {
                manifest_digest: manifest,
                staged_extent,
                physical_bytes,
            },
            _control: stream,
        })
    }
}
fn check_size(size: u64) -> Result<()> {
    if !(1..=MAX_ARCHIVE).contains(&size) {
        return Err(Error::Invalid("OCI upload size"));
    }
    Ok(())
}

struct Published {
    backend: Arc<LocalDom0Backend>,
    source: ImageSource,
}
impl Drop for Published {
    fn drop(&mut self) {
        let _ = self
            .backend
            .discard_staged_image(&self.source.staged_extent);
    }
}

pub(super) async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    backend: Option<&Arc<LocalDom0Backend>>,
    digest: Digest,
    size: u64,
) -> Result<()> {
    use std::result::Result as StdResult;
    let admission = async {
        let local = backend.ok_or(Error::Fenced)?;
        check_size(size)?;
        let permit = local
            .remote_image_upload
            .acquire()
            .await
            .map_err(|_| Error::Interrupted)?;
        // A token-shaped temporary filename participates in ordinary boot recovery and accounting.
        let token = hex::encode(crate::keys::random::<16>()?.as_ref());
        let file = tempfile::Builder::new()
            .prefix(&token)
            .rand_bytes(0)
            .tempfile_in(&local.cfg.staging)?;
        Ok::<_, Error>((local.clone(), permit, file))
    }
    .await;
    let (local, permit, file) = match admission {
        Ok(a) => a,
        Err(e) => return write(&mut stream, &StdResult::<(), Fault>::Err(e.into())).await,
    };
    write(&mut stream, &StdResult::<(), Fault>::Ok(())).await?;
    let result = async {
        let mut target = tokio::fs::File::from_std(file.reopen()?);
        let copied = tokio::time::timeout(
            Duration::from_secs(600),
            tokio::io::copy(&mut (&mut stream).take(size), &mut target),
        )
        .await
        .map_err(|_| Error::Interrupted)??;
        if copied != size {
            return Err(Error::Invalid("incomplete OCI upload"));
        }
        target.flush().await?;
        drop(target);
        tokio::task::spawn_blocking(move || {
            let source = local.stage_image(file.path(), digest)?;
            Ok(Published {
                backend: local,
                source,
            })
        })
        .await
        .map_err(|_| Error::Interrupted)?
    }
    .await;
    drop(permit);
    let lease = match result {
        Ok(lease) => lease,
        Err(e) => return write(&mut stream, &StdResult::<(), Fault>::Err(e.into())).await,
    };
    write(
        &mut stream,
        &StdResult::<_, Fault>::Ok((&lease.source.staged_extent, lease.source.physical_bytes)),
    )
    .await?;
    let _ = stream.read_u8().await;
    Ok(())
}
