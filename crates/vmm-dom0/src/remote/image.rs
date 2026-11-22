//! The carrier sends OCI or pre-built EROFS bytes over one host-only connection
//! and holds it through installation.
use super::{Connect, Fault, Request, read, write};
use crate::{backend::LocalDom0Backend, catalog::image_id, extent::ctl_error};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use vmm_core::{
    Digest, Error, ImageSource, Result,
    erofs_bundle::{
        EROFS_BLOCK_BYTES, EROFS_SUPER_MAGIC_LE, EROFS_SUPER_OFFSET, ErofsBundleCompression,
        ParsedErofsBundle,
    },
};

const MAX_ARCHIVE: u64 = 8 << 30;
const MAX_EROFS: u64 = 16 << 30;
const STAGE_TIMEOUT: Duration = Duration::from_secs(720);
const STREAM_CHUNK_BYTES: usize = 1 << 20;

#[derive(Serialize, Deserialize)]
pub(super) enum StageErofsAdmission {
    AlreadyInstalled { physical_bytes: u64 },
    Upload,
}

pub struct RemoteImageStage {
    pub source: ImageSource,
    _control: tokio::net::UnixStream,
}
impl RemoteImageStage {
    pub async fn upload(connector: &dyn Connect, path: &Path, manifest: Digest) -> Result<Self> {
        tokio::time::timeout(
            STAGE_TIMEOUT,
            Self::transfer(connector, path, manifest),
        )
        .await
        .map_err(|_| Error::Interrupted)?
    }

    pub async fn upload_erofs(
        connector: &dyn Connect,
        path: &Path,
        manifest: Digest,
        physical_bytes: u64,
        sha256: Digest,
        defaults: crate::image::Defaults,
    ) -> Result<Self> {
        tokio::time::timeout(
            STAGE_TIMEOUT,
            Self::transfer_erofs(connector, path, manifest, physical_bytes, sha256, defaults),
        )
        .await
        .map_err(|_| Error::Interrupted)?
    }

    pub async fn upload_erofs_bundle(
        connector: &dyn Connect,
        path: &Path,
        parsed: &ParsedErofsBundle,
    ) -> Result<Self> {
        tokio::time::timeout(
            STAGE_TIMEOUT,
            Self::transfer_erofs_bundle(connector, path, parsed),
        )
        .await
        .map_err(|_| Error::Interrupted)?
    }

    async fn transfer_erofs_bundle(
        connector: &dyn Connect,
        path: &Path,
        parsed: &ParsedErofsBundle,
    ) -> Result<Self> {
        let physical_bytes = parsed.header.erofs_bytes;
        let manifest = parsed.manifest_digest;
        let expected_sha256 = parsed.erofs_sha256;
        check_erofs_size(physical_bytes)?;
        let mut stream = connector.connect().await?;
        write(
            &mut stream,
            &(
                1_u32,
                Request::StageErofs {
                    manifest,
                    physical_bytes,
                    sha256: expected_sha256,
                    defaults: parsed.header.defaults.clone(),
                },
            ),
        )
        .await?;
        match read::<_, std::result::Result<StageErofsAdmission, Fault>>(&mut stream)
            .await?
            .map_err(Error::from)?
        {
            StageErofsAdmission::AlreadyInstalled { physical_bytes } => {
                return Ok(Self {
                    source: ImageSource {
                        manifest_digest: manifest,
                        staged_extent: String::new(),
                        physical_bytes,
                    },
                    _control: stream,
                });
            }
            StageErofsAdmission::Upload => {}
        }

        let bundle_path = path.to_path_buf();
        let payload_offset = parsed.payload_offset;
        let payload_len = parsed.payload_len;
        let compression = parsed.header.compression;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>>>(4);
        let reader_task = tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<()> {
                let mut file = std::fs::File::open(&bundle_path)?;
                file.seek(SeekFrom::Start(payload_offset))?;
                let bounded = file.take(payload_len);
                let mut reader: Box<dyn Read> = match compression {
                    ErofsBundleCompression::None => Box::new(bounded),
                    ErofsBundleCompression::Zstd => Box::new(
                        zstd::stream::read::Decoder::new(BufReader::new(bounded))
                            .map_err(|_| Error::Invalid("invalid zstd EROFS payload"))?,
                    ),
                };
                let mut limited = (&mut reader).take(physical_bytes + 1);
                let mut sha = Sha256::new();
                let mut total = 0u64;
                let mut first_chunk = true;
                loop {
                    let mut buf = vec![0u8; STREAM_CHUNK_BYTES];
                    let mut filled = 0usize;
                    while filled < buf.len() {
                        match limited.read(&mut buf[filled..]) {
                            Ok(0) => break,
                            Ok(n) => filled += n,
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(_) if compression == ErofsBundleCompression::Zstd => {
                                return Err(Error::Invalid("invalid zstd EROFS payload"));
                            }
                            Err(e) => return Err(Error::Io(e)),
                        }
                    }
                    if filled == 0 {
                        break;
                    }
                    total = total
                        .checked_add(filled as u64)
                        .ok_or(Error::Invalid("EROFS bundle decompressed length mismatch"))?;
                    if total > physical_bytes {
                        return Err(Error::Invalid("EROFS bundle decompressed length mismatch"));
                    }
                    if first_chunk {
                        first_chunk = false;
                        if filled < EROFS_SUPER_OFFSET + 4
                            || buf[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4]
                                != EROFS_SUPER_MAGIC_LE
                        {
                            return Err(Error::Invalid("invalid EROFS superblock"));
                        }
                    }
                    buf.truncate(filled);
                    sha.update(&buf);
                    if tx.blocking_send(Ok(buf)).is_err() {
                        return Ok(());
                    }
                }
                if total != physical_bytes {
                    return Err(Error::Invalid("EROFS bundle decompressed length mismatch"));
                }
                if Digest::from(sha.finalize()) != expected_sha256 {
                    return Err(Error::Invalid("EROFS bundle sha256 mismatch"));
                }
                Ok(())
            })();
            if let Err(err) = result {
                let _ = tx.blocking_send(Err(err));
            }
        });

        while let Some(chunk) = rx.recv().await {
            let chunk = chunk?;
            stream.write_all(&chunk).await?;
        }
        reader_task.await.map_err(|_| Error::Interrupted)?;
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

    async fn transfer_erofs(
        connector: &dyn Connect,
        path: &Path,
        manifest: Digest,
        physical_bytes: u64,
        sha256: Digest,
        defaults: crate::image::Defaults,
    ) -> Result<Self> {
        check_erofs_size(physical_bytes)?;
        let mut image = tokio::fs::File::open(path).await?;
        let actual = image.metadata().await?.len();
        if actual != physical_bytes {
            return Err(Error::Invalid("staged EROFS length"));
        }
        let mut stream = connector.connect().await?;
        write(
            &mut stream,
            &(
                1_u32,
                Request::StageErofs {
                    manifest,
                    physical_bytes,
                    sha256,
                    defaults,
                },
            ),
        )
        .await?;
        match read::<_, std::result::Result<StageErofsAdmission, Fault>>(&mut stream)
            .await?
            .map_err(Error::from)?
        {
            StageErofsAdmission::AlreadyInstalled { physical_bytes } => {
                return Ok(Self {
                    source: ImageSource {
                        manifest_digest: manifest,
                        staged_extent: String::new(),
                        physical_bytes,
                    },
                    _control: stream,
                });
            }
            StageErofsAdmission::Upload => {}
        }
        if tokio::io::copy(&mut (&mut image).take(physical_bytes), &mut stream).await?
            != physical_bytes
        {
            return Err(Error::Invalid("incomplete EROFS upload"));
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

fn check_erofs_size(size: u64) -> Result<()> {
    if !(EROFS_BLOCK_BYTES..=MAX_EROFS).contains(&size) || !size.is_multiple_of(EROFS_BLOCK_BYTES) {
        return Err(Error::Invalid("EROFS upload size"));
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

pub(super) async fn serve_erofs<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    backend: Option<&Arc<LocalDom0Backend>>,
    manifest: Digest,
    physical_bytes: u64,
    expected_sha256: Digest,
    defaults: crate::image::Defaults,
) -> Result<()> {
    use std::result::Result as StdResult;
    let local = match backend {
        Some(local) => local.clone(),
        None => {
            return write(
                &mut stream,
                &StdResult::<StageErofsAdmission, Fault>::Err(Fault::Fenced),
            )
            .await;
        }
    };
    if let Err(e) = check_erofs_size(physical_bytes) {
        return write(
            &mut stream,
            &StdResult::<StageErofsAdmission, Fault>::Err(e.into()),
        )
        .await;
    }
    if let Ok(true) = local.image_installed(manifest) {
        let installed_bytes: u64 = local
            .pool
            .lookup(&image_id(manifest))
            .map(|exts| exts.iter().map(|e| e.sectors * 512).sum())
            .unwrap_or(physical_bytes);
        return write(
            &mut stream,
            &StdResult::<StageErofsAdmission, Fault>::Ok(
                StageErofsAdmission::AlreadyInstalled {
                    physical_bytes: installed_bytes,
                },
            ),
        )
        .await;
    }
    let permit = match local.remote_image_upload.acquire().await {
        Ok(p) => p,
        Err(_) => {
            return write(
                &mut stream,
                &StdResult::<StageErofsAdmission, Fault>::Err(Fault::Interrupted),
            )
            .await;
        }
    };
    if let Ok(true) = local.image_installed(manifest) {
        drop(permit);
        let installed_bytes: u64 = local
            .pool
            .lookup(&image_id(manifest))
            .map(|exts| exts.iter().map(|e| e.sectors * 512).sum())
            .unwrap_or(physical_bytes);
        return write(
            &mut stream,
            &StdResult::<StageErofsAdmission, Fault>::Ok(
                StageErofsAdmission::AlreadyInstalled {
                    physical_bytes: installed_bytes,
                },
            ),
        )
        .await;
    }
    let allocated = (|| -> Result<(String, Vec<vmm_l4hctl::DiskExtent>)> {
        let token = hex::encode(crate::keys::random::<16>()?.as_ref());
        let stage_obj = format!("stage/{token}");
        let extents = local.pool.allocate(&stage_obj, physical_bytes)?;
        Ok((token, extents))
    })();
    let (token, extents) = match allocated {
        Ok(v) => v,
        Err(e) => {
            drop(permit);
            return write(
                &mut stream,
                &StdResult::<StageErofsAdmission, Fault>::Err(e.into()),
            )
            .await;
        }
    };
    let lease = Published {
        backend: local.clone(),
        source: ImageSource {
            manifest_digest: manifest,
            staged_extent: token.clone(),
            physical_bytes,
        },
    };
    write(
        &mut stream,
        &StdResult::<StageErofsAdmission, Fault>::Ok(StageErofsAdmission::Upload),
    )
    .await?;
    let result = tokio::time::timeout(STAGE_TIMEOUT, async {
        let mut sha = Sha256::new();
        let disk = local.pool.disk();
        let mut remaining_sectors = physical_bytes / 512;
        let mut first_chunk = true;
        for ext in extents {
            if remaining_sectors == 0 {
                break;
            }
            let mut pos = ext.sector;
            let end = ext.sector + ext.sectors;
            while pos < end && remaining_sectors > 0 {
                let sectors = (end - pos).min(2048).min(remaining_sectors); // up to 1 MiB
                let byte_len = (sectors * 512) as usize;
                let mut buf = vec![0u8; byte_len];
                stream.read_exact(&mut buf).await.map_err(|e| {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        Error::Invalid("incomplete EROFS upload")
                    } else {
                        Error::Io(e)
                    }
                })?;
                if first_chunk {
                    first_chunk = false;
                    if buf.len() < EROFS_SUPER_OFFSET + 4
                        || buf[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4] != EROFS_SUPER_MAGIC_LE
                    {
                        return Err(Error::Invalid("invalid EROFS superblock"));
                    }
                }
                sha.update(&buf);
                let ctl = local.ctl.clone();
                let write_pos = pos;
                tokio::task::spawn_blocking(move || {
                    ctl.lock()
                        .map_err(|_| Error::Interrupted)?
                        .blk_write(disk, write_pos, &buf)
                        .map_err(ctl_error)
                })
                .await
                .map_err(|_| Error::Interrupted)??;
                pos += sectors;
                remaining_sectors -= sectors;
            }
        }
        if Digest::from(sha.finalize()) != expected_sha256 {
            return Err(Error::Invalid("staged EROFS sha256 mismatch"));
        }
        let ctl = local.ctl.clone();
        tokio::task::spawn_blocking(move || {
            ctl.lock()
                .map_err(|_| Error::Interrupted)?
                .blk_flush(disk)
                .map_err(ctl_error)
        })
        .await
        .map_err(|_| Error::Interrupted)??;
        local
            .catalog()
            .put(&format!("stage/{token}"), &(manifest, defaults, physical_bytes))?;
        Ok::<_, Error>(())
    })
    .await
    .map_err(|_| Error::Interrupted)
    .and_then(|r| r);
    drop(permit);
    if let Err(e) = result {
        drop(lease);
        return write(&mut stream, &StdResult::<(String, u64), Fault>::Err(e.into())).await;
    }
    write(
        &mut stream,
        &StdResult::<_, Fault>::Ok((&lease.source.staged_extent, lease.source.physical_bytes)),
    )
    .await?;
    let _ = stream.read_u8().await;
    Ok(())
}
