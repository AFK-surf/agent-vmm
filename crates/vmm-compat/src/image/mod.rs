//! Receive and verify immutable OCI archives before granting an image to a scope.
mod base;
pub mod build;
pub(crate) mod copy;
mod download;
pub(crate) mod grant;
mod import;
pub use import::ImageImports;
mod oci;
mod pull;
mod registry;
pub(crate) mod retained;

use crate::error::invalid_field;
use agent_vmm_proto::host::v1 as h;
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};
use vmm_rpc::Status;

pub const MAX_ARCHIVE_BYTES: u64 = 8 << 30;
const MAX_CHUNK: usize = 3 << 20;

/// Owns the staging file until the backend finishes consuming it.
pub struct VerifiedArchive {
    file: NamedTempFile,
    logical_bytes: u64,
    header: h::ImportImageHeader,
    digest: [u8; 32],
}

impl VerifiedArchive {
    pub fn path(&self) -> &Path {
        self.file.path()
    }
    pub(crate) fn into_file(self) -> NamedTempFile {
        self.file
    }
    pub fn header(&self) -> &h::ImportImageHeader {
        &self.header
    }
    pub fn manifest_digest(&self) -> [u8; 32] {
        self.digest
    }

    pub async fn receive<S>(staging: &Path, src: S) -> Result<Self, Status>
    where
        S: Stream<Item = Result<h::ImportImageRequest, Status>> + Send + Unpin,
    {
        tokio::time::timeout(Duration::from_secs(600), Self::receive_inner(staging, src))
            .await
            .map_err(|_| Status::deadline_exceeded("image import timed out"))?
    }

    async fn receive_inner<S>(staging: &Path, mut src: S) -> Result<Self, Status>
    where
        S: Stream<Item = Result<h::ImportImageRequest, Status>> + Send + Unpin,
    {
        use h::import_image_request::Payload;
        let Some(Payload::Header(header)) = next_frame(&mut src).await?.and_then(|r| r.payload)
        else {
            return Err(invalid_field(
                "header",
                "complete bounded linux/arm64 OCI import metadata is required",
            ));
        };
        let digest = oci::digest(&header.manifest_digest)
            .map_err(|_| invalid_field("header", "invalid manifest digest"))?;
        if header.request_id.is_empty()
            || header.image_reference.is_empty()
            || header.platform != "linux/arm64"
            || header.archive_sha256.len() != 32
            || !(1..=MAX_ARCHIVE_BYTES).contains(&header.archive_size)
        {
            return Err(invalid_field(
                "header",
                "complete bounded linux/arm64 OCI import metadata is required",
            ));
        }
        let file = NamedTempFile::new_in(staging)
            .map_err(|_| Status::unavailable("cannot create image staging file"))?;
        let mut writer = ArchiveWriter {
            file: tokio::fs::File::from_std(
                file.reopen()
                    .map_err(|_| Status::unavailable("cannot open image staging file"))?,
            ),
            remaining: header.archive_size,
            sha: Sha256::new(),
        };
        if header.archive_url.is_empty() {
            while let Some(frame) = next_frame(&mut src).await? {
                let Some(Payload::ArchiveChunk(bytes)) = frame.payload else {
                    return Err(invalid_field(
                        "archive_chunk",
                        "archive chunks must be non-empty, bounded, and within archive_size",
                    ));
                };
                if bytes.is_empty() || bytes.len() > MAX_CHUNK {
                    return Err(invalid_field(
                        "archive_chunk",
                        "archive chunks must be non-empty, bounded, and within archive_size",
                    ));
                }
                writer.append(&bytes).await?;
            }
        } else {
            if next_frame(&mut src).await?.is_some() {
                return Err(invalid_field(
                    "archive",
                    "runtime OCI bytes must be fetched from archive_url",
                ));
            }
            download::download(&header.archive_url, &mut writer).await?;
        }
        writer
            .file
            .flush()
            .await
            .map_err(|_| Status::unavailable("image staging write failed"))?;
        if writer.remaining != 0 || writer.sha.finalize().as_slice() != header.archive_sha256 {
            return Err(invalid_field(
                "archive",
                "OCI archive size or sha256 does not match the header",
            ));
        }
        tokio::task::spawn_blocking(move || {
            let logical_bytes = oci::validate(file.path(), digest)?;
            Ok(Self {
                file,
                logical_bytes,
                header,
                digest,
            })
        })
        .await
        .map_err(|_| Status::internal("archive verifier interrupted"))?
    }
}

async fn next_frame<S>(src: &mut S) -> Result<Option<h::ImportImageRequest>, Status>
where
    S: Stream<Item = Result<h::ImportImageRequest, Status>> + Unpin,
{
    tokio::time::timeout(Duration::from_secs(30), src.next())
        .await
        .map_err(|_| Status::deadline_exceeded("image import stream idle"))?
        .transpose()
}

struct ArchiveWriter {
    file: tokio::fs::File,
    remaining: u64,
    sha: Sha256,
}
impl ArchiveWriter {
    async fn append(&mut self, bytes: &[u8]) -> Result<(), Status> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len() as u64)
            .ok_or_else(|| {
                invalid_field(
                    "archive_chunk",
                    "archive chunks must be non-empty, bounded, and within archive_size",
                )
            })?;
        self.file.write_all(bytes).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::StorageFull {
                Status::resource_exhausted("image staging filesystem full")
            } else {
                Status::unavailable("image staging write failed")
            }
        })?;
        self.sha.update(bytes);
        Ok(())
    }
}

mod staging;
pub use staging::ImageStaging;
