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

use crate::{
    error::{available_bytes, check_import_headroom, import_headroom, invalid_field, is_enospc},
    mutation::ReentrantFailure,
};
use agent_vmm_proto::host::v1 as h;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};
use vmm_rpc::Status;

pub const MAX_ARCHIVE_BYTES: u64 = 8 << 30;
const MAX_CHUNK: usize = 3 << 20;
pub(super) const IMPORT_TIMEOUT: Duration = Duration::from_secs(720);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArchiveKind {
    Oci,
    ErofsBundle,
}

enum ArchiveBacking {
    Temp(NamedTempFile),
    Part(PathBuf),
}

/// Owns the staging file until the backend finishes consuming it.
pub struct VerifiedArchive {
    backing: ArchiveBacking,
    kind: ArchiveKind,
    logical_bytes: u64,
    header: h::ImportImageHeader,
    digest: [u8; 32],
}

fn validate_archive(path: &Path, digest: [u8; 32]) -> Result<(ArchiveKind, u64), Status> {
    match vmm_core::erofs_bundle::parse_and_validate_file(path, Some(digest)) {
        Ok(Some(parsed)) => Ok((ArchiveKind::ErofsBundle, parsed.header.logical_bytes)),
        Ok(None) => Ok((ArchiveKind::Oci, oci::validate(path, digest)?)),
        Err(vmm_core::Error::Io(_)) => Err(Status::unavailable("staged archive unavailable")),
        Err(_) => Err(Status::invalid_argument(
            "invalid EROFS bundle or content identity",
        )),
    }
}

impl VerifiedArchive {
    pub(super) fn from_temp(
        file: NamedTempFile,
        logical_bytes: u64,
        header: h::ImportImageHeader,
        digest: [u8; 32],
    ) -> Self {
        Self {
            backing: ArchiveBacking::Temp(file),
            kind: ArchiveKind::Oci,
            logical_bytes,
            header,
            digest,
        }
    }
    pub(super) fn is_oci(&self) -> bool {
        self.kind == ArchiveKind::Oci
    }
    pub(crate) fn into_file(self) -> NamedTempFile {
        match self.backing {
            ArchiveBacking::Temp(file) => file,
            ArchiveBacking::Part(_) => unreachable!("into_file is only called for inline uploads"),
        }
    }
    pub fn path(&self) -> &Path {
        match &self.backing {
            ArchiveBacking::Temp(file) => file.path(),
            ArchiveBacking::Part(path) => path.as_path(),
        }
    }
    pub fn header(&self) -> &h::ImportImageHeader {
        &self.header
    }
    pub fn manifest_digest(&self) -> [u8; 32] {
        self.digest
    }

    pub async fn receive<S>(staging: &Path, mut src: S) -> Result<Self, Status>
    where
        S: Stream<Item = Result<h::ImportImageRequest, Status>> + Send + Unpin,
    {
        tokio::time::timeout(IMPORT_TIMEOUT, async {
            let (header, digest) = parse_header(&mut src).await?;
            if header.archive_url.is_empty() {
                Self::receive_chunks(staging, header, digest, &mut src, 0).await
            } else {
                if next_frame(&mut src).await?.is_some() {
                    return Err(invalid_field(
                        "archive",
                        "runtime OCI bytes must be fetched from archive_url",
                    ));
                }
                Self::receive_url(staging, header, digest, 0)
                    .await
                    .map_err(|e| e.status)
            }
        })
        .await
        .map_err(|_| Status::deadline_exceeded("image import timed out"))?
    }

    pub(super) async fn receive_chunks<S>(
        staging: &Path,
        header: h::ImportImageHeader,
        digest: [u8; 32],
        src: &mut S,
        min_free_bytes: u64,
    ) -> Result<Self, Status>
    where
        S: Stream<Item = Result<h::ImportImageRequest, Status>> + Send + Unpin,
    {
        use h::import_image_request::Payload;
        check_import_headroom(staging, header.archive_size, min_free_bytes)?;
        let file = NamedTempFile::new_in(staging).map_err(|e| {
            if is_enospc(&e) {
                import_headroom(available_bytes(staging), header.archive_size)
            } else {
                Status::unavailable("cannot create image staging file")
            }
        })?;
        let mut writer = ArchiveWriter {
            staging: staging.to_path_buf(),
            total: header.archive_size,
            file: tokio::fs::File::from_std(
                file.reopen()
                    .map_err(|_| Status::unavailable("cannot open image staging file"))?,
            ),
            remaining: header.archive_size,
            sha: Sha256::new(),
        };
        while let Some(frame) = next_frame(src).await? {
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
        writer.file.flush().await.map_err(|e| {
            if is_enospc(&e) {
                import_headroom(available_bytes(staging), header.archive_size)
            } else {
                Status::unavailable("image staging write failed")
            }
        })?;
        if writer.remaining != 0 || writer.sha.finalize().as_slice() != header.archive_sha256 {
            return Err(invalid_field(
                "archive",
                "OCI archive size or sha256 does not match the header",
            ));
        }
        tokio::task::spawn_blocking(move || {
            let (kind, logical_bytes) = validate_archive(file.path(), digest)?;
            Ok(Self {
                backing: ArchiveBacking::Temp(file),
                kind,
                logical_bytes,
                header,
                digest,
            })
        })
        .await
        .map_err(|_| Status::internal("archive verifier interrupted"))?
    }

    pub(super) async fn receive_url(
        staging: &Path,
        header: h::ImportImageHeader,
        digest: [u8; 32],
        min_free_bytes: u64,
    ) -> Result<Self, ReentrantFailure> {
        let expected_sha256: [u8; 32] = header
            .archive_sha256
            .as_slice()
            .try_into()
            .map_err(|_| {
                ReentrantFailure::terminal(invalid_field(
                    "header",
                    "complete bounded linux/arm64 OCI import metadata is required",
                ))
            })?;
        let part = download::download_resumable(
            staging,
            &header.archive_url,
            header.archive_size,
            expected_sha256,
            min_free_bytes,
        )
        .await?;
        let part_for_validate = part.clone();
        let validated = tokio::task::spawn_blocking(move || {
            validate_archive(&part_for_validate, digest)
        })
        .await
        .map_err(|_| ReentrantFailure::retryable(Status::unavailable("archive verifier interrupted")))?;
        match validated {
            Ok((kind, logical_bytes)) => Ok(Self {
                backing: ArchiveBacking::Part(part),
                kind,
                logical_bytes,
                header,
                digest,
            }),
            Err(status) => {
                // Invalid OCI content on a hash-verified complete archive is deterministic.
                if status.code() == vmm_rpc::Code::InvalidArgument {
                    let _ = tokio::fs::remove_file(&part).await;
                    Err(ReentrantFailure::terminal(status))
                } else {
                    Err(ReentrantFailure::retryable(status))
                }
            }
        }
    }
}

pub(super) async fn parse_header<S>(src: &mut S) -> Result<(h::ImportImageHeader, [u8; 32]), Status>
where
    S: Stream<Item = Result<h::ImportImageRequest, Status>> + Unpin,
{
    use h::import_image_request::Payload;
    let Some(Payload::Header(header)) = next_frame(src).await?.and_then(|r| r.payload) else {
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
    Ok((header, digest))
}

pub(super) async fn next_frame<S>(src: &mut S) -> Result<Option<h::ImportImageRequest>, Status>
where
    S: Stream<Item = Result<h::ImportImageRequest, Status>> + Unpin,
{
    tokio::time::timeout(Duration::from_secs(30), src.next())
        .await
        .map_err(|_| Status::deadline_exceeded("image import stream idle"))?
        .transpose()
}

struct ArchiveWriter {
    staging: PathBuf,
    total: u64,
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
            if is_enospc(&e) {
                import_headroom(available_bytes(&self.staging), self.total)
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
