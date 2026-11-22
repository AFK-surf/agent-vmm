use super::{IMPORT_TIMEOUT, ImageStaging, VerifiedArchive, next_frame, parse_header};
use crate::{
    Containers, RuntimeContext,
    context::core_status,
    error::{import_core_status, invalid_field},
    mutation::{
        ReentrantFailure, is_non_terminal_status_bytes, is_resumable_receipt,
        is_terminal_core_error, mutate_reentrant,
    },
};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;
use tokio_stream::Stream;
use vmm_core::RequestKey;
use vmm_rpc::Status;

#[derive(Clone)]
pub struct ImageImports {
    pub directory: PathBuf,
    pub staging: Arc<dyn ImageStaging>,
    pub min_free_bytes: u64,
    build_gate: Arc<Semaphore>,
}

impl ImageImports {
    pub fn new(directory: PathBuf, staging: Arc<dyn ImageStaging>) -> Self {
        Self {
            directory,
            staging,
            min_free_bytes: 0,
            build_gate: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn with_min_free_bytes(mut self, bytes: u64) -> Self {
        self.min_free_bytes = bytes;
        self
    }

    /// Make the image receipts that a previous release stored as `failed` for a
    /// non-deterministic error retryable. Returns the number of changed receipts.
    pub fn migrate_legacy_failed_receipts(store: &vmm_core::Store) -> vmm_core::Result<usize> {
        store.normalize_failed_receipts(
            &["runtime-import-image", "runtime-pull-image"],
            is_non_terminal_status_bytes,
        )
    }

    pub async fn receive<S>(
        &self,
        ctx: RuntimeContext,
        containers: Containers,
        mut input: S,
    ) -> Result<h::ImportImageResponse, Status>
    where
        S: Stream<Item = Result<h::ImportImageRequest, Status>> + Send + Unpin,
    {
        let (header, digest) = parse_header(&mut input).await?;
        let mut canonical = header.clone();
        // The expiring download URL is transport metadata. The digest and length
        // bind the same bytes on retries that carry a refreshed URL.
        canonical.archive_url.clear();
        let request_id = header.request_id.clone();
        let fingerprint_input = canonical.encode_to_vec();
        let stage = self.staging.clone();
        let directory = self.directory.clone();
        let min_free_bytes = self.min_free_bytes;
        let build_gate = self.build_gate.clone();

        if !header.archive_url.is_empty() {
            if next_frame(&mut input).await?.is_some() {
                return Err(invalid_field(
                    "archive",
                    "runtime OCI bytes must be fetched from archive_url",
                ));
            }
            let archive_sha256: [u8; 32] = header
                .archive_sha256
                .as_slice()
                .try_into()
                .map_err(|_| invalid_field("header", "invalid archive sha256"))?;
            let archive_size = header.archive_size;
            return mutate_reentrant(
                ctx.core.store.clone(),
                ctx.scope.id.clone(),
                "runtime-import-image",
                "image_import",
                request_id,
                fingerprint_input,
                async move {
                    if let Some(grant) = Self::try_short_circuit_url(
                        &ctx,
                        &containers,
                        &header.image_reference,
                        digest,
                        archive_sha256,
                        archive_size,
                        &directory,
                    )
                    .await?
                    {
                        return Ok(h::ImportImageResponse { image: Some(grant) });
                    }
                    let archive =
                        VerifiedArchive::receive_url(&directory, header, digest, min_free_bytes)
                            .await?;
                    // Acquire the build/stage concurrency permit BEFORE starting the
                    // staging/install timer so queued imports do not exhaust their budget.
                    let _permit = build_gate.acquire_owned().await.map_err(|_| {
                        ReentrantFailure::retryable(Status::unavailable(
                            "image import queue closed",
                        ))
                    })?;
                    let grant = tokio::time::timeout(
                        IMPORT_TIMEOUT,
                        Self::publish_reentrant(
                            ctx,
                            containers,
                            archive,
                            stage,
                            Some((archive_sha256, archive_size)),
                        ),
                    )
                    .await
                    .map_err(|_| {
                        ReentrantFailure::retryable(Status::deadline_exceeded(
                            "image import timed out",
                        ))
                    })??;
                    Ok(h::ImportImageResponse { image: Some(grant) })
                },
            )
            .await;
        }

        // Inline chunked upload: pre-check receipt before reading archive chunks.
        let key = RequestKey {
            scope_id: &ctx.scope.id,
            domain: "runtime-import-image",
            request_id: &request_id,
        };
        if let Some(existing) = ctx.core.store.receipt(key).map_err(core_status)?
            && !is_resumable_receipt(&existing)
        {
            return mutate_reentrant(
                ctx.core.store.clone(),
                ctx.scope.id.clone(),
                "runtime-import-image",
                "image_import",
                request_id,
                fingerprint_input,
                async move {
                    Err(ReentrantFailure::retryable(Status::internal(
                        "unexpected inline replay execution",
                    )))
                },
            )
            .await;
        }

        let archive = tokio::time::timeout(
            IMPORT_TIMEOUT,
            VerifiedArchive::receive_chunks(
                &self.directory,
                header,
                digest,
                &mut input,
                min_free_bytes,
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("image import timed out"))??;

        mutate_reentrant(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-import-image",
            "image_import",
            request_id,
            fingerprint_input,
            async move {
                let _permit = build_gate.acquire_owned().await.map_err(|_| {
                    ReentrantFailure::retryable(Status::unavailable("image import queue closed"))
                })?;
                let grant = tokio::time::timeout(
                    IMPORT_TIMEOUT,
                    Self::publish_reentrant(ctx, containers, archive, stage, None),
                )
                .await
                .map_err(|_| {
                    ReentrantFailure::retryable(Status::deadline_exceeded(
                        "image import timed out",
                    ))
                })??;
                Ok(h::ImportImageResponse { image: Some(grant) })
            },
        )
        .await
    }

    async fn try_short_circuit_url(
        ctx: &RuntimeContext,
        containers: &Containers,
        reference: &str,
        digest: [u8; 32],
        archive_sha256: [u8; 32],
        archive_size: u64,
        directory: &std::path::Path,
    ) -> Result<Option<h::Image>, ReentrantFailure> {
        let _guard = containers.definitions.lock().await;
        let scope = &ctx.scope.id;
        let new_grant = match ctx.core.store.resolve_image(scope, reference) {
            Ok(_) => false,
            Err(vmm_core::Error::Missing(_)) => true,
            Err(error) => {
                let terminal = is_terminal_core_error(&error);
                return Err(ReentrantFailure {
                    status: import_core_status(directory, archive_size, error),
                    terminal,
                });
            }
        };
        let granted = ctx
            .core
            .try_grant_verified_url_import(scope, reference, digest, archive_sha256, archive_size)
            .await
            .map_err(|error| {
                let terminal = is_terminal_core_error(&error);
                ReentrantFailure {
                    status: import_core_status(directory, archive_size, error),
                    terminal,
                }
            })?;
        let Some(image) = granted else {
            return Ok(None);
        };
        let grant = containers
            .image_grant(
                &ctx.core.store,
                scope,
                reference,
                &image,
                Some(archive_size),
                new_grant,
            )
            .map_err(ReentrantFailure::retryable)?;
        Ok(Some(grant))
    }

    async fn publish_reentrant(
        ctx: RuntimeContext,
        containers: Containers,
        archive: VerifiedArchive,
        stage: Arc<dyn ImageStaging>,
        verified_url: Option<([u8; 32], u64)>,
    ) -> Result<h::Image, ReentrantFailure> {
        let _guard = containers.definitions.lock().await;
        let reference = archive.header().image_reference.clone();
        let archive_size = archive.header().archive_size;
        let scope = &ctx.scope.id;
        let new_grant = match ctx.core.store.resolve_image(scope, &reference) {
            Ok(_) => false,
            Err(vmm_core::Error::Missing(_)) => true,
            Err(error) => {
                let terminal = is_terminal_core_error(&error);
                return Err(ReentrantFailure {
                    status: core_status(error),
                    terminal,
                });
            }
        };
        let staging_dir = archive
            .path()
            .parent()
            .and_then(|p| {
                if p.file_name().is_some_and(|n| n == "parts") {
                    p.parent()
                } else {
                    Some(p)
                }
            })
            .ok_or_else(|| ReentrantFailure::retryable(Status::internal("image staging directory")))?
            .to_owned();
        let retain_dir = staging_dir.clone();
        let archive = tokio::task::spawn_blocking({
            let store = ctx.core.store.clone();
            move || {
                super::base::collect(&retain_dir, &store)?;
                if archive.is_oci() {
                    super::base::retain(&retain_dir, archive.path(), archive.digest)?;
                }
                Ok::<_, Status>(archive)
            }
        })
        .await
        .map_err(|_| {
            ReentrantFailure::retryable(Status::unavailable(
                "image archive storage task interrupted",
            ))
        })?
        .map_err(ReentrantFailure::retryable)?;
        let logical_bytes = archive.logical_bytes;
        let part_to_clean = if verified_url.is_some() {
            Some(archive.path().to_path_buf())
        } else {
            None
        };
        let image = archive
            .install_with_options(&ctx.core, scope, stage, None, verified_url)
            .await
            .map_err(|error| {
                // Keep `image-staging/oci/<digest>` and `.part` on retryable failures
                // so retries resume without re-downloading.
                let terminal = is_terminal_core_error(&error);
                ReentrantFailure {
                    status: import_core_status(&staging_dir, archive_size, error),
                    terminal,
                }
            })?;
        if let Some(part) = part_to_clean {
            let _ = std::fs::remove_file(part);
        }
        containers
            .image_grant(
                &ctx.core.store,
                scope,
                &reference,
                &image,
                Some(logical_bytes),
                new_grant,
            )
            .map_err(ReentrantFailure::retryable)
    }

    pub(super) async fn publish(
        ctx: RuntimeContext,
        containers: Containers,
        archive: VerifiedArchive,
        stage: Arc<dyn ImageStaging>,
    ) -> Result<h::Image, Status> {
        Self::publish_output(ctx, containers, archive, stage, None).await
    }

    pub(super) async fn publish_output(
        ctx: RuntimeContext,
        containers: Containers,
        archive: VerifiedArchive,
        stage: Arc<dyn ImageStaging>,
        output: Option<super::staging::BuildPublication<'_>>,
    ) -> Result<h::Image, Status> {
        let _guard = containers.definitions.lock().await;
        let reference = archive.header().image_reference.clone();
        let archive_size = archive.header().archive_size;
        let scope = &ctx.scope.id;
        let new_grant = match ctx.core.store.resolve_image(scope, &reference) {
            Ok(_) => false,
            Err(vmm_core::Error::Missing(_)) => true,
            Err(error) => return Err(core_status(error)),
        };
        let (archive, cache) = tokio::task::spawn_blocking({
            let store = ctx.core.store.clone();
            move || {
                let dir = archive
                    .path()
                    .parent()
                    .ok_or_else(|| Status::internal("image staging directory"))?
                    .to_owned();
                super::base::collect(&dir, &store)?;
                if archive.is_oci() {
                    super::base::retain(&dir, archive.path(), archive.digest)?;
                }
                Ok::<_, Status>((archive, dir))
            }
        })
        .await
        .map_err(|_| Status::unavailable("image archive storage task interrupted"))??;
        let logical_bytes = archive.logical_bytes;
        let installed = archive
            .install_with_build(&ctx.core, scope, stage, output)
            .await
            .map_err(|err| import_core_status(&cache, archive_size, err));
        let image = match installed {
            Ok(image) => image,
            Err(status) => {
                super::base::collect(&cache, &ctx.core.store)?;
                return Err(status);
            }
        };
        containers.image_grant(
            &ctx.core.store,
            scope,
            &reference,
            &image,
            Some(logical_bytes),
            new_grant,
        )
    }
}
