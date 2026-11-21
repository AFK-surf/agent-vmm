//! Ownership of the backend staging token follows the import future.
use super::VerifiedArchive;
use std::{path::Path, sync::Arc};
use vmm_core::{Core, Digest, Error, ImageArtifact, ImageSource, Result};

pub trait ImageStaging: Send + Sync + 'static {
    fn stage_image(&self, archive: &Path, manifest: Digest) -> Result<ImageSource>;
    fn discard_staged_image(&self, token: &str) -> Result<()>;
}

struct Staged {
    driver: Arc<dyn ImageStaging>,
    source: ImageSource,
    discarded: bool,
}

impl Staged {
    fn discard(&mut self) -> Result<()> {
        self.driver
            .discard_staged_image(&self.source.staged_extent)?;
        self.discarded = true;
        Ok(())
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        if !self.discarded {
            let _ = self.discard();
        }
    }
}

pub(super) struct BuildPublication<'a> {
    pub context: &'a crate::RuntimeContext,
    pub output: &'a vmm_core::RetainedImage,
    pub cancelled: tokio::sync::watch::Receiver<bool>,
    pub deadline: i64,
}

impl VerifiedArchive {
    pub async fn install(
        self,
        core: &Core,
        scope: &str,
        driver: Arc<dyn ImageStaging>,
    ) -> Result<ImageArtifact> {
        self.install_with_options(core, scope, driver, None, None)
            .await
    }

    pub(super) async fn install_with_build(
        self,
        core: &Core,
        scope: &str,
        driver: Arc<dyn ImageStaging>,
        build: Option<BuildPublication<'_>>,
    ) -> Result<ImageArtifact> {
        self.install_with_options(core, scope, driver, build, None)
            .await
    }

    pub(super) async fn install_with_options(
        self,
        core: &Core,
        scope: &str,
        driver: Arc<dyn ImageStaging>,
        build: Option<BuildPublication<'_>>,
        verified_url: Option<(Digest, u64)>,
    ) -> Result<ImageArtifact> {
        let reference = self.header().image_reference.clone();
        let digest = self.manifest_digest();
        // The worker owns both files until the blocking stage returns. If the
        // waiter disappears, dropping the worker's return value cleans the token.
        let mut staged = tokio::task::spawn_blocking(move || {
            let source = driver.stage_image(self.path(), digest)?;
            Ok::<_, Error>(Staged {
                driver,
                source,
                discarded: false,
            })
        })
        .await
        .map_err(|_| Error::Interrupted)??;
        let result = if staged.source.manifest_digest == digest {
            match (build, verified_url) {
                (Some(build), _) => {
                    let ctx = build.context;
                    core.publish_build(
                        &ctx.session,
                        ctx.remote.as_ref().map(|r| r.registration_id.as_str()),
                        &staged.source,
                        build.output,
                        || {
                            if *build.cancelled.borrow()
                                || crate::execution::now().map_err(|_| Error::Interrupted)?
                                    >= build.deadline
                            {
                                return Err(Error::Interrupted);
                            }
                            Ok(())
                        },
                    )
                    .await
                }
                (None, Some((archive_sha256, archive_size))) => {
                    core.import_verified_url_image(
                        scope,
                        &reference,
                        &staged.source,
                        archive_sha256,
                        archive_size,
                    )
                    .await
                }
                (None, None) => core.import_image(scope, &reference, &staged.source).await,
            }
        } else {
            Err(Error::Fenced)
        };
        let cleanup = staged.discard();
        let artifact = result?;
        cleanup?;
        Ok(artifact)
    }
}
