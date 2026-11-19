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

impl VerifiedArchive {
    pub async fn install(
        self,
        core: &Core,
        scope: &str,
        driver: Arc<dyn ImageStaging>,
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
            core.import_image(scope, &reference, &staged.source).await
        } else {
            Err(Error::Fenced)
        };
        let cleanup = staged.discard();
        let artifact = result?;
        cleanup?;
        Ok(artifact)
    }
}
