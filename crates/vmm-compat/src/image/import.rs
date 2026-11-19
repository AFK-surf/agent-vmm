use super::{ImageStaging, VerifiedArchive};
use crate::{Containers, RuntimeContext, context::core_status, mutation::mutate};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tokio_stream::Stream;
use vmm_rpc::Status;

#[derive(Clone)]
pub struct ImageImports {
    pub directory: PathBuf,
    pub staging: Arc<dyn ImageStaging>,
}

impl ImageImports {
    pub async fn receive<S>(
        &self,
        ctx: RuntimeContext,
        containers: Containers,
        input: S,
    ) -> Result<h::ImportImageResponse, Status>
    where
        S: Stream<Item = Result<h::ImportImageRequest, Status>> + Send + Unpin,
    {
        let archive = VerifiedArchive::receive(&self.directory, input).await?;
        let mut header = archive.header().clone();
        // The expiring download URL is transport metadata. The digest and length
        // bind the same bytes on retries that carry a refreshed URL.
        header.archive_url.clear();
        let stage = self.staging.clone();
        mutate(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-import-image",
            header.request_id.clone(),
            header.encode_to_vec(),
            async move {
                let grant = Self::publish(ctx, containers, archive, stage).await?;
                Ok(h::ImportImageResponse { image: Some(grant) })
            },
        )
        .await
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
                super::base::retain(&dir, archive.path(), archive.digest)?;
                Ok::<_, Status>((archive, dir))
            }
        })
        .await
        .map_err(|_| Status::unavailable("image archive storage task interrupted"))??;
        let logical_bytes = archive.logical_bytes;
        let installed = archive
            .install_with_build(&ctx.core, scope, stage, output)
            .await
            .map_err(core_status);
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
