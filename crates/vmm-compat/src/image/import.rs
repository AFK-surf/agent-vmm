use super::{ImageStaging, VerifiedArchive};
use crate::{Containers, RuntimeContext, context::core_status, mutation::mutate};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tokio_stream::Stream;
use tonic::Status;

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
                let _guard = containers.definitions.lock().await;
                let reference = header.image_reference;
                let scope = ctx.scope.id;
                let new_grant = match ctx.core.store.resolve_image(&scope, &reference) {
                    Ok(_) => false,
                    Err(vmm_core::Error::Missing(_)) => true,
                    Err(error) => return Err(core_status(error)),
                };
                let logical_bytes = archive.logical_bytes;
                let image = archive
                    .install(&ctx.core, &scope, stage)
                    .await
                    .map_err(core_status)?;
                let grant = containers.image_grant(
                    &scope,
                    &reference,
                    &image,
                    Some(logical_bytes),
                    new_grant,
                )?;
                Ok(h::ImportImageResponse { image: Some(grant) })
            },
        )
        .await
    }
}
