use super::{ImageImports, registry};
use crate::{
    Containers, RuntimeContext,
    context::core_status,
    error::{host_status, invalid_field},
    guest::Output,
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as h;
use oci_spec::distribution::Reference;
use prost::Message;
use tokio::sync::mpsc;
use vmm_rpc::{Code, Status};

fn progress(status: &str) -> h::PullImageResponse {
    h::PullImageResponse {
        status: status.into(),
        ..Default::default()
    }
}

impl ImageImports {
    pub(crate) fn pull(
        &self,
        ctx: RuntimeContext,
        containers: Containers,
        req: h::PullImageRequest,
    ) -> Result<Output<h::PullImageResponse>, Status> {
        if req.request_id.is_empty() || req.reference.is_empty() {
            return Err(invalid_field(
                "request_id/reference",
                "request ID and image reference are required",
            ));
        }
        let reference: Reference = req.reference.parse().map_err(|_| {
            registry::with_image(
                host_status(
                    Code::InvalidArgument,
                    h::ErrorReason::ImageReferenceInvalid,
                    "image reference is invalid",
                ),
                &req.reference,
            )
        })?;
        let imports = self.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            if tx.send(Ok(progress("resolving"))).await.is_err() {
                return;
            }
            let notify = tx.clone();
            let terminal = mutate(
                ctx.core.store.clone(),
                ctx.scope.id.clone(),
                "runtime-pull-image",
                req.request_id.clone(),
                req.encode_to_vec(),
                async move {
                    let canonical = reference.whole();
                    let cached = {
                        let _guard = containers.definitions.lock().await;
                        match super::grant::resolve(&ctx, &req.reference) {
                            Ok((name, image)) => Some(containers.image_grant(
                                &ctx.core.store,
                                &ctx.scope.id,
                                &name,
                                &image,
                                None,
                                false,
                            )?),
                            Err(vmm_core::Error::Missing(_)) => None,
                            Err(e) => return Err(core_status(e)),
                        }
                    };
                    let image = if let Some(image) = cached {
                        image
                    } else {
                        let _ = notify.try_send(Ok(progress("downloading from registry")));
                        let archive = registry::export(&imports.directory, &reference)
                            .await
                            .map_err(|s| registry::with_image(s, &canonical))?;
                        let grant =
                            Self::publish(ctx, containers, archive, imports.staging).await?;
                        let _ = notify.try_send(Ok(progress("resolving")));
                        grant
                    };
                    Ok(h::PullImageResponse {
                        status: "complete".into(),
                        digest: Some(image.digest),
                        logical_bytes: image.logical_bytes,
                        canonical_reference: canonical,
                    })
                },
            )
            .await;
            let _ = tx.send(terminal).await;
        });
        Ok(rx.into())
    }
}
