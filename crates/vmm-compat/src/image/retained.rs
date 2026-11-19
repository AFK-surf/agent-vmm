use crate::{
    RuntimeContext,
    error::{core_status, invalid_field},
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use tonic::Status;
use vmm_core::Error;

fn registration(ctx: &RuntimeContext) -> Result<&str, Status> {
    ctx.remote
        .as_ref()
        .map(|r| r.registration_id.as_str())
        .ok_or_else(|| Status::permission_denied("retained images require a controller allocation"))
}
fn status(e: Error) -> Status {
    if matches!(e, Error::Fenced) {
        Status::permission_denied("retained image authority is no longer current")
    } else {
        core_status(e)
    }
}
pub(crate) fn list(ctx: &RuntimeContext) -> Result<h::ListImagesResponse, Status> {
    let rows = ctx
        .core
        .store
        .list_retained_images(registration(ctx)?, ctx.session())
        .map_err(status)?;
    let images = rows
        .into_iter()
        .map(|r| {
            let receipt = h::ImageOperationReceipt::decode(r.build_receipt.as_slice())
                .map_err(|_| Status::data_loss("invalid retained build receipt"))?;
            Ok(h::Image {
                reference: r.reference,
                digest: format!("sha256:{}", hex::encode(r.digest)),
                generation_id: r.generation,
                logical_bytes: r.logical_bytes,
                platforms: vec!["linux/arm64".into()],
                build_receipt: Some(receipt),
                ..Default::default()
            })
        })
        .collect::<Result<_, Status>>()?;
    Ok(h::ListImagesResponse { images })
}
pub(crate) async fn delete(
    ctx: RuntimeContext,
    req: h::DeleteImageRequest,
) -> Result<h::DeleteImageResponse, Status> {
    let registration = registration(&ctx)?.to_owned();
    if req.expected_generation_id.is_empty() {
        return Err(invalid_field(
            "expected_generation_id",
            "retained generation required",
        ));
    }
    let digest = super::oci::digest(&req.expected_digest)
        .map_err(|_| invalid_field("expected_digest", "expected sha256 digest required"))?;
    let canonical = req
        .reference
        .parse::<oci_spec::distribution::Reference>()
        .map_err(|_| invalid_field("reference", "valid image reference required"))?
        .whole();
    mutate(
        ctx.core.store.clone(),
        ctx.scope.id.clone(),
        "runtime-delete-retained-image",
        req.request_id.clone(),
        req.encode_to_vec(),
        async move {
            ctx.core
                .remove_retained_image(
                    &registration,
                    &ctx.session,
                    &req.reference,
                    &canonical,
                    &req.expected_generation_id,
                    digest,
                )
                .await
                .map_err(status)?;
            Ok(h::DeleteImageResponse {})
        },
    )
    .await
}
