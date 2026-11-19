use crate::{
    RuntimeContext,
    error::{core_status, host_status, invalid_field, with_metadata, with_resource},
};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use sha2::{Digest, Sha256};
use vmm_rpc::{Code, Status};
use vmm_core::{CopyAuthority, CopySource, Error, ImageCopyReceipt};

fn authority<'a>(
    ctx: &'a RuntimeContext,
    target: &'a str,
    generation: u64,
    source: &'a CopySource,
) -> Result<CopyAuthority<'a>, Status> {
    let caller = ctx
        .remote
        .as_ref()
        .ok_or_else(|| Status::permission_denied("image copy requires a controller session"))?;
    if target != caller.allocation_id || generation != caller.generation {
        return Err(Status::permission_denied(
            "target differs from current allocation",
        ));
    }
    Ok(CopyAuthority {
        session: ctx.session(),
        registration: &caller.registration_id,
        target,
        generation,
        source,
    })
}
fn source(origin: Option<h::ImageSource>) -> Result<CopySource, Status> {
    use h::image_source::Source;
    match origin.and_then(|s| s.source) {
        Some(Source::Allocation(a))
            if !a.allocation_id.is_empty() && a.allocation_generation > 0 =>
        {
            Ok(CopySource::Allocation {
                id: a.allocation_id,
                generation: a.allocation_generation,
            })
        }
        Some(Source::Retained(r)) if !r.build_request_id.is_empty() => Ok(CopySource::Retained {
            build_request: r.build_request_id,
        }),
        _ => Err(invalid_field("source", "an exact copy source is required")),
    }
}
fn error(e: Error, reference: &str) -> Status {
    match e {
        Error::Fenced => Status::permission_denied("image copy authority is no longer current"),
        Error::Missing("retained image") => with_resource(
            with_metadata(
                Code::NotFound,
                h::ErrorReason::ImageCacheNotAdmitted,
                "retained image unavailable",
                [("admission".into(), "not_accepted".into())].into(),
            ),
            "image",
            reference,
        ),
        Error::Conflict("copy digest") => with_resource(
            host_status(
                Code::Aborted,
                h::ErrorReason::ImageDigestConflict,
                "image reference does not match the expected digest",
            ),
            "image",
            reference,
        ),
        other => core_status(other),
    }
}
pub(super) fn receipt(r: ImageCopyReceipt) -> h::ImageOperationReceipt {
    h::ImageOperationReceipt {
        request_id: r.request_id,
        input_sha256: hex::encode(r.fingerprint),
        kind: h::ImageOperationKind::Copy as i32,
        state: h::ImageOperationState::Succeeded as i32,
        writer_settled: true,
        image_reference: r.reference,
        manifest_digest: format!("sha256:{}", hex::encode(r.digest)),
        ..Default::default()
    }
}

pub(crate) async fn copy(
    ctx: &RuntimeContext,
    req: h::CopyImageRequest,
) -> Result<h::CopyImageResponse, Status> {
    if req.request_id.is_empty() || req.request_id.len() > 128 {
        return Err(invalid_field("request_id", "bounded request ID required"));
    }
    let canonical = req
        .image_reference
        .parse::<oci_spec::distribution::Reference>()
        .map_err(|_| invalid_field("image_reference", "valid image reference required"))?
        .whole();
    let digest = super::oci::digest(&req.manifest_digest)
        .map_err(|_| invalid_field("manifest_digest", "expected sha256 digest required"))?;
    let fingerprint = Sha256::digest(req.encode_to_vec()).into();
    let origin = source(req.source)?;
    let authority = authority(
        ctx,
        &req.target_allocation_id,
        req.target_allocation_generation,
        &origin,
    )?;
    let stored = ctx
        .core
        .store
        .copy_image_grant(
            &authority,
            &req.request_id,
            fingerprint,
            &req.image_reference,
            &canonical,
            digest,
        )
        .await
        .map_err(|e| error(e, &req.image_reference))?;
    let build_receipt = stored
        .build_receipt
        .as_deref()
        .map(h::ImageOperationReceipt::decode)
        .transpose()
        .map_err(|_| Status::data_loss("invalid stored build receipt"))?;
    let generation_id = stored.generation.clone();
    let receipt = receipt(stored);
    Ok(h::CopyImageResponse {
        image: Some(h::Image {
            reference: receipt.image_reference.clone(),
            digest: receipt.manifest_digest.clone(),
            generation_id,
            build_receipt,
            copy_receipt: Some(receipt),
            ..Default::default()
        }),
    })
}

pub(crate) fn observe(
    ctx: &RuntimeContext,
    req: h::ObserveImageOperationRequest,
) -> Result<h::ImageOperationReceipt, Status> {
    let origin = source(req.copy_source)?;
    let authority = authority(
        ctx,
        &req.target_allocation_id,
        req.target_allocation_generation,
        &origin,
    )?;
    if !req.execution_id.is_empty()
        || !req.container_id.is_empty()
        || !req.expected_instance_id.is_empty()
    {
        return Err(Status::aborted("copy operation has no build execution"));
    }
    let fingerprint = super::oci::digest(&format!("sha256:{}", req.input_sha256))
        .map_err(|_| invalid_field("input_sha256", "expected lowercase sha256 required"))?;
    let r = ctx
        .core
        .store
        .observe_image_copy(&authority, &req.request_id, fingerprint)
        .map_err(|e| error(e, ""))?;
    Ok(receipt(r))
}

pub(crate) fn cancel(
    ctx: &RuntimeContext,
    req: h::CancelImageOperationRequest,
) -> Result<h::ImageOperationReceipt, Status> {
    // The grant and writer settlement have one commit. Cancellation observes either
    // an unaccepted request or its immutable terminal receipt.
    observe(
        ctx,
        h::ObserveImageOperationRequest {
            request_id: req.request_id,
            kind: req.kind,
            input_sha256: req.input_sha256,
            execution_id: req.execution_id,
            container_id: req.container_id,
            expected_instance_id: req.expected_instance_id,
            copy_source: req.copy_source,
            target_allocation_id: req.target_allocation_id,
            target_allocation_generation: req.target_allocation_generation,
        },
    )
}
