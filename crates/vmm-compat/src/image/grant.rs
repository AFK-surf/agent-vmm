use crate::{
    Containers, RuntimeContext,
    container::db_error,
    context::core_status,
    error::{host_status, invalid_field},
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use tonic::{Code, Status};
use vmm_core::ImageArtifact;
use vmm_core::Store;

pub(crate) fn resolve(
    ctx: &RuntimeContext,
    input: &str,
) -> vmm_core::Result<(String, ImageArtifact)> {
    let store = &ctx.core.store;
    match store.resolve_image(&ctx.scope.id, input) {
        Ok(image) => Ok((input.into(), image)),
        Err(vmm_core::Error::Missing(kind)) => {
            let normalized = input
                .parse::<oci_spec::distribution::Reference>()
                .map_err(|_| vmm_core::Error::Missing(kind))?
                .whole();
            if normalized == input {
                return Err(vmm_core::Error::Missing(kind));
            }
            let image = store.resolve_image(&ctx.scope.id, &normalized)?;
            Ok((normalized, image))
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn initialize(db: &Connection) -> Result<(), Status> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS image_grants(scope TEXT NOT NULL,reference TEXT NOT NULL,digest BLOB NOT NULL,generation TEXT NOT NULL,logical_bytes INTEGER,PRIMARY KEY(scope,reference))").map_err(db_error)?;
    if !db.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('image_grants') WHERE name='logical_bytes')", [], |r| r.get::<_, bool>(0)).map_err(db_error)? {
        db.execute_batch("ALTER TABLE image_grants ADD COLUMN logical_bytes INTEGER").map_err(db_error)?;
    }
    if !db.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('image_grants') WHERE name='core_generation')", [], |r| r.get::<_, bool>(0)).map_err(db_error)? {
        db.execute_batch("ALTER TABLE image_grants ADD COLUMN core_generation TEXT").map_err(db_error)?;
    }
    Ok(())
}

impl Containers {
    pub(super) fn image_grant(
        &self,
        store: &Store,
        scope: &str,
        reference: &str,
        image: &ImageArtifact,
        bytes: Option<u64>,
        new: bool,
    ) -> Result<h::Image, Status> {
        let metadata = store
            .image_grant_metadata(scope, reference, image.manifest_digest)
            .map_err(core_status)?;
        let logical = bytes
            .map(i64::try_from)
            .transpose()
            .map_err(|_| Status::out_of_range("logical image size"))?;
        let (generation_id, logical_bytes) = self.db()?.query_row(
            "INSERT INTO image_grants(scope,reference,digest,generation,logical_bytes,core_generation) VALUES(?1,?2,?3,?4,?5,?4)
             ON CONFLICT(scope,reference) DO UPDATE SET
             generation=CASE WHEN ?6 OR digest!=excluded.digest OR (core_generation IS NOT NULL AND core_generation!=excluded.core_generation) THEN excluded.generation ELSE generation END,
             logical_bytes=CASE WHEN ?6 OR digest!=excluded.digest OR (core_generation IS NOT NULL AND core_generation!=excluded.core_generation) THEN excluded.logical_bytes ELSE COALESCE(excluded.logical_bytes,logical_bytes) END,
             core_generation=excluded.core_generation,digest=excluded.digest RETURNING generation,COALESCE(logical_bytes,0)",
            params![scope,reference,image.manifest_digest,metadata.generation,logical,new],
            |r| Ok((r.get(0)?, r.get::<_,i64>(1)?))
        ).map_err(db_error)?;
        Ok(h::Image {
            reference: reference.into(),
            digest: format!("sha256:{}", hex::encode(image.manifest_digest)),
            logical_bytes: u64::try_from(logical_bytes)
                .map_err(|_| Status::data_loss("invalid stored image size"))?,
            platforms: vec!["linux/arm64".into()],
            generation_id,
            build_receipt: metadata
                .build_receipt
                .as_deref()
                .map(h::ImageOperationReceipt::decode)
                .transpose()
                .map_err(|_| Status::data_loss("invalid stored build receipt"))?,
            copy_receipt: metadata.copy_receipt.map(super::copy::receipt),
            ..Default::default()
        })
    }

    pub(crate) async fn list_grants(
        &self,
        ctx: &RuntimeContext,
    ) -> Result<h::ListImagesResponse, Status> {
        let _guard = self.definitions.lock().await;
        let scope = &ctx.scope.id;
        let mut users = HashMap::<String, Vec<String>>::new();
        for sb in ctx
            .core
            .store
            .list_sandboxes(scope, None, usize::MAX)
            .map_err(core_status)?
        {
            users
                .entry(sb.definition.image_reference)
                .or_default()
                .push(sb.definition.key.sandbox_id);
        }
        let mut images = Vec::new();
        for (reference, artifact) in ctx
            .core
            .store
            .list_images(scope, None, usize::MAX)
            .map_err(core_status)?
        {
            let mut image =
                self.image_grant(&ctx.core.store, scope, &reference, &artifact, None, false)?;
            image.used_by_container_ids = users.remove(&reference).unwrap_or_default();
            images.push(image);
        }
        Ok(h::ListImagesResponse { images })
    }

    pub(crate) async fn delete_grant(
        &self,
        ctx: RuntimeContext,
        req: h::DeleteImageRequest,
    ) -> Result<h::DeleteImageResponse, Status> {
        let expected = super::oci::digest(&req.expected_digest)
            .map_err(|_| invalid_field("expected_digest", "expected sha256 digest required"))?;
        if req.expected_generation_id.is_empty() {
            return Err(invalid_field(
                "expected_generation_id",
                "image generation is required",
            ));
        }
        let containers = self.clone();
        mutate(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-delete-image",
            req.request_id.clone(),
            req.encode_to_vec(),
            async move {
                let _guard = containers.definitions.lock().await;
                let scope = &ctx.scope.id;
                let (reference, image) = resolve(&ctx, &req.reference).map_err(core_status)?;
                let reference = &reference;
                if image.manifest_digest != expected {
                    return Err(host_status(
                        Code::Aborted,
                        h::ErrorReason::ImageDigestConflict,
                        "image content changed",
                    ));
                }
                if containers
                    .image_grant(&ctx.core.store, scope, reference, &image, None, false)?
                    .generation_id
                    != req.expected_generation_id
                {
                    return Err(host_status(
                        Code::Aborted,
                        h::ErrorReason::ImageGenerationConflict,
                        "image grant changed",
                    ));
                }
                if ctx
                    .core
                    .store
                    .list_sandboxes(scope, None, usize::MAX)
                    .map_err(core_status)?
                    .iter()
                    .any(|s| &s.definition.image_reference == reference)
                {
                    return Err(host_status(
                        Code::FailedPrecondition,
                        h::ErrorReason::ImageInUse,
                        "image is used by a container",
                    ));
                }
                ctx.core
                    .remove_image_reference(scope, reference, expected)
                    .await
                    .map_err(core_status)?;
                containers
                    .db()?
                    .execute(
                        "DELETE FROM image_grants WHERE scope=? AND reference=? AND generation=?",
                        params![scope, reference, req.expected_generation_id],
                    )
                    .map_err(db_error)?;
                Ok(h::DeleteImageResponse {})
            },
        )
        .await
    }
}
