//! Canonical manifests and their admission receipts are committed together.
use super::{ContentStore, a, h, storage, validate};
use crate::error::{host_status, with_resource};
use prost::Message;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};
use vmm_rpc::{Code, Status};

fn limits(limits: Option<&h::ResourceLimits>) -> bool {
    limits.is_some_and(|r| r.pids.unwrap_or(0) > 0 && r.disk_bytes.unwrap_or(0) > 0)
}

fn canonical(m: &a::ControllerApplianceManifest) -> Result<Vec<u8>, Status> {
    if m.encoded_len() > 1 << 20
        || m.contract_version != "1"
        || m.product_id.is_empty()
        || !(1 << 30..=64 << 30).contains(&m.data_volume_bytes)
        || !(1..=300).contains(&m.startup_deadline_seconds)
        || !limits(m.system_limits.as_ref())
        || !limits(
            m.requested_capacity
                .as_ref()
                .and_then(|c| c.per_environment_limits.as_ref()),
        )
    {
        return Err(Status::failed_precondition(
            "invalid appliance manifest or resource declaration",
        ));
    }
    if (a::GetContentStatusRequest {
        content_refs: m.content_refs.clone(),
    })
    .encoded_len()
        > 32 << 10
    {
        return Err(Status::failed_precondition(
            "manifest content references exceed the frame limit",
        ));
    }
    let mut controller = false;
    let mut seen = HashSet::new();
    for r in &m.content_refs {
        validate(r).map_err(|e| Status::failed_precondition(e.message().to_owned()))?;
        if !seen.insert((r.role, &r.image_manifest_digest)) {
            return Err(Status::failed_precondition(
                "duplicate appliance content reference",
            ));
        }
        if r.role() == a::ApplianceContentRole::Controller {
            if controller
                || r.image_reference != m.image_reference
                || r.image_manifest_digest != m.image_manifest_digest
                || r.platform != m.platform
            {
                return Err(Status::failed_precondition(
                    "controller content does not bind the manifest image",
                ));
            }
            controller = true;
        }
    }
    if !controller {
        return Err(Status::failed_precondition("controller content required"));
    }
    Ok(m.encode_to_vec())
}

impl ContentStore {
    pub(in crate::operator) async fn stage_artifact(
        &self,
        request: a::StageArtifactRequest,
    ) -> Result<a::StageArtifactResponse, Status> {
        if request.request_id.is_empty() || request.request_id.len() > 32 << 10 {
            return Err(Status::invalid_argument("bounded request_id required"));
        }
        let m = request
            .manifest
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("manifest required"))?;
        let canonical = canonical(m)?;
        let hash = Sha256::digest(&canonical);
        let prior = || -> Result<Option<a::StageArtifactResponse>, Status> {
            let Some(op) = self.lookup(&request.request_id, true)? else {
                return Ok(None);
            };
            if op.kind() == a::ApplianceOperationKind::Stage
                && op.deadline_unix_millis == request.deadline_unix_millis
            {
                let id = std::str::from_utf8(&op.receipt)
                    .map_err(|_| Status::data_loss("invalid artifact receipt"))?;
                let db = self.db.lock().map_err(storage)?;
                let matches: bool = db
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM appliance_artifacts WHERE id=? AND hash=?)",
                        params![id, hash.as_slice()],
                        |r| r.get(0),
                    )
                    .map_err(storage)?;
                if matches {
                    return Ok(Some(a::StageArtifactResponse {
                        artifact_id: id.to_owned(),
                        operation: Some(op),
                    }));
                }
            }
            Err(with_resource(
                host_status(
                    Code::Aborted,
                    h::ErrorReason::RequestIdConflict,
                    "artifact request has a different payload",
                ),
                "appliance_artifact",
                "",
            ))
        };
        if let Some(response) = prior()? {
            return Ok(response);
        }
        let wait = || {
            request
                .deadline_unix_millis
                .saturating_sub(crate::execution::now().unwrap_or(i64::MAX) / 1_000_000)
        };
        let expired = || Status::deadline_exceeded("artifact admission deadline elapsed");
        let remaining = wait();
        if remaining <= 0 {
            return Err(expired());
        }
        let _writer =
            tokio::time::timeout(Duration::from_millis(remaining as u64), self.writer.lock())
                .await
                .map_err(|_| expired())?;
        if let Some(response) = prior()? {
            return Ok(response);
        }
        let mut db = self.db.lock().map_err(storage)?;
        let tx = db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        if wait() <= 0 {
            return Err(expired());
        }
        let existing = tx
            .query_row(
                "SELECT id FROM appliance_artifacts WHERE hash=?",
                [hash.as_slice()],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(storage)?;
        let id = match existing {
            Some(id) => id,
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                tx.execute(
                    "INSERT INTO appliance_artifacts(id,hash,manifest) VALUES(?,?,?)",
                    params![id, hash.as_slice(), canonical],
                )
                .map_err(storage)?;
                id
            }
        };
        let now = Some(SystemTime::now().into());
        let op = a::ApplianceOperation {
            operation_id: uuid::Uuid::new_v4().to_string(),
            request_id: request.request_id,
            deadline_unix_millis: request.deadline_unix_millis,
            receipt: id.as_bytes().to_vec(),
            kind: a::ApplianceOperationKind::Stage as i32,
            state: a::ApplianceOperationState::Succeeded as i32,
            phase: a::ApplianceOperationPhase::Terminal as i32,
            terminal_reason: a::ApplianceTerminalReason::Succeeded as i32,
            started_at: now,
            updated_at: now,
            ..Default::default()
        };
        tx.execute(
            "INSERT INTO appliance_receipts(id,request,operation) VALUES(?,?,?)",
            params![op.operation_id, op.request_id, op.encode_to_vec()],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(a::StageArtifactResponse {
            operation: Some(op),
            artifact_id: id,
        })
    }
}
