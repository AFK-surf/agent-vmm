//! Persist the admission before writing its protected seed. Recovery uses the
//! same private registration, volume and bootstrap identities on either side of a crash.
use super::{ContentStore, a, grant, h, storage};
use crate::error::{core_status, invalid_field, with_metadata, with_resource};
use prost::Message;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest as _, Sha256};
use std::time::{Duration, SystemTime};
use tonic::{Code, Status};
use uuid::Uuid;
use zeroize::Zeroizing;

pub(super) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS managed_installations(
 id TEXT PRIMARY KEY, status BLOB NOT NULL, volume TEXT NOT NULL UNIQUE, bootstrap TEXT NOT NULL UNIQUE,
 operation TEXT NOT NULL UNIQUE, published INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS appliance_install_requests(operation TEXT PRIMARY KEY, fingerprint BLOB NOT NULL, seed_hash BLOB NOT NULL);";

fn deadline(id: &str) -> Status {
    with_resource(
        with_metadata(
            Code::DeadlineExceeded,
            h::ErrorReason::ApplianceOperationNotAdmitted,
            "appliance admission deadline elapsed",
            [("admission".into(), "not_accepted".into())].into(),
        ),
        "appliance_installation",
        id,
    )
}
fn remaining(end: i64, id: &str) -> Result<Duration, Status> {
    let ms = end.saturating_sub(crate::execution::now()? / 1_000_000);
    if ms <= 0 {
        return Err(deadline(id));
    }
    Ok(Duration::from_millis(ms as u64))
}
fn id() -> String {
    Uuid::new_v4().to_string()
}
impl ContentStore {
    fn install_manifest(
        &self,
        r: &a::PlanInstallRequest,
    ) -> Result<a::ControllerApplianceManifest, Status> {
        if r.installation_id.is_empty() || r.request_id.is_empty() {
            return Err(invalid_field("installation_id/request_id", "required"));
        }
        remaining(r.deadline_unix_millis, &r.installation_id)?;
        if r.expected_revision != 0 || r.artifact_id.is_empty() {
            return Err(Status::aborted("install plan is stale"));
        }
        if r.installation_id == "agent-vmm-mcp"
            || self
                .core
                .appliance(&r.installation_id)
                .map_err(core_status)?
                .is_some()
        {
            return Err(Status::already_exists("installation already exists"));
        }
        let db = self.db.lock().map_err(storage)?;
        let exists: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM managed_installations WHERE id=?)",
                [&r.installation_id],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if exists {
            return Err(Status::already_exists("installation already admitted"));
        }
        let manifest: Vec<u8> = db
            .query_row(
                "SELECT manifest FROM appliance_artifacts WHERE id=?",
                [&r.artifact_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?
            .ok_or_else(|| Status::failed_precondition("artifact unavailable"))?;
        drop(db);
        let m = a::ControllerApplianceManifest::decode(manifest.as_slice())
            .map_err(|_| Status::data_loss("invalid stored artifact"))?;
        let g = r
            .approved_grant
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("approved grant required"))?;
        grant::approved(&m, g, self.workload_disk)?;
        for content in &m.content_refs {
            if !self.available(content)? {
                return Err(Status::failed_precondition(
                    "approved appliance content must be staged before admission",
                ));
            }
        }
        Ok(m)
    }
    pub(in crate::operator) fn plan_install(
        &self,
        r: a::PlanInstallRequest,
    ) -> Result<a::PlanInstallResponse, Status> {
        self.install_manifest(&r)?;
        Ok(a::PlanInstallResponse {
            plan: Some(a::AppliancePlan {
                installation_id: r.installation_id,
                artifact_id: r.artifact_id,
                kind: a::AppliancePlanKind::Install as i32,
                ..Default::default()
            }),
        })
    }
    pub(in crate::operator) async fn install(
        &self,
        mut r: a::InstallRequest,
    ) -> Result<a::InstallResponse, Status> {
        let seed = Zeroizing::new(std::mem::take(&mut r.opaque_bootstrap_seed));
        if !(1..=vmm_signing::MAX_BOOTSTRAP_BYTES).contains(&seed.len()) {
            return Err(invalid_field(
                "opaque_bootstrap_seed",
                "seed exceeds bounds or is empty",
            ));
        }
        let seed_hash = Sha256::digest(seed.as_slice());
        let fingerprint = Sha256::new()
            .chain_update(r.encode_to_vec())
            .chain_update(seed_hash)
            .finalize();
        let replay = || -> Result<Option<a::ApplianceOperation>, Status> {
            let Some(op) = self.lookup(&r.request_id, true)? else {
                return Ok(None);
            };
            if op.kind() == a::ApplianceOperationKind::Install
                && op.installation_id == r.installation_id
            {
                let db = self.db.lock().map_err(storage)?;
                let matches:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM appliance_install_requests WHERE operation=? AND fingerprint=?)",params![op.operation_id,fingerprint.as_slice()],|r|r.get(0)).map_err(storage)?;
                if matches {
                    return Ok(Some(op));
                }
            }
            Err(with_resource(
                crate::error::host_status(
                    Code::Aborted,
                    h::ErrorReason::RequestIdConflict,
                    "appliance request was reused",
                ),
                "appliance_installation",
                &r.installation_id,
            ))
        };
        let result = |op| {
            Ok(a::InstallResponse {
                operation: Some(op),
                artifact_id: String::new(),
            })
        };
        if let Some(op) = replay()?
            && op.state() != a::ApplianceOperationState::Running
        {
            return result(op);
        }
        let wait = remaining(r.deadline_unix_millis, &r.installation_id)?;
        let _writer = tokio::time::timeout(wait, self.writer.lock())
            .await
            .map_err(|_| deadline(&r.installation_id))?;
        if let Some(op) = replay()? {
            return result(self.finish_install(op)?);
        }
        let plan = a::PlanInstallRequest {
            installation_id: r.installation_id.clone(),
            request_id: r.request_id.clone(),
            expected_revision: r.expected_revision,
            artifact_id: r.artifact_id.clone(),
            deadline_unix_millis: r.deadline_unix_millis,
            approved_grant: r.approved_grant.take(),
        };
        let manifest = self.install_manifest(&plan)?;
        let mut grant = plan.approved_grant.expect("validated grant");
        grant.revision = 1;
        let status = a::ApplianceStatus {
            installation_id: r.installation_id.clone(),
            product_id: manifest.product_id,
            artifact_id: r.artifact_id,
            revision: 1,
            registration_id: id(),
            appliance_state: a::ApplianceState::Stopped as i32,
            channel_state: a::ControllerChannelState::Disconnected as i32,
            approved_grant: Some(grant),
            runtime_stopped: Some(true),
            ..Default::default()
        };
        let now = Some(SystemTime::now().into());
        let op = a::ApplianceOperation {
            operation_id: id(),
            installation_id: r.installation_id,
            request_id: r.request_id,
            kind: a::ApplianceOperationKind::Install as i32,
            state: a::ApplianceOperationState::Running as i32,
            phase: a::ApplianceOperationPhase::Planned as i32,
            started_at: now,
            updated_at: now,
            deadline_unix_millis: r.deadline_unix_millis,
            ..Default::default()
        };
        let bootstrap = id();
        {
            let mut db = self.db.lock().map_err(storage)?;
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(storage)?;
            remaining(r.deadline_unix_millis, &op.installation_id)?;
            tx.execute("INSERT INTO managed_installations(id,status,volume,bootstrap,operation) VALUES(?,?,?,?,?)",params![op.installation_id,status.encode_to_vec(),id(),bootstrap,op.operation_id]).map_err(storage)?;
            tx.execute(
                "INSERT INTO appliance_install_requests VALUES(?,?,?)",
                params![
                    op.operation_id,
                    fingerprint.as_slice(),
                    seed_hash.as_slice()
                ],
            )
            .map_err(storage)?;
            tx.execute(
                "INSERT INTO appliance_receipts(id,request,operation) VALUES(?,?,?)",
                params![op.operation_id, op.request_id, op.encode_to_vec()],
            )
            .map_err(storage)?;
            tx.commit().map_err(storage)?;
        }
        self.secrets
            .store_bootstrap(&bootstrap, &seed)
            .map_err(storage)?;
        result(self.finish_install(op)?)
    }

    pub(super) fn finish_install(
        &self,
        mut op: a::ApplianceOperation,
    ) -> Result<a::ApplianceOperation, Status> {
        if op.state() != a::ApplianceOperationState::Running {
            return Ok(op);
        }
        let (raw,bootstrap,seed_hash):(Vec<u8>,String,Vec<u8>)=self.db.lock().map_err(storage)?.query_row(
            "SELECT m.status,m.bootstrap,r.seed_hash FROM managed_installations m JOIN appliance_install_requests r ON r.operation=m.operation WHERE m.operation=?", [&op.operation_id],
            |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map_err(storage)?;
        let status = a::ApplianceStatus::decode(raw.as_slice())
            .map_err(|_| Status::data_loss("invalid installation state"))?;
        let present = match self.secrets.load_bootstrap(&bootstrap) {
            Ok(seed) => Sha256::digest(seed.as_slice()).as_slice() == seed_hash,
            Err(vmm_signing::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(storage(e)),
        };
        if present {
            if let Some(existing) = self
                .core
                .appliance(&status.installation_id)
                .map_err(core_status)?
            {
                if existing.registration_id != status.registration_id {
                    return Err(Status::data_loss("private registration conflict"));
                }
            } else {
                self.core
                    .install_appliance(&status.installation_id, &status.registration_id)
                    .map_err(core_status)?;
            }
        } else {
            if self
                .core
                .appliance(&status.installation_id)
                .map_err(core_status)?
                .is_some()
            {
                return Err(Status::data_loss("installed bootstrap seed missing"));
            }
            self.secrets.delete_bootstrap(&bootstrap).map_err(storage)?;
            op.error = Some(a::ApplianceOperationError {
                reason: "INSTALL_INTERRUPTED".into(),
                message: "protected bootstrap seed was not published".into(),
            });
        }
        let mut db = self.db.lock().map_err(storage)?;
        let tx = db.transaction().map_err(storage)?;
        if present {
            tx.execute(
                "UPDATE managed_installations SET published=1 WHERE operation=?",
                [&op.operation_id],
            )
            .map_err(storage)?;
            op.state = a::ApplianceOperationState::Succeeded as i32;
            op.terminal_reason = a::ApplianceTerminalReason::Succeeded as i32;
        } else {
            tx.execute(
                "DELETE FROM managed_installations WHERE operation=?",
                [&op.operation_id],
            )
            .map_err(storage)?;
            op.state = a::ApplianceOperationState::Failed as i32;
            op.terminal_reason = a::ApplianceTerminalReason::OperationFailed as i32;
        }
        op.phase = a::ApplianceOperationPhase::Terminal as i32;
        op.updated_at = Some(SystemTime::now().into());
        tx.execute(
            "UPDATE appliance_receipts SET operation=? WHERE id=?",
            params![op.encode_to_vec(), op.operation_id],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(op)
    }
    pub(super) fn recover_installations(&self) -> Result<(), Status> {
        let pending = {
            let db = self.db.lock().map_err(storage)?;
            let mut query = db
                .prepare("SELECT operation FROM managed_installations WHERE published=0")
                .map_err(storage)?;
            query
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?
        };
        for id in pending {
            let op = self
                .lookup(&id, false)?
                .ok_or_else(|| Status::data_loss("missing installation receipt"))?;
            self.finish_install(op)?;
        }
        Ok(())
    }
    pub(in crate::operator) fn statuses(
        &self,
        id: Option<&str>,
    ) -> Result<Vec<a::ApplianceStatus>, Status> {
        let db = self.db.lock().map_err(storage)?;
        let mut query=db.prepare("SELECT status FROM managed_installations WHERE published=1 AND (?1 IS NULL OR id=?1) ORDER BY id").map_err(storage)?;
        query
            .query_map([id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(storage)?
            .map(|row| {
                a::ApplianceStatus::decode(row.map_err(storage)?.as_slice())
                    .map_err(|_| Status::data_loss("invalid appliance status"))
            })
            .collect()
    }
}
