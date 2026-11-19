//! Durable local disposal receipts. The protocol store records intent before Core isolates a target.
use super::*;
use crate::context::core_status as runtime_status;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{path::Path, sync::Mutex};
use vmm_core::Core;

pub trait RegistrationControl: Send + Sync {
    /// Return the durable operator policy revision and display name.
    fn metadata(&self, id: &str) -> Result<(u64, String), Status>;
    /// Hold registration admission while checking the policy revision, stopping the connector
    /// and recording the fence. Restore admission if isolation fails.
    fn isolate(
        &self,
        id: &str,
        revision: u64,
        fence: &mut dyn FnMut() -> Result<(), Status>,
    ) -> Result<(), Status>;
    fn delete_credential(&self, id: &str) -> Result<(), Status>;
}

pub struct LocalDisposals {
    store: Arc<Store>,
    database: Mutex<Connection>,
    registrations: Arc<dyn RegistrationControl>,
}

type Kind = h::LocalDisposalTargetKind;

fn db_error(_: rusqlite::Error) -> Status {
    Status::unavailable("local disposal receipt store unavailable")
}

struct Receipt {
    plan: h::LocalDisposalPlan,
    operation: h::Operation,
}

impl Receipt {
    fn decode((plan, op): (Vec<u8>, Vec<u8>)) -> Result<Self, Status> {
        let invalid = |_| Status::data_loss("invalid local disposal receipt");
        Ok(Self {
            plan: h::LocalDisposalPlan::decode(plan.as_slice()).map_err(invalid)?,
            operation: h::Operation::decode(op.as_slice()).map_err(invalid)?,
        })
    }
}

impl LocalDisposals {
    pub fn open(
        path: &Path,
        store: Arc<Store>,
        registrations: Arc<dyn RegistrationControl>,
    ) -> Result<Self, Status> {
        let db = Connection::open(path).map_err(db_error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS local_disposals(operation_id TEXT PRIMARY KEY,request TEXT UNIQUE NOT NULL,kind INTEGER NOT NULL,target TEXT NOT NULL,plan BLOB NOT NULL,operation BLOB NOT NULL,complete INTEGER NOT NULL DEFAULT 0,UNIQUE(kind,target));
            CREATE INDEX IF NOT EXISTS pending_disposals ON local_disposals(operation_id) WHERE complete=0;").map_err(db_error)?;
        Ok(Self {
            store,
            database: Mutex::new(db),
            registrations,
        })
    }

    fn db(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Status> {
        self.database
            .lock()
            .map_err(|_| Status::unavailable("local disposal receipt store unavailable"))
    }

    pub fn plan(&self, req: h::PlanLocalDisposalRequest) -> Result<h::LocalDisposalPlan, Status> {
        if req.target_id.is_empty() {
            return Err(invalid_field(
                "target_id",
                "an exact local target is required",
            ));
        }
        let id = &req.target_id;
        let mut plan = h::LocalDisposalPlan {
            target_kind: req.target_kind,
            target_id: id.clone(),
            activity_unknown: true,
            ..Default::default()
        };
        match req.target_kind() {
            Kind::Environment => {
                let scope = self.store.scope(id).map_err(runtime_status)?;
                if scope.origin != Origin::Local {
                    return Err(Status::failed_precondition(
                        "remote environments require a new plan for their entire registration",
                    ));
                }
                if id.starts_with("system.") {
                    return Err(Status::failed_precondition("private system environment"));
                }
                plan.revision = scope.revision;
                plan.namespace_id = id.clone();
                plan.display_name = id.clone();
                plan.environment_count = 1;
            }
            Kind::Registration => {
                plan.environment_count = self
                    .store
                    .operator_registration_scope_count(id)
                    .map_err(runtime_status)?;
                (plan.revision, plan.display_name) = self.registrations.metadata(id)?;
            }
            Kind::Unspecified => {
                return Err(invalid_field(
                    "target_kind",
                    "select a local environment or exact external registration",
                ));
            }
        }
        if plan.revision == 0 {
            return Err(Status::data_loss("invalid stored operator revision"));
        }
        Ok(plan)
    }

    fn isolate(&self, plan: &h::LocalDisposalPlan, request: &str) -> Result<(), Status> {
        match plan.target_kind() {
            Kind::Environment => {
                self.store
                    .begin_operator_environment_disposal(&plan.target_id, plan.revision, request)
                    .map_err(disposal_status)?;
            }
            Kind::Registration => {
                let id = &plan.target_id;
                if !self
                    .store
                    .check_operator_registration_disposal(id, request)
                    .map_err(disposal_status)?
                {
                    self.registrations.isolate(id, plan.revision, &mut || {
                        self.store
                            .begin_operator_registration_disposal(id, request)
                            .map_err(disposal_status)
                    })?;
                }
            }
            Kind::Unspecified => {
                return Err(invalid_field("target_kind", "invalid local deletion scope"));
            }
        }
        Ok(())
    }

    pub fn execute(&self, req: h::ExecuteLocalDisposalRequest) -> Result<h::Operation, Status> {
        let plan = req
            .plan
            .filter(|p| p.revision > 0 && !p.target_id.is_empty());
        let Some(plan) = plan.filter(|_| {
            req.irreversible_confirmation
                && !req.request_id.is_empty()
                && req.request_id.len() <= 128
        }) else {
            return Err(invalid_field(
                "request_id/plan/irreversible_confirmation",
                "confirm the exact revision-bound local deletion plan",
            ));
        };
        let db = self.db()?;
        let rows = db.prepare_cached("SELECT plan,operation FROM local_disposals WHERE request=? OR (kind=? AND target=?)")
            .and_then(|mut stmt|stmt.query_map(params![req.request_id,plan.target_kind,plan.target_id],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(db_error)?;
        if rows.len() > 1 {
            return Err(Status::aborted(
                "local deletion request conflicts with existing receipt",
            ));
        }
        if let Some(row) = rows.into_iter().next() {
            let existing = Receipt::decode(row)?;
            if existing.plan.target_id != plan.target_id
                || existing.plan.target_kind != plan.target_kind
                || existing.plan.namespace_id != plan.namespace_id
                || (existing.plan.revision != plan.revision
                    && (req.request_id == existing.operation.request_id
                        || plan.target_kind() != Kind::Registration))
            {
                return Err(Status::aborted("local deletion scope changed"));
            }
            if existing.operation.stage == "accepting" {
                return self.accept(&db, &existing.plan, existing.operation);
            }
            return Ok(existing.operation);
        }
        let current = self.plan(h::PlanLocalDisposalRequest {
            target_id: plan.target_id.clone(),
            target_kind: plan.target_kind,
        })?;
        if current.revision != plan.revision || current.namespace_id != plan.namespace_id {
            return Err(Status::aborted("local deletion scope changed"));
        }
        let now = Some(SystemTime::now().into());
        let op = h::Operation {
            id: Uuid::new_v4().to_string(),
            kind: "local-disposal".into(),
            resource: Some(h::ResourceReference {
                kind: "environment".into(),
                environment_id: plan.target_id.clone(),
                ..Default::default()
            }),
            state: h::OperationState::Running as i32,
            stage: "accepting".into(),
            request_id: req.request_id,
            started_at: now,
            updated_at: now,
            ..Default::default()
        };
        db.execute("INSERT INTO local_disposals(operation_id,request,kind,target,plan,operation) VALUES(?,?,?,?,?,?)",
            params![op.id,op.request_id,plan.target_kind,plan.target_id,current.encode_to_vec(),op.encode_to_vec()]).map_err(db_error)?;
        self.accept(&db, &current, op)
    }

    fn accept(
        &self,
        db: &Connection,
        plan: &h::LocalDisposalPlan,
        mut op: h::Operation,
    ) -> Result<h::Operation, Status> {
        if let Err(error) = self.isolate(plan, &op.request_id) {
            if matches!(
                error.code(),
                vmm_rpc::Code::Aborted | vmm_rpc::Code::NotFound | vmm_rpc::Code::FailedPrecondition
            ) {
                db.execute(
                    "DELETE FROM local_disposals WHERE operation_id=? AND complete=0",
                    [&op.id],
                )
                .map_err(db_error)?;
            }
            return Err(error);
        }
        op.stage = "isolated".into();
        op.updated_at = Some(SystemTime::now().into());
        db.execute(
            "UPDATE local_disposals SET operation=? WHERE operation_id=?",
            params![op.encode_to_vec(), op.id],
        )
        .map_err(db_error)?;
        Ok(op)
    }

    fn receipt(db: &Connection, id: &str) -> Result<Receipt, Status> {
        let row = db
            .query_row(
                "SELECT plan,operation FROM local_disposals WHERE operation_id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| Status::not_found("operation not found"))?;
        Receipt::decode(row)
    }
    pub fn operation(&self, id: &str) -> Result<h::Operation, Status> {
        let db = self.db()?;
        Ok(Self::receipt(&db, id)?.operation)
    }
    pub fn pending(&self, after: &str) -> Result<Vec<String>, Status> {
        let db = self.db()?;
        let mut stmt=db.prepare_cached("SELECT operation_id FROM local_disposals WHERE complete=0 AND operation_id>? ORDER BY operation_id LIMIT 32").map_err(db_error)?;
        stmt.query_map([after], |r| r.get(0))
            .map_err(db_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(db_error)
    }

    pub async fn reconcile(&self, core: &Core, id: &str) -> Result<(), Status> {
        if !Arc::ptr_eq(&core.store, &self.store) {
            return Err(Status::internal("disposal backing store mismatch"));
        }
        let (plan, mut operation) = {
            let db = self.db()?;
            let Receipt {
                plan,
                mut operation,
            } = Self::receipt(&db, id)?;
            if operation.state() == h::OperationState::Succeeded {
                return Ok(());
            }
            if operation.stage == "accepting" {
                operation = self.accept(&db, &plan, operation)?;
            }
            (plan, operation)
        };
        let result = match plan.target_kind() {
            Kind::Registration => match self.registrations.delete_credential(&plan.target_id) {
                Ok(()) => core
                    .dispose_operator_registration(&plan.target_id, &operation.request_id)
                    .await
                    .map_err(disposal_status),
                Err(error) => Err(error),
            },
            Kind::Environment => core
                .dispose_operator_environment(&plan.target_id, plan.revision, &operation.request_id)
                .await
                .map_err(disposal_status),
            Kind::Unspecified => {
                return Err(Status::data_loss("invalid persisted disposal target"));
            }
        };
        let complete = result.is_ok();
        if complete {
            operation.stage = "complete".into();
            operation.state = h::OperationState::Succeeded as i32;
            operation.error = None;
        } else {
            operation.error=Some(h::OperationError{reason:"LOCAL_DISPOSAL_PENDING".into(),message:"The target is isolated. Local deletion must continue when its runtime is available.".into(),resource:operation.resource.clone()});
        }
        operation.updated_at = Some(SystemTime::now().into());
        self.db()?.execute("UPDATE local_disposals SET operation=?,complete=? WHERE operation_id=? AND complete=0",params![operation.encode_to_vec(),complete,id]).map_err(db_error)?;
        result
    }
}

fn disposal_status(e: vmm_core::Error) -> Status {
    match e {
        vmm_core::Error::Fenced
        | vmm_core::Error::Conflict(
            "scope disposal already accepted" | "registration disposal already accepted",
        ) => Status::aborted("local deletion scope changed"),
        other => runtime_status(other),
    }
}
