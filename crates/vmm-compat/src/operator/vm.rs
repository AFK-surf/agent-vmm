//! Whole-VM operations have a single admission owner and survive a client disconnect.
use super::{BOOT_ID, LocalOperator, h};
use crate::error::{core_status, host_status, invalid_field};
use async_trait::async_trait;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tonic::{Code, Status};
use vmm_core::Core;

#[async_trait]
pub trait VmControl: Send + Sync {
    async fn observe(&self) -> Result<h::Vm, Status>;
    async fn change(&self, start: bool, force: bool, operation_id: &str) -> Result<(), Status>;
}

pub struct NativeVmControl(pub Core);
#[async_trait]
impl VmControl for NativeVmControl {
    async fn observe(&self) -> Result<h::Vm, Status> {
        let observed = self.0.store.host_vm_observation().map_err(core_status)?;
        let state = match observed.state.as_str() {
            "stopped" => h::VmState::Stopped,
            "stopping" => h::VmState::Stopping,
            "running" => h::VmState::Running,
            _ => return Err(Status::data_loss("invalid local VM admission state")),
        };
        Ok(h::Vm {
            state: state as i32,
            boot_id: observed.boot_id,
            ..Default::default()
        })
    }
    async fn change(&self, start: bool, force: bool, _: &str) -> Result<(), Status> {
        if start {
            self.0.start_local_host().await
        } else {
            self.0.stop_local_host(force).await
        }
        .map_err(core_status)
    }
}

pub struct VmOperations {
    db: Mutex<Connection>,
    admission: tokio::sync::Mutex<()>,
}
fn unavailable<E>(_: E) -> Status {
    Status::unavailable("VM operation store unavailable")
}
impl VmOperations {
    pub(super) fn open(path: &Path) -> Result<Self, Status> {
        let db = Connection::open(path).map_err(unavailable)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS vm_operations(id TEXT PRIMARY KEY,request TEXT NOT NULL UNIQUE,operation BLOB NOT NULL)").map_err(unavailable)?;
        Ok(Self {
            db: Mutex::new(db),
            admission: Default::default(),
        })
    }
    fn lookup(&self, by_request: bool, id: &str) -> Result<Option<h::Operation>, Status> {
        let sql = if by_request {
            "SELECT operation FROM vm_operations WHERE request=?"
        } else {
            "SELECT operation FROM vm_operations WHERE id=?"
        };
        self.db
            .lock()
            .map_err(unavailable)?
            .query_row(sql, [id], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(unavailable)?
            .map(|wire| {
                h::Operation::decode(wire.as_slice())
                    .map_err(|_| Status::data_loss("invalid VM operation"))
            })
            .transpose()
    }
    fn save(&self, op: &h::Operation) -> Result<(), Status> {
        self.db.lock().map_err(unavailable)?.execute("INSERT INTO vm_operations VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET operation=excluded.operation",params![op.id,op.request_id,op.encode_to_vec()]).map_err(|_|crate::error::with_metadata(Code::Unavailable,h::ErrorReason::VmTerminalPersistFailed,"VM operation persistence failed",[("operation_id".into(),op.id.clone()),("request_id".into(),op.request_id.clone())].into()))?;
        Ok(())
    }
    pub(super) fn operation(&self, id: &str) -> Result<Option<h::Operation>, Status> {
        let Some(op) = self.lookup(false, id)? else {
            return Ok(None);
        };
        if op.state() == h::OperationState::Running
            && let Ok(_guard) = self.admission.try_lock()
            && let Some(current) = self.lookup(false, id)?
        {
            return self.recover(current).map(Some);
        }
        Ok(Some(op))
    }
    fn recover(&self, mut op: h::Operation) -> Result<h::Operation, Status> {
        if op.state() == h::OperationState::Running {
            op.state = h::OperationState::Interrupted as i32;
            op.stage = "interrupted".into();
            op.updated_at = Some(SystemTime::now().into());
            op.error = Some(h::OperationError {
                reason: "VM_TERMINAL_RESULT_UNKNOWN".into(),
                message: "VM operation interrupted".into(),
                resource: op.resource.clone(),
            });
            self.save(&op)?;
        }
        Ok(op)
    }
    async fn mutate(
        &self,
        control: Option<Arc<dyn VmControl>>,
        start: bool,
        force: bool,
        request: String,
        fence: Option<h::VmMutationFence>,
    ) -> Result<h::Operation, Status> {
        if request.is_empty() {
            return Err(invalid_field("request_id", "request ID is required"));
        }
        let _guard = self.admission.lock().await;
        let kind = if start { "vm-start" } else { "vm-stop" };
        if let Some(op) = self.lookup(true, &request)? {
            if op.kind != kind {
                return Err(host_status(
                    Code::Aborted,
                    h::ErrorReason::RequestIdConflict,
                    "request ID belongs to another operation",
                ));
            }
            return self.recover(op);
        }
        let control =
            control.ok_or_else(|| Status::failed_precondition("VM backend is not configured"))?;
        if let Some(f) = fence {
            let observed = tokio::time::timeout(Duration::from_secs(4), control.observe())
                .await
                .map_err(|_| Status::unavailable("VM observation timed out"))??;
            if !f.whole_vm_impact_acknowledged
                || f.state != observed.state
                || f.boot_id != observed.boot_id
            {
                return Err(Status::aborted(
                    "VM impact acknowledgement, state or boot identity changed",
                ));
            }
        }
        let now = Some(SystemTime::now().into());
        let mut op = h::Operation {
            id: uuid::Uuid::new_v4().to_string(),
            kind: kind.into(),
            resource: Some(h::ResourceReference {
                kind: "vm".into(),
                resource_id: "local".into(),
                ..Default::default()
            }),
            state: h::OperationState::Running as i32,
            stage: "accepted".into(),
            request_id: request,
            started_at: now,
            updated_at: now,
            ..Default::default()
        };
        self.save(&op)?;
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            control.change(start, force, &op.id),
        )
        .await;
        op.updated_at = Some(SystemTime::now().into());
        match result {
            Ok(Ok(())) => {
                op.state = h::OperationState::Succeeded as i32;
                op.stage = if start { "running" } else { "stopped" }.into();
                self.save(&op)?;
                Ok(op)
            }
            failure => {
                let (state, reason, message) = match failure {
                    Ok(Err(error)) => (
                        h::OperationState::Failed,
                        if start {
                            "VM_START_FAILED"
                        } else {
                            "VM_STOP_FAILED"
                        },
                        error.message().to_owned(),
                    ),
                    _ => (
                        h::OperationState::Interrupted,
                        "VM_TERMINAL_RESULT_UNKNOWN",
                        "VM operation timed out".into(),
                    ),
                };
                op.state = state as i32;
                op.stage = if state == h::OperationState::Failed {
                    "failed"
                } else {
                    "interrupted"
                }
                .into();
                op.error = Some(h::OperationError {
                    reason: reason.into(),
                    message: message.clone(),
                    resource: op.resource.clone(),
                });
                self.save(&op)?;
                Err(Status::failed_precondition(message))
            }
        }
    }
}
impl LocalOperator {
    pub(super) async fn snapshot(&self) -> Result<h::Vm, Status> {
        match &self.vm_control {
            Some(control) => tokio::time::timeout(Duration::from_secs(4), control.observe())
                .await
                .map_err(|_| Status::unavailable("VM observation timed out"))?,
            None => Ok(h::Vm {
                state: h::VmState::Stopped as i32,
                boot_id: BOOT_ID.clone(),
                ..Default::default()
            }),
        }
    }
    async fn mutate_vm(
        &self,
        start: bool,
        force: bool,
        request: String,
        fence: Option<h::VmMutationFence>,
    ) -> Result<h::Operation, Status> {
        let owner = self.vm_ops.clone();
        let control = self.vm_control.clone();
        tokio::spawn(async move { owner.mutate(control, start, force, request, fence).await })
            .await
            .map_err(|_| Status::unavailable("VM operation interrupted"))?
    }
    pub(super) async fn start_vm(
        &self,
        req: h::StartVmRequest,
    ) -> Result<h::StartVmResponse, Status> {
        Ok(h::StartVmResponse {
            operation: Some(
                self.mutate_vm(true, false, req.request_id, req.fence)
                    .await?,
            ),
        })
    }
    pub(super) async fn stop_vm(&self, req: h::StopVmRequest) -> Result<h::StopVmResponse, Status> {
        Ok(h::StopVmResponse {
            operation: Some(
                self.mutate_vm(false, req.force, req.request_id, req.fence)
                    .await?,
            ),
        })
    }
}
