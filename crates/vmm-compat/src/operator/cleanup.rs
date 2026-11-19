use super::{LocalOperator, h};
use crate::error::{core_status, host_status, invalid_field, with_resource};
use vmm_rpc::{Code, Status};
use vmm_core::{CleanupKind, CleanupOperation, Error};

fn status(err: Error, id: &str) -> Status {
    let (code, reason) = match err {
        Error::Conflict("controller-owned allocation") => (
            Code::FailedPrecondition,
            h::ErrorReason::RemoteAllocationControllerOwned,
        ),
        Error::Conflict("remote allocation active") => (
            Code::FailedPrecondition,
            h::ErrorReason::RemoteAllocationActive,
        ),
        Error::Conflict("cleanup inventory limit") => (
            Code::ResourceExhausted,
            h::ErrorReason::CleanupInventoryLimit,
        ),
        Error::Conflict("volume confirmation required") => (
            Code::FailedPrecondition,
            h::ErrorReason::VolumeConfirmationRequired,
        ),
        Error::Fenced => (Code::Aborted, h::ErrorReason::CleanupPlanStale),
        _ => return core_status(err),
    };
    with_resource(
        host_status(code, reason, &err.to_string()),
        "environment",
        id,
    )
}

fn kind(k: CleanupKind) -> h::CleanupKind {
    match k {
        CleanupKind::Reset => h::CleanupKind::Reset,
        CleanupKind::Discard => h::CleanupKind::Discard,
    }
}
fn resource(scope: &str, kind: &str, id: &str, generation: String) -> h::ResourceReference {
    h::ResourceReference {
        environment_id: scope.into(),
        kind: kind.into(),
        resource_id: id.into(),
        generation_id: generation,
        ..Default::default()
    }
}
pub(super) fn operation(op: CleanupOperation) -> h::Operation {
    h::Operation {
        id: op.id,
        request_id: op.request,
        kind: format!("environment-{}", kind(op.kind).as_str_name()),
        resource: Some(resource(&op.scope, "environment", &op.scope, String::new())),
        started_at: Some(prost_types::Timestamp {
            seconds: op.started_at,
            nanos: 0,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: op.updated_at,
            nanos: 0,
        }),
        stage: if op.complete { "complete" } else { "accepted" }.into(),
        state: if op.complete {
            h::OperationState::Succeeded
        } else {
            h::OperationState::Running
        } as i32,
        ..Default::default()
    }
}
impl LocalOperator {
    fn cleanup_plan(&self, id: &str, k: CleanupKind) -> Result<h::CleanupPlan, Status> {
        let inv = self
            .inventory
            .store
            .cleanup_inventory(id, k)
            .map_err(|e| status(e, id))?;
        cleanup_plan(id, k, inv)
    }
    pub(super) async fn plan_reset(
        &self,
        r: h::PlanResetEnvironmentRequest,
    ) -> Result<h::PlanResetEnvironmentResponse, Status> {
        Ok(h::PlanResetEnvironmentResponse {
            plan: Some(self.cleanup_plan(&r.environment_id, CleanupKind::Reset)?),
        })
    }
    pub(super) async fn plan_discard(
        &self,
        r: h::PlanDiscardEnvironmentRequest,
    ) -> Result<h::PlanDiscardEnvironmentResponse, Status> {
        Ok(h::PlanDiscardEnvironmentResponse {
            plan: Some(self.cleanup_plan(&r.environment_id, CleanupKind::Discard)?),
        })
    }
    pub(super) async fn execute_cleanup(
        &self,
        r: h::ExecuteCleanupRequest,
    ) -> Result<h::ExecuteCleanupResponse, Status> {
        let plan = r
            .plan
            .ok_or_else(|| invalid_field("plan", "a reviewed cleanup plan is required"))?;
        if r.request_id.is_empty() || r.request_id.len() > 128 || plan.revision == 0 {
            return Err(invalid_field(
                "request_id/plan",
                "request ID and revision-bound plan are required",
            ));
        }
        let k = match plan.kind() {
            h::CleanupKind::Discard => CleanupKind::Discard,
            h::CleanupKind::Reset => CleanupKind::Reset,
            _ => return Err(invalid_field("plan.kind", "select reset or discard")),
        };
        let store = &self.inventory.store;
        let prior = store.cleanup_request(&r.request_id).map_err(core_status)?;
        if let Some(op) = prior {
            if op.scope != plan.environment_id || op.kind != k {
                return Err(core_status(Error::Conflict("request fingerprint")));
            }
            return Ok(h::ExecuteCleanupResponse {
                operation: Some(operation(op)),
            });
        }
        let backend = self
            .backend
            .clone()
            .ok_or_else(|| Status::unavailable("cleanup requires the runtime backend"))?;
        let op = store
            .accept_cleanup(
                &plan.environment_id,
                k,
                plan.revision,
                &r.confirmed_volume_ids,
                &r.request_id,
            )
            .await
            .map_err(|e| status(e, &plan.environment_id))?;
        self.wake.notify_one();
        let core = vmm_core::Core::new(store.clone(), backend);
        // The worker can resume this durable intent if the RPC is cancelled.
        let op = match tokio::time::timeout(
            std::time::Duration::from_secs(120),
            core.drive_cleanup(&op.id),
        )
        .await
        {
            Ok(Ok(done)) => done,
            // GetOperation exposes the original receipt while backend reconciliation continues.
            Ok(Err(_)) | Err(_) => op,
        };
        Ok(h::ExecuteCleanupResponse {
            operation: Some(operation(op)),
        })
    }
}

pub(crate) fn cleanup_plan(
    id: &str,
    k: CleanupKind,
    inv: vmm_core::CleanupInventory,
) -> Result<h::CleanupPlan, Status> {
    let discard = k == CleanupKind::Discard;
    let mut impacts = Vec::new();
    for item in inv.resources {
        let (effect, irreversible) = match item.kind.as_str() {
            "grant" => ("access will be revoked", false),
            "container" => ("writable snapshot and logs will be deleted", true),
            "volume" if discard => ("named volume and all contents will be deleted", true),
            "volume" => ("named volume will be retained", false),
            "image" => (
                "namespace-scoped image record will be deleted; shared content is retained while referenced",
                true,
            ),
            _ => return Err(Status::data_loss("invalid cleanup inventory")),
        };
        impacts.push(h::CleanupImpact {
            resource: Some(resource(id, &item.kind, &item.id, item.generation)),
            effect: effect.into(),
            irreversible,
            blocker: false,
        });
    }
    if discard {
        impacts.push(h::CleanupImpact {
            resource: Some(resource(id, "environment", id, String::new())),
            effect: "namespace and host policy will be deleted".into(),
            irreversible: true,
            blocker: false,
        });
    }
    Ok(h::CleanupPlan {
        environment_id: id.into(),
        kind: kind(k) as i32,
        revision: inv.revision,
        impacts,
    })
}
