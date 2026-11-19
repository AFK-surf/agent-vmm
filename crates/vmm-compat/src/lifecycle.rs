use crate::{
    Containers, RuntimeContext,
    container::{current, db_error},
    context::core_status,
    error::host_status,
    execution::{count, fenced, gate, live, now, pending},
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use rusqlite::{TransactionBehavior, params};
use std::time::Duration;
use vmm_rpc::{Code, Status};
use vmm_core::{BootIdentity, SandboxState};

const STOP_BUDGET_NS: i64 = 30_000_000_000;

impl Containers {
    /// A current pressure observation grants a short-lived reclaim candidate to this exact boot.
    pub fn grant_reclaim_candidate(
        &self,
        ctx: &RuntimeContext,
        container: &str,
        instance: &str,
        until_ns: i64,
    ) -> Result<(), Status> {
        let db = self.db()?;
        live(ctx, container, instance)?;
        let time = now()?;
        gate(&db, instance, time)?;
        db.execute(
            "UPDATE execution_gates SET reclaim_until=? WHERE instance=?",
            params![until_ns.min(time + STOP_BUDGET_NS), instance],
        )
        .map_err(db_error)?;
        Ok(())
    }
    pub fn quiesce(
        &self,
        ctx: &RuntimeContext,
        r: h::QuiesceContainerRequest,
    ) -> Result<h::QuiesceContainerResponse, Status> {
        if r.request_id.is_empty() || r.minimum_idle_seconds > 86_400 {
            return Err(Status::invalid_argument("invalid quiesce command"));
        }
        let mut db = self.db()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let boot = live(ctx, &r.container_id, &r.expected_instance_id)?;
        let instance = &boot.instance_id;
        let time = now()?;
        let g = gate(&tx, instance, time)?;
        if r.cancel {
            if g.stopping || (!g.id.is_empty() && g.id != r.request_id) {
                return Err(Status::aborted("quiesce command no longer owns admission"));
            }
            tx.execute(
                "UPDATE execution_gates SET quiesce='',until_ns=0,reclaim_until=0 WHERE instance=?",
                [instance],
            )
            .map_err(db_error)?;
        } else {
            if count(&tx, instance)? != 0 {
                return Err(host_status(
                    Code::ResourceExhausted,
                    h::ErrorReason::WorkloadExecutionBusy,
                    "workload has unsettled executions",
                ));
            }
            if g.stopping || (!g.id.is_empty() && (g.id != r.request_id || g.until <= time)) {
                return Err(pending(g.until, time));
            }
            let minimum = i64::from(r.minimum_idle_seconds) * 1_000_000_000;
            if g.idle == 0 || time.saturating_sub(g.idle) < minimum {
                return Err(Status::resource_exhausted(
                    "minimum idle period has not elapsed",
                ));
            }
            if minimum > 0 && g.reclaim_until <= time {
                return Err(Status::aborted(
                    "a current pressure reclaim candidate is required",
                ));
            }
            if g.id.is_empty() {
                let until = if minimum > 0 {
                    g.reclaim_until.min(time + STOP_BUDGET_NS)
                } else {
                    time + STOP_BUDGET_NS
                };
                tx.execute(
                    "UPDATE execution_gates SET quiesce=?,until_ns=? WHERE instance=?",
                    params![r.request_id, until, instance],
                )
                .map_err(db_error)?;
            }
        }
        tx.commit().map_err(db_error)?;
        Ok(h::QuiesceContainerResponse {
            container: Some(h::Container {
                id: r.container_id,
                instance_id: boot.instance_id,
                state: h::ContainerState::Running as i32,
                ..Default::default()
            }),
        })
    }
    fn accept_stop(
        &self,
        ctx: &RuntimeContext,
        r: &h::StopContainerRequest,
    ) -> Result<Option<BootIdentity>, Status> {
        let mut db = self.db()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let sb = current(ctx, &r.container_id)?;
        if !r.expected_instance_id.is_empty()
            && sb
                .boot
                .as_ref()
                .is_none_or(|b| b.instance_id != r.expected_instance_id)
        {
            return Err(fenced());
        }
        if !r.quiesce_request_id.is_empty() && r.expected_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "quiesced stop requires expected_instance_id",
            ));
        }
        let Some(boot) = sb.boot else { return Ok(None) };
        let time = now()?;
        let g = gate(&tx, &boot.instance_id, time)?;
        if !r.quiesce_request_id.is_empty() {
            if g.id != r.quiesce_request_id || count(&tx, &boot.instance_id)? != 0 {
                return Err(Status::aborted("safe stop does not own current admission"));
            }
            if g.until <= time {
                return Err(pending(g.until, time));
            }
        }
        tx.execute(
            "UPDATE execution_gates SET stopping=1,until_ns=? WHERE instance=?",
            params![time + STOP_BUDGET_NS, boot.instance_id],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(Some(boot))
    }
    pub async fn stop(
        &self,
        ctx: &RuntimeContext,
        r: h::StopContainerRequest,
    ) -> Result<h::StopContainerResponse, Status> {
        let host = self.clone();
        let owner = ctx.clone();
        mutate(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-stop-container",
            r.request_id.clone(),
            r.encode_to_vec(),
            async move {
                if let Some(boot) = host.accept_stop(&owner, &r)? {
                    // Preserve a guest-reported exit before releasing the boot.
                    let sb = owner
                        .core
                        .store
                        .sandbox(&boot.sandbox)
                        .map_err(core_status)?;
                    if matches!(sb.state, SandboxState::Running | SandboxState::Quiesced) {
                        let _ = host.observe(&owner.core.store, &sb, None).await;
                    }
                    let grace = Duration::from_secs(u64::from(r.timeout_seconds.unwrap_or(30)));
                    if grace.is_zero() {
                        owner.core.stop(&boot, true).await.map_err(core_status)?;
                    } else if let Ok(result) =
                        tokio::time::timeout(grace, owner.core.stop(&boot, false)).await
                    {
                        result.map_err(core_status)?;
                    } else {
                        owner.core.stop(&boot, true).await.map_err(core_status)?;
                    }
                    let mut db = host.db()?;
                    let tx = db.transaction().map_err(db_error)?;
                    tx.execute(
                        "DELETE FROM execution_rights WHERE instance=?",
                        [&boot.instance_id],
                    )
                    .map_err(db_error)?;
                    tx.execute(
                        "DELETE FROM execution_gates WHERE instance=?",
                        [&boot.instance_id],
                    )
                    .map_err(db_error)?;
                    tx.commit().map_err(db_error)?;
                }
                let sb = current(&owner, &r.container_id)?;
                Ok(h::StopContainerResponse {
                    container: Some(host.observe(&owner.core.store, &sb, None).await?),
                })
            },
        )
        .await
    }
    pub async fn wait(
        &self,
        ctx: &RuntimeContext,
        r: h::WaitContainerRequest,
    ) -> Result<h::WaitContainerResponse, Status> {
        let initial = current(ctx, &r.container_id)?;
        let key = &initial.definition.key;
        let boot = initial.boot.as_ref();
        loop {
            let sb = ctx.core.store.sandbox(key).map_err(core_status)?;
            if sb.boot.is_some() && sb.boot.as_ref() != boot {
                return Err(fenced());
            }
            let observed = self
                .observe(&ctx.core.store, &sb, r.expected_configuration.clone())
                .await?;
            if observed.configuration_matches == Some(false) {
                return Err(host_status(
                    Code::Aborted,
                    h::ErrorReason::ContainerGenerationConflict,
                    "container configuration changed",
                ));
            }
            if let Some(code) = observed.exit_code {
                return Ok(h::WaitContainerResponse { exit_code: code });
            }
            if observed.state() != h::ContainerState::Running {
                return Err(host_status(
                    Code::FailedPrecondition,
                    h::ErrorReason::ContainerNotRunning,
                    "no terminal exit observation",
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
