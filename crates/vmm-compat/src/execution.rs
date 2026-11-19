use crate::{
    Containers, RuntimeContext,
    container::{db_error, find},
    context::core_status,
    error::host_status,
};
use agent_vmm_proto::host::v1 as h;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::time::{SystemTime, UNIX_EPOCH};
use vmm_rpc::{Code, Status};
use vmm_core::{BootIdentity, SandboxState};

pub(crate) fn initialize(db: &Connection) -> Result<(), Status> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS terminal_exits (instance TEXT PRIMARY KEY,code INTEGER NOT NULL,ended INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS execution_rights (
        instance TEXT NOT NULL, id TEXT NOT NULL, authority TEXT NOT NULL, owner TEXT NOT NULL,
        kind INTEGER NOT NULL, acquired INTEGER NOT NULL, deadline INTEGER NOT NULL,
        PRIMARY KEY (instance,id));
        CREATE TABLE IF NOT EXISTS execution_gates (
        instance TEXT PRIMARY KEY, quiesce TEXT NOT NULL DEFAULT '', until_ns INTEGER NOT NULL DEFAULT 0,
        stopping INTEGER NOT NULL DEFAULT 0, idle_since INTEGER NOT NULL, reclaim_until INTEGER NOT NULL DEFAULT 0);").map_err(db_error)
}
pub(crate) fn now() -> Result<i64, Status> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|t| i64::try_from(t.as_nanos()).ok())
        .ok_or_else(|| Status::internal("host clock out of range"))
}
pub(crate) fn fenced() -> Status {
    core_status(vmm_core::Error::Fenced)
}
fn stale_right() -> Status {
    host_status(
        Code::Aborted,
        h::ErrorReason::StaleExecution,
        "execution belongs to another owner or instance",
    )
}
pub(crate) fn live(ctx: &RuntimeContext, id: &str, expected: &str) -> Result<BootIdentity, Status> {
    if expected.is_empty() {
        return Err(Status::invalid_argument("expected_instance_id is required"));
    }
    find(ctx, id)?
        .and_then(|s| s.boot)
        .filter(|b| b.instance_id == expected)
        .ok_or_else(fenced)
}
pub(crate) fn pending(until: i64, time: i64) -> Status {
    let reason = if until != 0 && time >= until {
        h::ErrorReason::WorkloadStopUnresolved
    } else {
        h::ErrorReason::LifecycleConflict
    };
    host_status(
        Code::FailedPrecondition,
        reason,
        "workload admission is closed",
    )
}
pub(crate) struct Gate {
    pub id: String,
    pub until: i64,
    pub stopping: bool,
    pub idle: i64,
    pub reclaim_until: i64,
}
pub(crate) fn gate(db: &Connection, instance: &str, time: i64) -> Result<Gate, Status> {
    db.execute(
        "INSERT OR IGNORE INTO execution_gates(instance,idle_since) VALUES(?,?)",
        params![instance, time],
    )
    .map_err(db_error)?;
    db.query_row("SELECT quiesce,until_ns,stopping,idle_since,reclaim_until FROM execution_gates WHERE instance=?",[instance],|r|Ok(Gate{id:r.get(0)?,until:r.get(1)?,stopping:r.get(2)?,idle:r.get(3)?,reclaim_until:r.get(4)?})).map_err(db_error)
}
pub(crate) fn count(db: &Connection, instance: &str) -> Result<i64, Status> {
    db.query_row(
        "SELECT count(*) FROM execution_rights WHERE instance=?",
        [instance],
        |r| r.get(0),
    )
    .map_err(db_error)
}

impl Containers {
    pub fn acquire(
        &self,
        ctx: &RuntimeContext,
        r: h::AcquireExecutionRequest,
    ) -> Result<h::AcquireExecutionResponse, Status> {
        self.acquire_recorded(ctx, r, |_, _| Ok(()))
    }

    pub(crate) fn acquire_recorded(
        &self,
        ctx: &RuntimeContext,
        r: h::AcquireExecutionRequest,
        record: impl FnOnce(&Connection, bool) -> Result<(), Status>,
    ) -> Result<h::AcquireExecutionResponse, Status> {
        if r.execution_id.is_empty()
            || r.execution_id.len() > 256
            || !r.holder_id.is_empty()
            || r.kind() == h::ExecutionKind::Unspecified
        {
            return Err(Status::invalid_argument(
                "invalid execution ID, holder, or kind",
            ));
        }
        let mut db = self.db()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let boot = live(ctx, &r.container_id, &r.expected_instance_id)?;
        let instance = &boot.instance_id;
        let time = now()?;
        let g = gate(&tx, instance, time)?;
        if !g.id.is_empty() || g.stopping {
            return Err(pending(g.until, time));
        }
        if ctx
            .core
            .store
            .sandbox(&boot.sandbox)
            .map_err(core_status)?
            .state
            != SandboxState::Running
        {
            return Err(pending(0, time));
        }
        let authority = ctx.execution_authority()?;
        let owner = ctx.execution_owner();
        let prev: Option<(String, String, i32)> = tx
            .query_row(
                "SELECT authority,owner,kind FROM execution_rights WHERE instance=? AND id=?",
                params![instance, r.execution_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(db_error)?;
        let acquired = prev.is_none();
        if let Some((a, o, k)) = prev {
            if a != authority || o != owner || k != r.kind {
                return Err(stale_right());
            }
        } else {
            let main = r.kind() == h::ExecutionKind::MainExecution;
            if main != (r.deadline_unix_nano == 0) {
                return Err(Status::invalid_argument(
                    "operation deadline required except for main execution",
                ));
            }
            if !main && r.deadline_unix_nano <= time {
                return Err(Status::deadline_exceeded("execution deadline expired"));
            }
            if count(&tx, instance)? >= 64 {
                return Err(Status::resource_exhausted(
                    "workload activity limit reached",
                ));
            }
            tx.execute("INSERT INTO execution_rights(instance,id,authority,owner,kind,acquired,deadline) VALUES(?,?,?,?,?,?,?)",
                params![instance,r.execution_id,authority,owner,r.kind,time,r.deadline_unix_nano]).map_err(db_error)?;
            tx.execute(
                "UPDATE execution_gates SET idle_since=0,reclaim_until=0 WHERE instance=?",
                [instance],
            )
            .map_err(db_error)?;
        }
        record(&tx, acquired)?;
        tx.commit().map_err(db_error)?;
        Ok(h::AcquireExecutionResponse {
            acquired,
            execution_id: r.execution_id,
            container_instance_id: boot.instance_id,
        })
    }

    pub fn release(
        &self,
        ctx: &RuntimeContext,
        r: h::ReleaseExecutionRequest,
    ) -> Result<h::ReleaseExecutionResponse, Status> {
        if r.execution_id.is_empty() || !r.holder_id.is_empty() {
            return Err(Status::invalid_argument("invalid execution identity"));
        }
        let mut db = self.db()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let boot = live(ctx, &r.container_id, &r.expected_instance_id)?;
        let key = params![boot.instance_id, r.execution_id];
        let changed=tx.execute("DELETE FROM execution_rights WHERE instance=? AND id=? AND authority=? AND owner=?",params![boot.instance_id,r.execution_id,ctx.execution_authority()?,ctx.execution_owner()]).map_err(db_error)?;
        if changed == 0 {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM execution_rights WHERE instance=? AND id=?)",
                    key,
                    |row| row.get(0),
                )
                .map_err(db_error)?;
            if exists {
                return Err(stale_right());
            }
        } else if count(&tx, &boot.instance_id)? == 0 {
            tx.execute(
                "UPDATE execution_gates SET idle_since=? WHERE instance=?",
                params![now()?, boot.instance_id],
            )
            .map_err(db_error)?;
        }
        tx.commit().map_err(db_error)?;
        Ok(h::ReleaseExecutionResponse {})
    }

    pub fn executions(
        &self,
        ctx: &RuntimeContext,
        r: h::ListExecutionsRequest,
    ) -> Result<h::ListExecutionsResponse, Status> {
        use h::ExecutionListStatus as S;
        if r.expected_instance_id.is_empty() {
            return Err(Status::invalid_argument("expected_instance_id required"));
        }
        let mut out = h::ListExecutionsResponse {
            allocation_authority: ctx.execution_authority()?.into(),
            status: S::NotFound as i32,
            ..Default::default()
        };
        let Some(sb) = find(ctx, &r.container_id)? else {
            return Ok(out);
        };
        let Some(boot) = sb.boot else {
            if sb.state == SandboxState::Stopped
                && self
                    .facts(&sb.definition.key)?
                    .is_some_and(|f| f.instance == r.expected_instance_id)
            {
                out.status = S::Stopped as i32;
            }
            return Ok(out);
        };
        out.container_instance_id = boot.instance_id;
        let db = self.db()?;
        let time = now()?;
        if out.container_instance_id != r.expected_instance_id {
            out.status = S::Stale as i32;
        } else if !matches!(sb.state, SandboxState::Running | SandboxState::Quiesced)
            || gate(&db, &out.container_instance_id, time)?.stopping
        {
            out.status = S::Recovering as i32;
        } else {
            let mut stmt=db.prepare("SELECT id,owner,kind,acquired,deadline FROM execution_rights WHERE instance=? ORDER BY id").map_err(db_error)?;
            out.executions = stmt
                .query_map([&out.container_instance_id], |row| {
                    let deadline: i64 = row.get(4)?;
                    let state = if deadline != 0 && time >= deadline {
                        h::ExecutionState::Unknown
                    } else {
                        h::ExecutionState::Active
                    };
                    Ok(h::Execution {
                        execution_id: row.get(0)?,
                        owner: row.get(1)?,
                        kind: row.get(2)?,
                        acquired_unix_nano: row.get(3)?,
                        deadline_unix_nano: deadline,
                        state: state as i32,
                        container_instance_id: out.container_instance_id.clone(),
                    })
                })
                .map_err(db_error)?
                .collect::<rusqlite::Result<_>>()
                .map_err(db_error)?;
            out.status = if out.executions.is_empty() {
                S::Empty
            } else {
                S::Present
            } as i32;
        }
        Ok(out)
    }
}
