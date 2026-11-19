use crate::{Containers, RuntimeContext, container::db_error, execution};
use agent_vmm_proto::host::v1 as h;
use prost::Message;
use rusqlite::{OptionalExtension, params};
use tonic::Status;

pub(super) struct Record {
    pub receipt: h::ImageOperationReceipt,
    pub lease: String,
    pub scope: String,
}

impl Record {
    pub fn new(ctx: &RuntimeContext, header: &h::BuildImageHeader, canonical: String) -> Self {
        Self {
            receipt: h::ImageOperationReceipt {
                request_id: header.request_id.clone(),
                input_sha256: super::input::fingerprint(header),
                kind: h::ImageOperationKind::Build as i32,
                state: h::ImageOperationState::Running as i32,
                container_id: header.container_id.clone(),
                expected_instance_id: header.expected_instance_id.clone(),
                execution_id: header.execution_id.clone(),
                image_reference: canonical,
                ..Default::default()
            },
            lease: uuid::Uuid::new_v4().simple().to_string(),
            scope: ctx.scope.id.clone(),
        }
    }
    pub fn active(&self) -> bool {
        !self.receipt.writer_settled
    }
    pub fn failed(&mut self, error: &Status, settled: bool) {
        let state = match error.code() {
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => {
                h::ImageOperationState::Stopped
            }
            _ if !settled => h::ImageOperationState::Unknown,
            _ => h::ImageOperationState::Failed,
        };
        self.receipt.state = state as i32;
        self.receipt.status = Some(
            agent_vmm_proto::google::rpc::Status::decode(error.details())
                .ok()
                .filter(|s| s.code == error.code() as i32)
                .unwrap_or_else(|| agent_vmm_proto::google::rpc::Status {
                    code: error.code() as i32,
                    message: error.message().into(),
                    details: vec![],
                }),
        );
        self.receipt.writer_settled = settled;
    }
}

pub(super) fn initialize(c: &Containers) -> Result<(), Status> {
    c.db()?.execute_batch("CREATE TABLE IF NOT EXISTS build_operations(scope TEXT NOT NULL,id TEXT NOT NULL,
        lease TEXT NOT NULL UNIQUE, authority TEXT NOT NULL,owner TEXT NOT NULL,receipt BLOB NOT NULL,
        settled INTEGER NOT NULL, PRIMARY KEY(scope,id));").map_err(db_error)
}

fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Record> {
    let bytes: Vec<u8> = r.get(2)?;
    let receipt = h::ImageOperationReceipt::decode(bytes.as_slice()).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Blob, Box::new(e))
    })?;
    Ok(Record {
        scope: r.get(0)?,
        lease: r.get(1)?,
        receipt,
    })
}
pub(super) fn lookup(c: &Containers, scope: &str, id: &str) -> Result<Option<Record>, Status> {
    c.db()?
        .query_row(
            "SELECT scope,lease,receipt FROM build_operations WHERE scope=? AND id=?",
            [scope, id],
            row,
        )
        .optional()
        .map_err(db_error)
}

pub(super) fn pending(c: &Containers) -> Result<Vec<Record>, Status> {
    c.db()?
        .prepare("SELECT scope,lease,receipt FROM build_operations WHERE settled=0")
        .map_err(db_error)?
        .query_map([], row)
        .map_err(db_error)?
        .collect::<rusqlite::Result<_>>()
        .map_err(db_error)
}

pub(super) fn admit(
    c: &Containers,
    ctx: &RuntimeContext,
    r: &Record,
    deadline: i64,
) -> Result<(), Status> {
    let receipt = &r.receipt;
    c.acquire_recorded(ctx,h::AcquireExecutionRequest{execution_id:receipt.execution_id.clone(),container_id:receipt.container_id.clone(),
        expected_instance_id:receipt.expected_instance_id.clone(),deadline_unix_nano:deadline,kind:h::ExecutionKind::Build as i32,..Default::default()},
        |db,acquired|{
            if !acquired{return Err(Status::aborted("execution already accepted"))}
            db.execute("INSERT INTO build_operations(scope,id,lease,authority,owner,receipt,settled) VALUES(?,?,?,?,?,?,0)",
                params![r.scope,receipt.request_id,r.lease,ctx.execution_authority()?,ctx.execution_owner(),receipt.encode_to_vec()]).map_err(db_error)?;
            Ok(())
        })?;
    Ok(())
}

pub(super) fn save(c: &Containers, r: &Record) -> Result<(), Status> {
    let receipt = &r.receipt;
    let mut db = c.db()?;
    let tx = db.transaction().map_err(db_error)?;
    let n = tx
        .execute(
            "UPDATE build_operations SET receipt=?,settled=? WHERE lease=? AND settled=0",
            params![receipt.encode_to_vec(), receipt.writer_settled, r.lease],
        )
        .map_err(db_error)?;
    if n == 1 && receipt.writer_settled {
        tx.execute("DELETE FROM execution_rights WHERE instance=? AND id=? AND kind=? AND (authority,owner)=(SELECT authority,owner FROM build_operations WHERE lease=?)",
            params![receipt.expected_instance_id,receipt.execution_id,h::ExecutionKind::Build as i32,r.lease]).map_err(db_error)?;
        if execution::count(&tx, &receipt.expected_instance_id)? == 0 {
            tx.execute(
                "UPDATE execution_gates SET idle_since=? WHERE instance=?",
                params![execution::now()?, receipt.expected_instance_id],
            )
            .map_err(db_error)?;
        }
    }
    tx.commit().map_err(db_error)
}
