//! A request is admitted durably before receiving the archive. Replay reconciles
//! interrupted publication against the exact cached archive and descriptor.
mod artifact;
mod grant;
mod install;
use crate::{
    error::{host_status, with_metadata, with_resource},
    image::{MAX_ARCHIVE_BYTES, VerifiedArchive},
};
use agent_vmm_proto::{appliance::v1 as a, host::v1 as h};
use oci_spec::distribution::Reference;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime},
};
use tempfile::NamedTempFile;
use tokio_stream::{Stream, StreamExt};
use vmm_rpc::{Code, Status};

pub(super) struct ContentStore {
    core: std::sync::Arc<vmm_core::Store>,
    secrets: std::sync::Arc<dyn vmm_signing::CredentialStore>,
    workload_disk: u64,
    directory: PathBuf,
    db: Mutex<Connection>,
    writer: tokio::sync::Mutex<()>,
}
fn storage<E>(_: E) -> Status {
    Status::unavailable("appliance content storage unavailable")
}
fn validate(r: &a::ApplianceContentReference) -> Result<(), Status> {
    let bad = || Status::invalid_argument("invalid appliance content identity");
    let reference: Reference = r.image_reference.parse().map_err(|_| bad())?;
    if r.encoded_len() > 4096
        || r.archive_sha256.len() != 32
        || !(1..=MAX_ARCHIVE_BYTES).contains(&r.archive_size)
        || r.platform != "linux/arm64"
        || r.role() == a::ApplianceContentRole::Unspecified
        || reference.whole() != r.image_reference
        || reference.digest() != Some(r.image_manifest_digest.as_str())
    {
        return Err(bad());
    }
    let hash = r
        .image_manifest_digest
        .strip_prefix("sha256:")
        .ok_or_else(bad)?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(bad());
    }
    Ok(())
}
fn descriptor(r: &a::ApplianceContentReference) -> a::ApplianceContentReference {
    a::ApplianceContentReference {
        role: a::ApplianceContentRole::Unspecified as i32,
        ..r.clone()
    }
}
impl ContentStore {
    pub(super) fn open(
        db: &Path,
        dir: &Path,
        core: std::sync::Arc<vmm_core::Store>,
        secrets: std::sync::Arc<dyn vmm_signing::CredentialStore>,
        workload_disk: u64,
    ) -> Result<Self, Status> {
        fs::DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(dir)
            .map_err(storage)?;
        let m = fs::symlink_metadata(dir).map_err(storage)?;
        if !m.is_dir() || m.permissions().mode() & 0o077 != 0 {
            return Err(Status::failed_precondition(
                "private appliance cache required",
            ));
        }
        let db = Connection::open(db).map_err(storage)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS appliance_receipts(id TEXT PRIMARY KEY,request TEXT UNIQUE NOT NULL,operation BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS appliance_artifacts(id TEXT PRIMARY KEY,hash BLOB UNIQUE NOT NULL,manifest BLOB NOT NULL)").map_err(storage)?;
        db.execute_batch(install::SCHEMA).map_err(storage)?;
        let store = Self {
            core,
            secrets,
            workload_disk,
            db: Mutex::new(db),
            directory: dir.to_owned(),
            writer: Default::default(),
        };
        store.recover_installations()?;
        Ok(store)
    }
    fn archive_path(&self, r: &a::ApplianceContentReference) -> PathBuf {
        self.directory
            .join(format!("{}.oci.tar", hex::encode(&r.archive_sha256)))
    }
    fn available(&self, r: &a::ApplianceContentReference) -> Result<bool, Status> {
        let path = self.archive_path(r);
        let meta = match fs::symlink_metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            result => result.map_err(storage)?,
        };
        if !meta.is_file() || meta.len() != r.archive_size {
            return Err(Status::failed_precondition(
                "content archive conflicts with its identity",
            ));
        }
        let p = path.with_extension("tar.pb");
        let meta = match fs::symlink_metadata(&p) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            result => result.map_err(storage)?,
        };
        if !meta.is_file() || meta.len() > 4096 {
            return Err(Status::data_loss("invalid cache descriptor"));
        }
        let mut bytes = Vec::new();
        File::open(p)
            .map_err(storage)?
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        if a::ApplianceContentReference::decode(bytes.as_slice()).ok() != Some(descriptor(r)) {
            return Err(Status::failed_precondition(
                "content descriptor conflicts with its identity",
            ));
        }
        Ok(true)
    }
    pub(super) fn status(
        &self,
        r: a::GetContentStatusRequest,
    ) -> Result<a::GetContentStatusResponse, Status> {
        if r.encoded_len() > 32 << 10 {
            return Err(Status::resource_exhausted(
                "content status request too large",
            ));
        }
        let contents = r
            .content_refs
            .into_iter()
            .map(|r| {
                validate(&r)?;
                Ok(a::ApplianceContentStatus {
                    available: self.available(&r)?,
                    content_ref: Some(r),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(a::GetContentStatusResponse { contents })
    }
    fn lookup(&self, key: &str, request: bool) -> Result<Option<a::ApplianceOperation>, Status> {
        let query = if request {
            "SELECT operation FROM appliance_receipts WHERE request=?"
        } else {
            "SELECT operation FROM appliance_receipts WHERE id=?"
        };
        let db = self.db.lock().map_err(storage)?;
        let bytes: Option<Vec<u8>> = db
            .query_row(query, [key], |r| r.get(0))
            .optional()
            .map_err(storage)?;
        bytes
            .map(|b| {
                a::ApplianceOperation::decode(b.as_slice())
                    .map_err(|_| Status::data_loss("invalid appliance receipt"))
            })
            .transpose()
    }
    pub(super) fn watch(&self, id: &str) -> Result<a::WatchApplianceOperationResponse, Status> {
        let operation = self
            .lookup(id, false)?
            .ok_or_else(|| Status::not_found("appliance operation"))?;
        Ok(a::WatchApplianceOperationResponse {
            artifact_id: if operation.kind() == a::ApplianceOperationKind::Stage {
                String::from_utf8(operation.receipt.clone())
                    .map_err(|_| Status::data_loss("invalid artifact receipt"))?
            } else {
                String::new()
            },
            operation: Some(operation),
        })
    }
    // The admission lock must be held: only an abandoned writer can be settled here.
    fn settle_interrupted(
        &self,
        op: a::ApplianceOperation,
    ) -> Result<a::ApplianceOperation, Status> {
        let content = a::StageContentHeader::decode(op.receipt.as_slice())
            .map_err(|_| Status::data_loss("invalid staged content receipt"))?
            .content_ref
            .ok_or_else(|| Status::data_loss("missing staged content identity"))?;
        validate(&content)?;
        let failure = (!self.available(&content)?).then(|| a::ApplianceOperationError {
            reason: "CONTENT_STAGE_INTERRUPTED".into(),
            message: "content stage ended before publication".into(),
        });
        self.settle(op, failure)
    }

    pub(super) async fn terminal(
        &self,
        id: &str,
    ) -> Result<a::WatchApplianceOperationResponse, Status> {
        let _writer = self.writer.lock().await;
        let mut response = self.watch(id)?;
        if let Some(op) = response.operation.as_mut()
            && op.state() == a::ApplianceOperationState::Running
        {
            *op = if op.kind() == a::ApplianceOperationKind::Install {
                self.finish_install(op.clone())?
            } else {
                self.settle_interrupted(op.clone())?
            };
        }
        Ok(response)
    }

    fn settle(
        &self,
        mut op: a::ApplianceOperation,
        failure: Option<a::ApplianceOperationError>,
    ) -> Result<a::ApplianceOperation, Status> {
        op.phase = a::ApplianceOperationPhase::Terminal as i32;
        op.state = if failure.is_none() {
            a::ApplianceOperationState::Succeeded
        } else {
            a::ApplianceOperationState::Failed
        } as i32;
        op.terminal_reason = if failure.is_none() {
            a::ApplianceTerminalReason::Succeeded
        } else {
            a::ApplianceTerminalReason::OperationFailed
        } as i32;
        op.error = failure;
        op.updated_at = Some(SystemTime::now().into());
        self.db
            .lock()
            .map_err(storage)?
            .execute(
                "UPDATE appliance_receipts SET operation=? WHERE id=?",
                params![op.encode_to_vec(), op.operation_id],
            )
            .map_err(storage)?;
        Ok(op)
    }
    pub(super) async fn stage<S>(&self, mut stream: S) -> Result<a::StageContentResponse, Status>
    where
        S: Stream<Item = Result<a::StageContentRequest, Status>> + Unpin + Send,
    {
        use a::stage_content_request::Payload;
        let header = match tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .map_err(|_| Status::deadline_exceeded("content header deadline"))?
            .transpose()?
            .and_then(|r| r.payload)
        {
            Some(Payload::Header(h)) if !h.request_id.is_empty() && h.encoded_len() <= 32 << 10 => {
                h
            }
            _ => return Err(Status::invalid_argument("bounded content header required")),
        };
        let r = header
            .content_ref
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("content reference required"))?;
        validate(r)?;
        let receipt = header.encode_to_vec();
        let prior = |op: &a::ApplianceOperation| {
            if op.receipt != receipt || op.kind() != a::ApplianceOperationKind::StageContent {
                return Err(with_resource(
                    host_status(
                        Code::Aborted,
                        h::ErrorReason::RequestIdConflict,
                        "content request has a different payload",
                    ),
                    "appliance_content",
                    &header.request_id,
                ));
            }
            Ok(a::StageContentResponse {
                operation: Some(op.clone()),
                artifact_id: String::new(),
            })
        };
        if let Some(op) = self.lookup(&header.request_id, true)? {
            let replay = prior(&op)?;
            if op.state() != a::ApplianceOperationState::Running {
                return Ok(replay);
            }
        }
        let remaining = || -> Result<Duration, Status> {
            Ok(Duration::from_millis(
                header
                    .deadline_unix_millis
                    .saturating_sub(crate::execution::now()? / 1_000_000)
                    .max(0) as u64,
            ))
        };
        let wait = remaining()?;
        let _writer = if wait.is_zero() {
            self.writer
                .try_lock()
                .map_err(|_| Status::deadline_exceeded("content admission in progress"))?
        } else {
            tokio::time::timeout(wait, self.writer.lock())
                .await
                .map_err(|_| Status::deadline_exceeded("content admission timed out"))?
        };
        if let Some(mut op) = self.lookup(&header.request_id, true)? {
            prior(&op)?;
            if op.state() == a::ApplianceOperationState::Running {
                op = self.settle_interrupted(op)?;
            }
            return prior(&op);
        }
        let wait = remaining()?;
        if wait.is_zero() {
            return Err(with_resource(
                with_metadata(
                    Code::DeadlineExceeded,
                    h::ErrorReason::ContentStageNotAdmitted,
                    "content stage deadline elapsed",
                    [("admission".into(), "not_accepted".into())].into(),
                ),
                "appliance_content",
                &header.request_id,
            ));
        }
        let now = Some(SystemTime::now().into());
        let op = a::ApplianceOperation {
            operation_id: uuid::Uuid::new_v4().to_string(),
            request_id: header.request_id.clone(),
            started_at: now,
            updated_at: now,
            phase: a::ApplianceOperationPhase::Planned as i32,
            state: a::ApplianceOperationState::Running as i32,
            kind: a::ApplianceOperationKind::StageContent as i32,
            deadline_unix_millis: header.deadline_unix_millis,
            receipt: receipt.clone(),
            ..Default::default()
        };
        self.db
            .lock()
            .map_err(storage)?
            .execute(
                "INSERT INTO appliance_receipts(id,request,operation) VALUES(?,?,?)",
                params![op.operation_id, op.request_id, op.encode_to_vec()],
            )
            .map_err(storage)?;
        let result = tokio::time::timeout(wait, self.receive(&header.request_id, r, stream))
            .await
            .unwrap_or_else(|_| Err(Status::deadline_exceeded("content stage timed out")));
        let failure = result.as_ref().err().map(|e| a::ApplianceOperationError {
            reason: "CONTENT_STAGE_FAILED".into(),
            message: e.message().into(),
        });
        let op = self.settle(op, failure)?;
        result?;
        prior(&op)
    }
    async fn receive<S>(
        &self,
        id: &str,
        r: &a::ApplianceContentReference,
        stream: S,
    ) -> Result<(), Status>
    where
        S: Stream<Item = Result<a::StageContentRequest, Status>> + Unpin + Send,
    {
        if self.available(r)? {
            return Ok(());
        }
        let space = rustix::fs::statvfs(&self.directory).map_err(storage)?;
        if space.f_bavail.saturating_mul(space.f_frsize) < r.archive_size + (2 << 30) {
            return Err(Status::resource_exhausted(
                "insufficient appliance cache headroom",
            ));
        }
        use h::import_image_request::Payload;
        let header = h::ImportImageHeader {
            request_id: id.into(),
            platform: r.platform.clone(),
            image_reference: r.image_reference.clone(),
            manifest_digest: r.image_manifest_digest.clone(),
            archive_size: r.archive_size,
            archive_sha256: r.archive_sha256.clone(),
            ..Default::default()
        };
        let frames = tokio_stream::once(Ok(h::ImportImageRequest {
            payload: Some(Payload::Header(header)),
        }))
        .chain(stream.map(|f| match f?.payload {
            Some(a::stage_content_request::Payload::ArchiveChunk(bytes))
                if !bytes.is_empty() && bytes.len() <= 1 << 20 =>
            {
                Ok(h::ImportImageRequest {
                    payload: Some(Payload::ArchiveChunk(bytes)),
                })
            }
            _ => Err(Status::invalid_argument("invalid content archive chunk")),
        }));
        let file = VerifiedArchive::receive(&self.directory, frames)
            .await?
            .into_file();
        let path = self.archive_path(r);
        let mut meta = NamedTempFile::new_in(&self.directory).map_err(storage)?;
        meta.write_all(&descriptor(r).encode_to_vec())
            .map_err(storage)?;
        for f in [file.as_file(), meta.as_file()] {
            f.set_permissions(fs::Permissions::from_mode(0o444))
                .map_err(storage)?;
            f.sync_all().map_err(storage)?;
        }
        file.persist(&path).map_err(storage)?;
        meta.persist(path.with_extension("tar.pb"))
            .map_err(storage)?;
        File::open(&self.directory)
            .map_err(storage)?
            .sync_all()
            .map_err(storage)
    }
}
