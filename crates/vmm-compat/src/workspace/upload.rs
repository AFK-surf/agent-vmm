use super::*;
use crate::error::detailed_status;
use std::io::{self, Cursor, Seek};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_stream::{Stream, StreamExt};
use vmm_rpc::Code;

const CHUNK: usize = g::MAX_CHUNK_BYTES;

fn quota() -> Status {
    detailed_status(
        Code::ResourceExhausted,
        "QUOTA_EXCEEDED",
        "environment disk quota exceeded",
        Default::default(),
    )
}
fn admit(ctx: &RuntimeContext, additional: u64) -> Result<(), Status> {
    let used = ctx
        .core
        .store
        .scope_used_bytes(&ctx.scope.id)
        .map_err(core_status)?;
    let scope = ctx.core.store.scope(&ctx.scope.id).map_err(core_status)?;
    if used
        .checked_add(additional)
        .is_none_or(|n| n > scope.limits.disk_quota_bytes)
    {
        return Err(quota());
    }
    Ok(())
}

fn sample(ctx: &RuntimeContext, id: &str, lease: &dyn WorkspaceLease) -> Result<(), Status> {
    ctx.core
        .store
        .record_volume_usage(&ctx.scope.id, id, lease.used_bytes()?)
        .map_err(core_status)
}

fn settle(
    ctx: &RuntimeContext,
    id: &str,
    lease: &dyn WorkspaceLease,
    observed: Option<u64>,
) -> Result<(), Status> {
    match observed {
        Some(bytes) => ctx
            .core
            .store
            .record_volume_usage(&ctx.scope.id, id, bytes)
            .map_err(core_status),
        None => sample(ctx, id, lease),
    }
}

enum Chunk {
    Data(Vec<u8>),
    End,
}
struct Source {
    rx: mpsc::Receiver<Chunk>,
    current: Cursor<Vec<u8>>,
    ended: bool,
    owner: RuntimeContext,
    total: u64,
    denied: Option<Status>,
}
impl Read for Source {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        while !self.ended && !dst.is_empty() {
            let n = self.current.read(dst)?;
            if n > 0 {
                return Ok(n);
            }
            match self.rx.blocking_recv() {
                Some(Chunk::Data(data)) => {
                    let check = self
                        .total
                        .checked_add(data.len() as u64)
                        .ok_or_else(quota)
                        .and_then(|total| {
                            admit(&self.owner, total)?;
                            self.total = total;
                            Ok(())
                        });
                    if let Err(status) = check {
                        self.denied = Some(status);
                        return Err(io::Error::other("workspace admission denied"));
                    }
                    self.current = Cursor::new(data);
                }
                Some(Chunk::End) => self.ended = true,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "workspace upload interrupted",
                    ));
                }
            }
        }
        Ok(0)
    }
}

fn check_id(id: &str) -> Result<(), Status> {
    if id.is_empty()
        || id.len() > g::MAX_REQUEST_ID_BYTES
        || !id.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(Status::invalid_argument("invalid workspace request ID"));
    }
    Ok(())
}

impl WorkspaceService {
    pub async fn write<S>(
        &self,
        ctx: &RuntimeContext,
        mut stream: S,
    ) -> Result<h::WriteWorkspaceResponse, Status>
    where
        S: Stream<Item = Result<h::WriteWorkspaceRequest, Status>> + Unpin + Send,
    {
        use h::write_workspace_request::Payload;
        let Some(Payload::Header(header)) =
            stream.next().await.transpose()?.and_then(|r| r.payload)
        else {
            return Err(Status::invalid_argument(
                "workspace write header must be first",
            ));
        };
        let target = target(&header.volume_id, header.path)?;
        check_id(&header.request_id)?;
        if target.path == "." || header.mode & !0o777 != 0 {
            return Err(Status::invalid_argument("invalid workspace file or mode"));
        }
        let prefix = match (header.offset, header.expected_prefix_sha256.as_slice()) {
            (0, []) => None,
            (_, hash) => Some(
                hash.try_into()
                    .map_err(|_| Status::invalid_argument("expected a 32-byte prefix digest"))?,
            ),
        };
        let id = target
            .volume
            .clone()
            .unwrap_or_else(|| DEFAULT_VOLUME.into());
        let req = g::WorkspaceWriteRequest {
            request_id: header.request_id,
            target: target.clone(),
            offset: header.offset,
            expected_prefix_sha256: prefix,
            mode: header.mode,
        };
        let lock = self.writer(&ctx.scope.id)?.lock_owned().await;
        let lease = self.resolve(ctx, &target, AccessMode::ReadWrite).await?;
        let owner = ctx.clone();
        let (tx, rx) = mpsc::channel(2);
        let task = tokio::task::spawn_blocking(move || {
            let _lock = lock;
            sample(&owner, &id, lease.as_ref())?;
            admit(&owner, 0)?;
            let mut source = Source {
                rx,
                current: Cursor::default(),
                ended: false,
                owner: owner.clone(),
                total: 0,
                denied: None,
            };
            let result = lease.write(req, &mut source);
            settle(
                &owner,
                &id,
                lease.as_ref(),
                result.as_ref().ok().and_then(|reply| reply.root_used_bytes),
            )?;
            if let Some(status) = source.denied {
                return Err(status);
            }
            let result = result?;
            if !source.ended {
                return Err(Status::data_loss("guest completed an unfinished write"));
            }
            Ok(h::WriteWorkspaceResponse {
                entry: Some(project(result.entry)),
                sha256: result.sha256.to_vec(),
            })
        });
        let transfer = async {
            while let Some(message) = stream.next().await.transpose()? {
                match message.payload {
                    Some(Payload::Data(data)) if data.len() <= CHUNK => {
                        if tx.send(Chunk::Data(data)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(Payload::Finish(true)) => {
                        let _ = tx.send(Chunk::End).await;
                        return Ok(());
                    }
                    _ => return Err(Status::invalid_argument("invalid workspace write frame")),
                }
            }
            Err(Status::invalid_argument("workspace write did not finish"))
        };
        tokio::pin!(task);
        tokio::select! {
            result=&mut task=>result.map_err(|_|Status::internal("workspace write task interrupted"))?,
            result=transfer=>{
                drop(tx);
                let written=task.await.map_err(|_|Status::internal("workspace write task interrupted"))?;
                result?;
                written
            }
        }
    }

    pub async fn extract<S>(
        &self,
        ctx: &RuntimeContext,
        mut stream: S,
    ) -> Result<h::ExtractWorkspaceResponse, Status>
    where
        S: Stream<Item = Result<h::ExtractWorkspaceRequest, Status>> + Unpin + Send,
    {
        use h::extract_workspace_request::Payload;
        let Some(Payload::Header(header)) =
            stream.next().await.transpose()?.and_then(|r| r.payload)
        else {
            return Err(Status::invalid_argument(
                "workspace extract header must be first",
            ));
        };
        check_id(&header.request_id)?;
        let target = target("", header.path)?;
        if target.path == "." {
            return Err(Status::invalid_argument(
                "destination must be a new directory",
            ));
        }
        let lock = self.writer(&ctx.scope.id)?.lock_owned().await;
        let lease = self.resolve(ctx, &target, AccessMode::ReadWrite).await?;
        let tmp = tempfile::tempfile().map_err(host_io)?;
        let mut tmp = tokio::fs::File::from_std(tmp);
        let mut total = 0;
        loop {
            match stream.next().await.transpose()?.and_then(|r| r.payload) {
                Some(Payload::Data(data)) if data.len() <= CHUNK => {
                    total += data.len() as u64;
                    if total > MAX_BYTES {
                        return Err(Status::resource_exhausted("archive exceeds size limit"));
                    }
                    tmp.write_all(&data).await.map_err(host_io)?;
                }
                Some(Payload::Finish(true)) => break,
                _ => {
                    return Err(Status::invalid_argument(
                        "invalid or incomplete workspace archive",
                    ));
                }
            }
        }
        tmp.flush().await.map_err(host_io)?;
        let mut tmp = tmp.into_std().await;
        let owner = ctx.clone();
        blocking(move || {
            let _lock = lock;
            tmp.rewind().map_err(host_io)?;
            let required = archive_size(&mut tmp)?;
            tmp.rewind().map_err(host_io)?;
            sample(&owner, DEFAULT_VOLUME, lease.as_ref())?;
            admit(&owner, required)?;
            let result = lease.import_archive(
                g::WorkspaceImportRequest {
                    request_id: header.request_id,
                    target,
                },
                &mut tmp,
            );
            settle(
                &owner,
                DEFAULT_VOLUME,
                lease.as_ref(),
                result.as_ref().ok().and_then(|reply| reply.root_used_bytes),
            )?;
            let result = result?;
            Ok(h::ExtractWorkspaceResponse {
                entry: Some(project(result.entry)),
                sha256: result.sha256.to_vec(),
            })
        })
        .await
    }
}

fn archive_size(input: &mut std::fs::File) -> Result<u64, Status> {
    let invalid = |_| Status::invalid_argument("invalid workspace tar archive");
    let mut archive = tar::Archive::new(input);
    let mut total = 0u64;
    for (index, entry) in archive.entries().map_err(invalid)?.enumerate() {
        let entry = entry.map_err(invalid)?;
        let kind = entry.header().entry_type();
        if index >= 1000 || !(kind.is_file() || kind.is_dir()) {
            return Err(Status::invalid_argument(
                "unsupported workspace archive entry",
            ));
        }
        total = total.checked_add(entry.size()).ok_or_else(quota)?;
        if total > MAX_BYTES {
            return Err(Status::resource_exhausted("archive contents exceed limit"));
        }
    }
    Ok(total)
}

fn host_io(err: io::Error) -> Status {
    match err.kind() {
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded => {
            Status::resource_exhausted("workspace staging storage is full")
        }
        _ => Status::internal("workspace staging I/O failed"),
    }
}
