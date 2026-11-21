use super::*;
use sha2::{Digest, Sha256};
use std::io;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub type WorkspaceStream<T> = ReceiverStream<Result<T, Status>>;

struct Download<T> {
    tx: mpsc::Sender<Result<T, Status>>,
    frame: fn(Vec<u8>, u64) -> T,
    offset: u64,
    remaining: u64,
    digest: Sha256,
}

impl<T> Write for Download<T> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let len = data.len().min(g::MAX_CHUNK_BYTES);
        let bytes = &data[..len];
        if len as u64 > self.remaining {
            return Err(io::Error::other("workspace response exceeds limit"));
        }
        let end = self
            .offset
            .checked_add(len as u64)
            .ok_or_else(|| io::Error::other("workspace offset overflow"))?;
        self.tx
            .blocking_send(Ok((self.frame)(bytes.to_vec(), self.offset)))
            .map_err(|_| io::ErrorKind::BrokenPipe)?;
        self.offset = end;
        self.remaining -= len as u64;
        self.digest.update(bytes);
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn download<T: Send + 'static>(
    start: u64,
    max: u64,
    frame: fn(Vec<u8>, u64) -> T,
    run: impl FnOnce(&mut Download<T>) -> Result<T, Status> + Send + 'static,
) -> WorkspaceStream<T> {
    let (tx, rx) = mpsc::channel(4);
    tokio::task::spawn_blocking(move || {
        let mut sink = Download {
            tx: tx.clone(),
            frame,
            offset: start,
            remaining: max,
            digest: Sha256::new(),
        };
        let result = run(&mut sink);
        let _ = tx.blocking_send(result);
    });
    ReceiverStream::new(rx)
}

impl WorkspaceService {
    pub async fn read(
        &self,
        ctx: &RuntimeContext,
        req: h::ReadWorkspaceRequest,
    ) -> Result<WorkspaceStream<h::ReadWorkspaceResponse>, Status> {
        let max = if req.max_bytes == 0 {
            MAX_BYTES
        } else {
            req.max_bytes
        };
        if max > MAX_BYTES {
            return Err(Status::invalid_argument(
                "workspace read range exceeds limits",
            ));
        }
        let mut target = target(&req.volume_id, req.path)?;
        let lease = self
            .resolve(ctx, &mut target, AccessMode::ReadOnly)
            .await?;
        Ok(download(
            req.offset,
            max,
            |data, offset| h::ReadWorkspaceResponse {
                data,
                offset,
                ..Default::default()
            },
            move |sink| {
                let reply = lease.read(
                    g::WorkspaceReadRequest {
                        target,
                        offset: req.offset,
                        max_bytes: max,
                    },
                    sink,
                )?;
                if reply.end_offset != sink.offset
                    || reply.sha256 != <[u8; 32]>::from(sink.digest.clone().finalize())
                {
                    return Err(Status::data_loss("workspace read receipt mismatch"));
                }
                Ok(h::ReadWorkspaceResponse {
                    offset: sink.offset,
                    sha256: reply.sha256.to_vec(),
                    r#final: true,
                    data: vec![],
                })
            },
        ))
    }

    pub async fn archive(
        &self,
        ctx: &RuntimeContext,
        req: h::ArchiveWorkspaceRequest,
    ) -> Result<WorkspaceStream<h::ArchiveWorkspaceResponse>, Status> {
        let mut target = target("", req.path)?;
        let lease = self
            .resolve(ctx, &mut target, AccessMode::ReadOnly)
            .await?;
        Ok(download(
            0,
            MAX_BYTES,
            |data, _| h::ArchiveWorkspaceResponse {
                data,
                ..Default::default()
            },
            move |sink| {
                let reply = lease.export_archive(target, sink)?;
                if reply.sha256 != <[u8; 32]>::from(sink.digest.clone().finalize()) {
                    return Err(Status::data_loss("workspace archive digest mismatch"));
                }
                Ok(h::ArchiveWorkspaceResponse {
                    data: vec![],
                    sha256: reply.sha256.to_vec(),
                    r#final: true,
                })
            },
        ))
    }
}
