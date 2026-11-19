//! Guest transports can be local to dom0 or supplied by the macOS carrier.
use crate::{
    RuntimeContext,
    container::current,
    error::{client_status, host_status},
    execution::live,
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
use prost::Message;
use std::sync::Arc;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use vmm_rpc::{Code, Status};
use vmm_core::BootIdentity;
use vmm_guest_proto::{client::Client, rpc as g};

pub type Output<T> = ReceiverStream<Result<T, Status>>;
pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

#[async_trait]
pub trait GuestDriver: Send + Sync {
    /// Return an authenticated client bound to the exact incarnation.
    async fn client(&self, boot: &BootIdentity) -> Result<Client, Status>;
    /// Dial the authorized guest port, retaining the complete incarnation fence.
    async fn connect_port(&self, boot: &BootIdentity, port: u16)
    -> Result<Box<dyn Duplex>, Status>;
}
pub(crate) fn signal(value: i32) -> Result<g::Signal, Status> {
    match h::ProcessSignal::try_from(value) {
        Ok(h::ProcessSignal::Int) => Ok(g::Signal::Int),
        Ok(h::ProcessSignal::Kill) => Ok(g::Signal::Kill),
        Ok(h::ProcessSignal::Term) => Ok(g::Signal::Term),
        _ => Err(Status::invalid_argument("invalid process signal")),
    }
}
pub(crate) fn running(sb: vmm_core::Sandbox) -> Result<BootIdentity, Status> {
    sb.boot.ok_or_else(|| {
        host_status(
            Code::FailedPrecondition,
            h::ErrorReason::ContainerNotRunning,
            "container has no live instance",
        )
    })
}
pub async fn signal_container(
    ctx: &RuntimeContext,
    driver: Arc<dyn GuestDriver>,
    r: h::SignalContainerRequest,
) -> Result<h::SignalContainerResponse, Status> {
    let sig = signal(r.signal)?;
    let boot = running(current(ctx, &r.container_id)?)?;
    mutate(
        ctx.core.store.clone(),
        ctx.scope.id.clone(),
        "runtime-signal-container",
        r.request_id.clone(),
        r.encode_to_vec(),
        async move {
            let client = driver.client(&boot).await?;
            tokio::task::spawn_blocking(move || client.send_signal(sig))
                .await
                .map_err(|_| Status::internal("signal task interrupted"))?
                .map_err(client_status)?;
            Ok(h::SignalContainerResponse {})
        },
    )
    .await
}
pub async fn forward<S>(
    ctx: &RuntimeContext,
    driver: &dyn GuestDriver,
    mut src: S,
) -> Result<Output<h::ForwardTcpResponse>, Status>
where
    S: Stream<Item = Result<h::ForwardTcpRequest, Status>> + Send + Unpin + 'static,
{
    use h::forward_tcp_request::Event as In;
    use h::forward_tcp_response::Event as Out;
    let Some(In::Header(start)) = src.next().await.transpose()?.and_then(|r| r.event) else {
        return Err(Status::invalid_argument(
            "first forward frame must be a header",
        ));
    };
    let boot = live(ctx, &start.container_id, &start.container_instance_id)?;
    let port = u16::try_from(start.port)
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| Status::invalid_argument("invalid port"))?;
    let io = driver.connect_port(&boot, port).await?;
    let (mut reader, mut writer) = tokio::io::split(io);
    let (tx, rx) = mpsc::channel(4);
    let send = tx.clone();
    tokio::spawn(async move {
        let upload = async {
            while let Some(frame) = src.next().await.transpose()? {
                match frame.event {
                    Some(In::Data(bytes)) => writer
                        .write_all(&bytes)
                        .await
                        .map_err(|_| Status::unavailable("guest TCP write failed"))?,
                    Some(In::CloseWrite(true)) => break,
                    _ => return Err(Status::invalid_argument("invalid forward frame")),
                }
            }
            writer
                .shutdown()
                .await
                .map_err(|_| Status::unavailable("guest TCP shutdown failed"))
        };
        let download = async {
            let mut buffer = vec![0; 32 << 10];
            loop {
                let n = reader
                    .read(&mut buffer)
                    .await
                    .map_err(|_| Status::unavailable("guest TCP read failed"))?;
                let event = if n == 0 {
                    Out::CloseRead(true)
                } else {
                    Out::Data(buffer[..n].to_vec())
                };
                tx.send(Ok(h::ForwardTcpResponse { event: Some(event) }))
                    .await
                    .map_err(|_| Status::cancelled("forward client disconnected"))?;
                if n == 0 {
                    return Ok(());
                }
            }
        };
        tokio::select! {
            result=async {tokio::try_join!(upload,download)}=>if let Err(e)=result {let _=send.send(Err(e)).await;},
            _=send.closed()=>{}
        }
    });
    Ok(ReceiverStream::new(rx))
}
pub async fn logs(
    ctx: &RuntimeContext,
    driver: &dyn GuestDriver,
    r: h::LogsRequest,
) -> Result<Output<h::LogsResponse>, Status> {
    logs_in_scope(ctx.core.store.clone(), &ctx.scope.id, driver, r).await
}

pub(crate) async fn logs_in_scope(
    store: Arc<vmm_core::Store>,
    scope: &str,
    driver: &dyn GuestDriver,
    r: h::LogsRequest,
) -> Result<Output<h::LogsResponse>, Status> {
    let sb = crate::container::find_in_scope(&store, scope, &r.container_id)?
        .ok_or_else(|| Status::not_found("container not found"))?;
    let generation_id = &sb.definition.key.generation_id;
    let prefix = format!("{}:{}:", r.container_id, generation_id);
    let after = if r.after_cursor.is_empty() {
        None
    } else {
        let mut parts = r.after_cursor.split(':');
        if parts.next() != Some(&r.container_id) {
            return Err(Status::invalid_argument("invalid log cursor"));
        }
        let generation = parts
            .next()
            .ok_or_else(|| Status::invalid_argument("invalid log cursor"))?;
        let num = parts
            .next()
            .and_then(|s| s.parse().ok())
            .filter(|_| parts.next().is_none())
            .ok_or_else(|| Status::invalid_argument("invalid log cursor sequence"))?;
        if generation != generation_id {
            return Err(host_status(
                Code::Aborted,
                h::ErrorReason::LogCursorStale,
                "container generation changed",
            ));
        }
        Some(num)
    };
    let since = r
        .since
        .map(|s| {
            s.seconds
                .checked_mul(1_000_000_000)
                .and_then(|n| n.checked_add(i64::from(s.nanos)))
                .filter(|_| (0..1_000_000_000).contains(&s.nanos))
                .ok_or_else(|| Status::invalid_argument("invalid log timestamp"))
        })
        .transpose()?;
    let (tx, rx) = mpsc::channel(8);
    if let Some(boot) = sb.boot {
        let client = driver.client(&boot).await?;
        tokio::spawn(async move {
            // Fetch finite snapshots so cancellation can release the guest connection.
            let mut position = after;
            let mut tail = r.tail_records;
            loop {
                let out = tx.clone();
                let client = client.clone();
                let prefix = prefix.clone();
                let req = g::ReadLogsRequest {
                    after_cursor: position,
                    since_unix_nanos: since,
                    tail_records: tail,
                    follow: false,
                };
                let batch = tokio::task::spawn_blocking(move || {
                    client.read_logs(req, |record| {
                        let stream = match record.stream {
                            g::LogStream::Stdout => h::LogStream::Stdout,
                            g::LogStream::Stderr => h::LogStream::Stderr,
                        };
                        let nanos = record.timestamp_unix_nanos;
                        let _ = out.blocking_send(Ok(h::LogsResponse {
                            cursor: format!("{prefix}{}", record.cursor),
                            timestamp: Some(prost_types::Timestamp {
                                seconds: nanos.div_euclid(1_000_000_000),
                                nanos: nanos.rem_euclid(1_000_000_000) as i32,
                            }),
                            stream: stream as i32,
                            data: record.data,
                            truncated: record.dropped_records > 0,
                            dropped_records: record.dropped_records,
                        }));
                    })
                })
                .await
                .map_err(|_| Status::internal("guest log task interrupted"))
                .and_then(|r| r.map_err(client_status));
                let end = match batch {
                    Ok(end) => end,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if !r.follow || end.ended || tx.is_closed() {
                    return;
                }
                position = Some(end.next_cursor);
                tail = 0;
                tokio::select! {_=tx.closed()=>return,_=tokio::time::sleep(std::time::Duration::from_millis(200))=>{}}
                if !matches!(store.sandbox(&boot.sandbox),Ok(sb) if sb.boot.as_ref()==Some(&boot)) {
                    return;
                }
            }
        });
    }
    Ok(ReceiverStream::new(rx))
}
