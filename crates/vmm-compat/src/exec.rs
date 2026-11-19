use crate::{
    Containers, RuntimeContext,
    container_spec::environment,
    error::client_status,
    execution::live,
    guest::{GuestDriver, Output, signal},
};
use agent_vmm_proto::host::v1 as h;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use vmm_rpc::Status;
use vmm_guest_proto::{
    client::{ExecEvent, ExecInput},
    rpc as g,
};

fn size(rows: u32, columns: u32) -> Result<g::TtySize, Status> {
    let bound =
        |v| u16::try_from(v).map_err(|_| Status::invalid_argument("terminal size exceeds uint16"));
    Ok(g::TtySize {
        rows: bound(rows)?,
        columns: bound(columns)?,
    })
}

impl Containers {
    pub async fn exec<S>(
        &self,
        ctx: &RuntimeContext,
        driver: &dyn GuestDriver,
        mut src: S,
    ) -> Result<Output<h::ExecResponse>, Status>
    where
        S: Stream<Item = Result<h::ExecRequest, Status>> + Send + Unpin + 'static,
    {
        use h::exec_request::Event as In;
        use h::exec_response::Event as Out;
        let Some(In::Start(r)) = src.next().await.transpose()?.and_then(|r| r.event) else {
            return Err(Status::invalid_argument("first exec event must be start"));
        };
        if r.request_id.is_empty()
            || r.argv.is_empty()
            || !matches!(
                r.execution_kind(),
                h::ExecutionKind::Exec | h::ExecutionKind::Process | h::ExecutionKind::Build
            )
        {
            return Err(Status::invalid_argument("invalid exec start"));
        }
        environment(&r.env)?;
        let tty = if r.tty {
            Some(size(r.rows, r.columns)?)
        } else {
            None
        };
        let boot = live(ctx, &r.container_id, &r.expected_instance_id)?;
        let client = driver.client(&boot).await?;
        let release = h::ReleaseExecutionRequest {
            execution_id: r.execution_id.clone(),
            container_id: r.container_id.clone(),
            expected_instance_id: r.expected_instance_id.clone(),
            kind: r.execution_kind,
            ..Default::default()
        };
        let claim = self.acquire(
            ctx,
            h::AcquireExecutionRequest {
                execution_id: r.execution_id,
                container_id: r.container_id,
                expected_instance_id: r.expected_instance_id,
                kind: r.execution_kind,
                deadline_unix_nano: r.execution_deadline_unix_nano,
                ..Default::default()
            },
        )?;
        if !claim.acquired {
            return Err(Status::aborted("execution already accepted"));
        }
        let guest = g::ExecRequest {
            command: g::CommandSpec {
                argv: r.argv,
                env: r.env,
                workdir: r.workdir,
                user: r.user,
            },
            stdin: true,
            tty,
            timeout_ms: 0,
            max_output_bytes: 0,
        };
        let (tx, rx) = mpsc::channel(8);
        let host = self.clone();
        let owner = ctx.clone();
        // Once acquired the host reads the terminal evidence even if the RPC peer disconnects.
        tokio::spawn(async move {
            let opened = tokio::task::spawn_blocking(move || client.exec_stream(guest))
                .await
                .map_err(|_| Status::internal("guest launch task interrupted"))
                .and_then(|r| r.map_err(client_status));
            let mut session = match opened {
                Ok(session) => session,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            let input = match session.input() {
                Ok(input) => input,
                Err(_) => {
                    let _ = tx
                        .send(Err(Status::unavailable("guest input unavailable")))
                        .await;
                    return;
                }
            };
            let (frames, mut inbox) = mpsc::channel(4);
            let input_tx = tx.clone();
            let writer = tokio::task::spawn_blocking(move || {
                if let Err(e) = write_frames(input, &mut inbox) {
                    let _ = input_tx.blocking_send(Err(e));
                }
            });
            let feed = tokio::spawn(async move {
                while let Some(frame) = src.next().await {
                    if frames.send(frame).await.is_err() {
                        break;
                    }
                }
            });
            let terminal = tokio::task::spawn_blocking(move || {
                loop {
                    let event = match session.next_event() {
                        Ok(ExecEvent::Stdout(data)) => Out::Stdout(data),
                        Ok(ExecEvent::Stderr(data)) => Out::Stderr(data),
                        Ok(ExecEvent::Exit(result)) => {
                            match u32::try_from(result.exit.code) {
                                Ok(code) => {
                                    let settled =
                                        host.release(&owner, release).map(|_| h::ExecResponse {
                                            event: Some(Out::ExitCode(code)),
                                        });
                                    let _ = tx.blocking_send(settled);
                                }
                                Err(_) => {
                                    let _ = tx
                                        .blocking_send(Err(Status::data_loss("invalid exit code")));
                                }
                            }
                            break;
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(client_status(e)));
                            break;
                        }
                    };
                    let _ = tx.blocking_send(Ok(h::ExecResponse { event: Some(event) }));
                }
            });
            let _ = terminal.await;
            feed.abort();
            let _ = writer.await;
        });
        Ok(ReceiverStream::new(rx))
    }
}

fn write_frames(
    mut input: ExecInput,
    frames: &mut mpsc::Receiver<Result<h::ExecRequest, Status>>,
) -> Result<(), Status> {
    use h::exec_request::Event as In;
    let io_status = |_: std::io::Error| Status::unavailable("guest exec input closed");
    let mut closed = false;
    let result = (|| {
        while let Some(frame) = frames.blocking_recv() {
            match frame?.event {
                Some(In::Stdin(bytes)) if !closed => input.write(&bytes),
                Some(In::CloseStdin(true)) if !closed => {
                    closed = true;
                    input.close()
                }
                Some(In::Resize(s)) => input.resize(size(s.rows, s.columns)?),
                Some(In::Signal(s)) => input.signal(signal(s)?),
                _ => return Err(Status::invalid_argument("invalid exec input frame")),
            }
            .map_err(io_status)?;
        }
        Ok(())
    })();
    if !closed {
        let close = input.close().map_err(io_status);
        result.and(close)
    } else {
        result
    }
}
