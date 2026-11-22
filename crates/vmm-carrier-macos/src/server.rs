#![forbid(unsafe_code)]
use crate::protocol::*;
use std::{io, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};

pub type PendingVsock = Pin<Box<dyn Future<Output = io::Result<UnixStream>> + Send>>;

#[async_trait::async_trait]
pub trait CarrierHypervisor: Send {
    async fn status(&mut self) -> io::Result<CarrierStatus>;
    async fn ensure_started(&mut self, operation_id: String) -> io::Result<()>;
    fn request_stop(&mut self) -> io::Result<()>;
    async fn graceful_stop(&mut self) -> io::Result<()> {
        self.request_stop()
    }
    async fn force_stop(&mut self) -> io::Result<()>;
    async fn set_host_shares(&mut self, shares: Shares) -> io::Result<()>;
    async fn punch(&mut self, offset: u64, len: u64) -> io::Result<()>;
    fn begin_open_vsock(&mut self, port: u32) -> io::Result<PendingVsock>;
}

async fn connection<H: CarrierHypervisor>(
    mut socket: UnixStream,
    machine: Arc<Mutex<H>>,
    control_permit: OwnedSemaphorePermit,
    streams: Arc<Semaphore>,
) -> io::Result<()> {
    let (version, req): (u8, Request) =
        timeout(Duration::from_secs(10), read(&mut socket)).await??;
    // After a data upgrade, a long-lived copy occupies only a data-stream slot.
    let _permit = if matches!(req, Request::OpenVsock { port } if port != CONTROL_PORT) {
        let Ok(permit) = streams.try_acquire_owned() else {
            return write(
                &mut socket,
                &Reply::Error("carrier data streams busy".into()),
            )
            .await;
        };
        drop(control_permit);
        permit
    } else {
        control_permit
    };
    let result: io::Result<_> = async {
        if version != 1 {
            return Err(io::Error::other("unsupported carrier protocol"));
        }
        let mut vm = machine.lock().await;
        match req {
            Request::Status => {}
            Request::EnsureStarted { operation_id } => vm.ensure_started(operation_id).await?,
            Request::Stop { force: true } => vm.force_stop().await?,
            Request::Stop { force: false } => {
                vm.graceful_stop().await?;
                let mut status = vm.status().await?;
                let stopping_boot = status.boot_id.clone();
                drop(vm);
                timeout(Duration::from_secs(60), async {
                    while status.state == State::Running && status.boot_id == stopping_boot {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        status = machine.lock().await.status().await?;
                    }
                    io::Result::Ok(())
                })
                .await??;
                return Ok((Reply::Status(status), None));
            }
            Request::SetHostShares { shares } => vm.set_host_shares(shares).await?,
            Request::PunchPoolRange { offset, len } => vm.punch(offset, len).await?,
            Request::OpenVsock { port } => {
                let connection = vm.begin_open_vsock(port)?;
                drop(vm);
                let guest = timeout(Duration::from_secs(10), connection).await??;
                return Ok((Reply::Stream, Some(guest)));
            }
        }
        Ok((Reply::Status(vm.status().await?), None))
    }
    .await;
    let (reply, stream) = result.unwrap_or_else(|e| (Reply::Error(e.to_string()), None));
    write(&mut socket, &reply).await?;
    if let Some(mut guest) = stream {
        tokio::io::copy_bidirectional(&mut socket, &mut guest).await?;
    }
    Ok(())
}

pub async fn serve<H: CarrierHypervisor + 'static>(
    socket: UnixListener,
    machine: Arc<Mutex<H>>,
    stop: impl Future<Output = ()>,
) -> io::Result<()> {
    tokio::pin!(stop);
    let permits = Arc::new(Semaphore::new(256));
    let streams = Arc::new(Semaphore::new(1024));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut stop => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = socket.accept() => {
                let (client,_) = accepted?;
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
                let machine = machine.clone();
                connections.spawn(connection(client, machine, permit, streams.clone()));
            }
        }
    }
    machine.lock().await.force_stop().await?;
    connections.shutdown().await;
    Ok(())
}
