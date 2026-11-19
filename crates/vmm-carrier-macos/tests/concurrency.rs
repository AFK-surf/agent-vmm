use std::{io, sync::Arc, time::Duration};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Mutex, oneshot},
    time::timeout,
};
use vmm_carrier_macos::{
    client::CarrierClient,
    mock::MockCarrierHypervisor,
    protocol::*,
    server::{CarrierHypervisor, PendingVsock, serve},
};

// Model a Guest that neither accepts its first vsock nor acknowledges a graceful stop.
struct UnresponsiveGuest {
    vm: MockCarrierHypervisor,
    dialing: Option<oneshot::Sender<()>>,
    finish_dial: Option<oneshot::Receiver<()>>,
    stopping: Option<oneshot::Sender<()>>,
}
#[async_trait::async_trait]
impl CarrierHypervisor for UnresponsiveGuest {
    fn begin_open_vsock(&mut self, _: u32) -> io::Result<PendingVsock> {
        if let Some(rx) = self.finish_dial.take() {
            self.dialing.take().unwrap().send(()).unwrap();
            return Ok(Box::pin(async move {
                let _ = rx.await;
                Err(io::ErrorKind::ConnectionReset.into())
            }));
        }
        Ok(Box::pin(async { Ok(UnixStream::pair()?.0) }))
    }
    fn request_stop(&mut self) -> io::Result<()> {
        self.stopping.take().unwrap().send(()).unwrap();
        Ok(())
    }
    async fn force_stop(&mut self) -> io::Result<()> {
        self.vm.force_stop().await
    }
    async fn status(&mut self) -> io::Result<CarrierStatus> {
        self.vm.status().await
    }
    async fn ensure_started(&mut self, id: String) -> io::Result<()> {
        self.vm.ensure_started(id).await
    }
    async fn set_target_virtual_machine_memory_size(&mut self, bytes: u64) -> io::Result<()> {
        self.vm.set_target_virtual_machine_memory_size(bytes).await
    }
    async fn set_host_shares(&mut self, shares: Shares) -> io::Result<()> {
        self.vm.set_host_shares(shares).await
    }
    async fn punch(&mut self, offset: u64, len: u64) -> io::Result<()> {
        self.vm.punch(offset, len).await
    }
}

#[tokio::test]
async fn pending_dial_and_graceful_stop_allow_other_requests() -> io::Result<()> {
    timeout(Duration::from_secs(3), async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("carrier");
        let listener = UnixListener::bind(&path)?;
        let (dialing, dial_started) = oneshot::channel();
        let (finish, finish_dial) = oneshot::channel();
        let (stopping, stop_started) = oneshot::channel();
        let guest = UnresponsiveGuest {
            vm: MockCarrierHypervisor::new(Default::default()),
            dialing: Some(dialing),
            finish_dial: Some(finish_dial),
            stopping: Some(stopping),
        };
        let client = CarrierClient::new(path);
        let server = tokio::spawn(serve(
            listener,
            Arc::new(Mutex::new(guest)),
            std::future::pending(),
        ));
        client
            .control(Request::EnsureStarted {
                operation_id: "boot".into(),
            })
            .await?;
        let dial_client = client.clone();
        let dial = tokio::spawn(async move { dial_client.open_vsock(GUEST_PORT).await });
        dial_started.await.unwrap();
        let stop_client = client.clone();
        let graceful =
            tokio::spawn(async move { stop_client.control(Request::Stop { force: false }).await });
        stop_started.await.unwrap();
        assert_eq!(client.control(Request::Status).await?.state, State::Running);
        drop(client.open_vsock(CONTROL_PORT).await?);
        assert_eq!(
            client.control(Request::Stop { force: true }).await?.state,
            State::Stopped
        );
        assert_eq!(graceful.await.unwrap()?.state, State::Stopped);
        finish.send(()).unwrap();
        assert!(dial.await.unwrap().is_err());
        server.abort();
        Ok(())
    })
    .await?
}
