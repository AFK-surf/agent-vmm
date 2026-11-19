//! Serve the runtime API over controller-authenticated HTTP/2 tunnels.
use agent_vmm_proto::host::v1::agent_runtime_service_server::{
    AgentRuntimeService, AgentRuntimeServiceServer,
};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{
    Request, Status,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Server, server::Connected},
};
use vmm_compat::session::{RemoteIdentity, RuntimeAuth};
use vmm_core::Store;
use vmm_remote_salix::{RuntimeSession, TunnelIo};

struct Connection {
    transport: TunnelIo,
    auth: RuntimeAuth,
}

impl Connected for Connection {
    type ConnectInfo = RuntimeAuth;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.auth.clone()
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.transport).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.transport).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.transport).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.transport).poll_shutdown(cx)
    }
}

fn authenticate(req: Request<()>) -> Result<Request<()>, Status> {
    let mut auth = req
        .extensions()
        .get::<RuntimeAuth>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("missing transport session"))?;
    auth.call(req)
}

pub async fn serve(
    store: Arc<Store>,
    service: Arc<impl AgentRuntimeService>,
    sessions: mpsc::Receiver<RuntimeSession>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let incoming = ReceiverStream::new(sessions).map(move |session| {
        let id = session.authorization.id;
        let identity = RemoteIdentity {
            session_id: id.clone(),
            registration_id: session.registration_id,
            allocation_id: session.allocation_id,
            generation: session.allocation_generation,
            execution_owner_id: session.execution_owner_id,
        };
        let auth = RuntimeAuth::bound(store.clone(), id, session.capability).for_remote(identity);
        Ok::<_, io::Error>(Connection {
            auth,
            transport: session.transport,
        })
    });
    let runtime = AgentRuntimeServiceServer::from_arc(service)
        .max_decoding_message_size(16 << 20)
        .max_encoding_message_size(16 << 20);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = Server::builder()
        .max_concurrent_streams(64)
        .add_service(InterceptedService::new(runtime, authenticate))
        .serve_with_incoming_shutdown(incoming, async {
            let _ = stopped.await;
        });
    crate::control::stop_server(server, stop, shutdown).await
}
