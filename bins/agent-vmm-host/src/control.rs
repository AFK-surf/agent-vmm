//! Private local management and session-scoped runtime listeners.
use crate::socket::{SocketPath, listen};
use agent_vmm_proto::{
    appliance::v1::appliance_admin_service_server::{
        ApplianceAdminService, ApplianceAdminServiceServer,
    },
    host::v1::{
        agent_runtime_service_server::{AgentRuntimeService, AgentRuntimeServiceServer},
        operator_service_server::{OperatorService, OperatorServiceServer},
    },
};
use std::{future::Future, io, path::Path, sync::Arc};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{
    service::{Routes, interceptor::InterceptedService},
    transport::{Error, Server},
};
use vmm_compat::session::RuntimeAuth;

const MAX_MESSAGE: usize = 16 << 20;

async fn stop_server(
    server: impl Future<Output = Result<(), Error>>,
    stop: tokio::sync::oneshot::Sender<()>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Error> {
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => return result,
        _ = shutdown => {},
    }
    let _ = stop.send(());
    // A client can keep an HTTP/2 stream open indefinitely after GOAWAY.
    match tokio::time::timeout(std::time::Duration::from_secs(3), server).await {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}

pub struct ControlListener {
    listener: UnixListener,
    _path: SocketPath,
    routes: Routes,
    max_streams: u32,
}

impl ControlListener {
    pub async fn operator(
        path: &Path,
        operator: impl OperatorService,
        appliances: impl ApplianceAdminService,
    ) -> io::Result<Self> {
        let operator = OperatorServiceServer::new(operator)
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        let appliances = ApplianceAdminServiceServer::new(appliances)
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        Self::open(path, Routes::new(operator).add_service(appliances), 128).await
    }

    pub async fn runtime(
        directory: &Path,
        auth: RuntimeAuth,
        service: Arc<impl AgentRuntimeService>,
    ) -> io::Result<Self> {
        let id = auth.session_id();
        if id.is_empty()
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid session ID",
            ));
        }
        let path = directory.join(format!("{id}.sock"));
        let runtime = AgentRuntimeServiceServer::from_arc(service)
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        Self::open(
            &path,
            Routes::new(InterceptedService::new(runtime, auth)),
            64,
        )
        .await
    }

    async fn open(path: &Path, routes: Routes, max_streams: u32) -> io::Result<Self> {
        let (listener, guard) = listen(path).await?;
        Ok(Self {
            listener,
            _path: guard,
            routes,
            max_streams,
        })
    }

    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> Result<(), Error> {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = Server::builder()
            .max_concurrent_streams(self.max_streams)
            .add_routes(self.routes)
            .serve_with_incoming_shutdown(UnixListenerStream::new(self.listener), async {
                let _ = stopped.await;
            });
        stop_server(server, stop, shutdown).await
    }
}

/// Expose the backend and API readiness independently during host startup.
pub async fn serve_local(
    path: &Path,
    api: vmm_compat::operator::LocalOperator,
    core: Option<vmm_core::Core>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (listener, _guard) = listen(path).await?;
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status(
            "vmm-dom0",
            if core.is_some() {
                tonic_health::ServingStatus::Serving
            } else {
                tonic_health::ServingStatus::NotServing
            },
        )
        .await;
    for name in [
        "",
        "agentvmm.host.v1.OperatorService",
        "agentvmm.host.v1.AgentRuntimeService",
    ] {
        reporter
            .set_service_status(name, tonic_health::ServingStatus::NotServing)
            .await;
    }
    let api = Arc::new(api);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = Server::builder()
        .max_concurrent_streams(128)
        .add_optional_service(
            api.mcp
                .clone()
                .map(vmm_compat::operator::ApplianceOperations),
        )
        .add_service(health)
        .add_service(vmm_compat::operator::LocalOperatorServer(api.clone()))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
            let _ = stopped.await;
        });
    let server = stop_server(server, stop, shutdown);
    if let Some(core) = core {
        tokio::select! {
            result=server => result?,
            _=api.drive_disposals(&core) => return Err(io::Error::other("operator disposal worker stopped").into()),
        }
    } else {
        server.await?;
    }
    Ok(())
}
