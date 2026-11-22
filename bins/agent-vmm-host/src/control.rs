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
use vmm_compat::session::RuntimeAuth;
use vmm_rpc::{Intercepted, Router};

pub struct ControlListener {
    listener: UnixListener,
    _path: SocketPath,
    routes: Router,
    max_streams: usize,
}

impl ControlListener {
    pub async fn operator(
        path: &Path,
        operator: impl OperatorService,
        appliances: impl ApplianceAdminService,
    ) -> io::Result<Self> {
        let operator = OperatorServiceServer::new(operator);
        let appliances = ApplianceAdminServiceServer::new(appliances);
        Self::open(path, Router::new(operator).add_service(appliances), 128).await
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
        let runtime = AgentRuntimeServiceServer::from_arc(service);
        Self::open(&path, Router::new(Intercepted::new(runtime, auth)), 64).await
    }

    async fn open(path: &Path, routes: Router, max_streams: usize) -> io::Result<Self> {
        let (listener, guard) = listen(path).await?;
        Ok(Self {
            listener,
            _path: guard,
            routes,
            max_streams,
        })
    }

    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> io::Result<()> {
        self.routes
            .serve_unix(self.listener, self.max_streams, shutdown)
            .await
    }
}

/// Expose the backend and API readiness independently during host startup.
pub async fn serve_local(
    path: &Path,
    api: impl Into<Arc<vmm_compat::operator::LocalOperator>>,
    core: Option<vmm_core::Core>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (listener, _guard) = listen(path).await?;
    let api = api.into();
    let mut routes = Router::new(vmm_compat::operator::LocalOperatorServer(api.clone()));
    if let Some(appliances) = api.appliances.clone() {
        routes = routes.add_service(appliances);
    }
    let server = routes.serve_unix(listener, 128, shutdown);
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

/// Online Linux and Carrier hosts accept exactly the tunnels issued by the registered connectors.
pub(crate) async fn serve_online(
    path: &Path,
    api: vmm_compat::operator::LocalOperator,
    registrations: Arc<crate::registrations::ExternalRegistrations>,
    runtime: Arc<vmm_compat::runtime::RuntimeService>,
    mcp_port: u16,
    available_disk: u64,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, mcp_port)).await?;
    let management = api.mcp.as_ref().ok_or("missing MCP token authority")?;
    let authority = management.token_authority();
    let tools = Arc::new(vmm_compat::tools::NativeTools::open(
        runtime.clone(),
        available_disk,
    )?);
    management.publish_frontend(listener.local_addr()?, tools.registration_id())?;
    let core = runtime.core.clone();

    // Allocation admission replaces the PID and disk limits from the accepted pool policy.
    let defaults = vmm_core::Limits {
        vcpus: 1,
        boot_ram_mb: 2048,
        max_ram_mb: 2048,
        pids_max: 0,
        disk_quota_bytes: 0,
    };
    let signer = api.trust.clone().ok_or("missing device identity")?;
    let identity = signer.identity()?;
    let node = if identity.state() == agent_vmm_proto::host::v1::DeviceIdentityState::Ready {
        Some(crate::service_tunnel::ServiceNode::bind(signer).await?)
    } else {
        None
    };
    let routes = crate::service_forward::ServiceForward {
        device: identity
            .identity
            .ok_or("missing device identity")?
            .device_id,
        store: core.store.clone(),
        guest: runtime.guest.clone(),
        // Enrollment currently supplies gateway TLS credentials only.
        trust: None,
        budgets: Default::default(),
    };
    let api = Arc::new(api);
    let (observations, latest) =
        tokio::sync::watch::channel(crate::observation::sample(&api).await);
    registrations.enable_connectors(
        core.clone(),
        defaults,
        Arc::new(AgentRuntimeServiceServer::from_arc(runtime)),
        node.as_ref().map(|n| n.observations()),
        Arc::new(move || latest.borrow().clone()),
    )?;
    let (stop, mut operator_stop) = tokio::sync::watch::channel(false);
    let mut mcp_stop = stop.subscribe();
    let mcp = vmm_mcp::serve(listener, authority, tools, async move {
        let _ = mcp_stop.changed().await;
    });
    let operator = serve_local(path, api.clone(), Some(core), async move {
        let _ = operator_stop.changed().await;
    });
    let mut data_stop = stop.subscribe();
    let data = async move {
        if let Some(node) = node {
            node.serve(routes, data_stop).await?;
        } else {
            let _ = data_stop.changed().await;
        }
        Ok(())
    };
    let mut observation_stop = stop.subscribe();
    let refresh = async {
        tokio::select! {
            _ = observation_stop.changed() => {},
            _ = crate::observation::refresh(&api, observations) => {},
        }
        Ok(())
    };
    let servers = async {
        tokio::try_join!(operator, data, refresh, async { Ok(mcp.await?) })?;
        Ok(())
    };
    tokio::pin!(servers);
    tokio::select! {
        result=&mut servers => result,
        _=shutdown=>{
            stop.send_replace(true);
            match tokio::time::timeout(std::time::Duration::from_secs(4),servers).await {
                Ok(result)=>result,
                Err(_)=>Ok(()),
            }
        }
    }
}
