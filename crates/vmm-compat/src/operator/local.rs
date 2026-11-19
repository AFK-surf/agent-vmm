//! The local management socket stays available while the runtime backend is offline.
use super::{BOOT_ID, DiskSample, LocalDisposals, LocalInventory, RegistrationControl, h};
use std::{collections::HashSet, sync::Arc, time::SystemTime};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};
use vmm_core::{Core, Store, backend::Dom0Backend};

pub struct LocalOperator {
    inventory: LocalInventory,
    registrations: Arc<dyn RegistrationControl>,
    disposals: LocalDisposals,
    backend: Option<Arc<dyn Dom0Backend>>,
    wake: Notify,
    pub mcp: Option<Arc<super::McpManagement>>,
    disk_sample: Option<Arc<dyn Fn() -> vmm_core::Result<DiskSample> + Send + Sync>>,
}

impl LocalOperator {
    pub fn open(
        path: &std::path::Path,
        store: Arc<Store>,
        registrations: Arc<dyn RegistrationControl>,
        backend: Option<Arc<dyn Dom0Backend>>,
    ) -> Result<Self, Status> {
        Ok(Self {
            disposals: LocalDisposals::open(path, store.clone(), registrations.clone())?,
            inventory: LocalInventory::new(store),
            registrations,
            backend,
            wake: Notify::new(),
            mcp: None,
            disk_sample: None,
        })
    }

    pub fn with_disk_sample(
        mut self,
        sample: impl Fn() -> vmm_core::Result<DiskSample> + Send + Sync + 'static,
    ) -> Self {
        self.disk_sample = Some(Arc::new(sample));
        self
    }

    pub async fn drive_disposals(&self, core: &Core) {
        let mut after = String::new();
        loop {
            if let Ok(page) = self.disposals.pending(&after) {
                for id in &page {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(120),
                        self.disposals.reconcile(core, id),
                    )
                    .await;
                }
                after = page
                    .last()
                    .filter(|_| page.len() == 32)
                    .cloned()
                    .unwrap_or_default();
            }
            tokio::select! {
                _ = self.wake.notified()=>{},
                _ = tokio::time::sleep(std::time::Duration::from_secs(15))=>{},
            }
        }
    }

    fn vm_state(&self) -> h::VmState {
        if self.backend.is_some() {
            h::VmState::Running
        } else {
            h::VmState::Stopped
        }
    }
    async fn vm(&self, _: h::GetVmRequest) -> Result<h::GetVmResponse, Status> {
        Ok(h::GetVmResponse {
            vm: Some(h::Vm {
                boot_id: BOOT_ID.clone(),
                state: self.vm_state() as i32,
                ..Default::default()
            }),
        })
    }
    async fn health(
        &self,
        _: h::GetSystemHealthSummaryRequest,
    ) -> Result<h::GetSystemHealthSummaryResponse, Status> {
        let now = Some(SystemTime::now().into());
        let (state, reason) = match &self.backend {
            None => (h::HealthState::Unavailable, "backend offline"),
            Some(b) => {
                match tokio::time::timeout(std::time::Duration::from_secs(4), b.list_live_vms())
                    .await
                {
                    Ok(Ok(_)) => (h::HealthState::Healthy, ""),
                    _ => (h::HealthState::Degraded, "backend observation unavailable"),
                }
            }
        };
        Ok(h::GetSystemHealthSummaryResponse {
            vm: self.vm(h::GetVmRequest {}).await?.vm,
            collected_at: now,
            health: Some(h::HealthStatus {
                state: state as i32,
                components: vec![h::ComponentHealth {
                    component: "dom0".into(),
                    state: state as i32,
                    reason: reason.into(),
                    observed_at: now,
                }],
            }),
            ..Default::default()
        })
    }
    async fn environments(
        &self,
        req: h::ListLocalEnvironmentPageRequest,
    ) -> Result<h::ListLocalEnvironmentPageResponse, Status> {
        let mut active = HashSet::new();
        if let Some(b) = &self.backend
            && let Ok(Ok(live)) =
                tokio::time::timeout(std::time::Duration::from_secs(4), b.list_live_vms()).await
        {
            for vm in live {
                let boot = vm.incarnation.boot;
                // Unowned VMs after a restart do not count as observations for an environment.
                if self
                    .inventory
                    .store
                    .sandbox(&boot.sandbox)
                    .is_ok_and(|s| s.boot.as_ref() == Some(&boot))
                {
                    active.insert(boot.sandbox.scope_id);
                }
            }
        }
        self.inventory.environments(req, self.vm_state(), &active)
    }
    async fn registrations(
        &self,
        req: h::ListLocalRegistrationPageRequest,
    ) -> Result<h::ListLocalRegistrationPageResponse, Status> {
        self.inventory
            .registrations(req, self.registrations.as_ref())
    }
    async fn disk(
        &self,
        _: h::GetLocalDiskSummaryRequest,
    ) -> Result<h::GetLocalDiskSummaryResponse, Status> {
        let sample = if let Some(sample) = self.disk_sample.clone() {
            tokio::time::timeout(
                std::time::Duration::from_secs(4),
                tokio::task::spawn_blocking(move || sample()),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(Result::ok)
        } else {
            None
        };
        Ok(self.inventory.disk(self.vm_state(), sample))
    }
    fn mcp(&self) -> Result<Arc<super::McpManagement>, Status> {
        self.mcp
            .clone()
            .ok_or_else(|| Status::failed_precondition("MCP management unavailable"))
    }
    async fn issue_token(
        &self,
        _: h::CreateMcpTokenRequest,
    ) -> Result<h::CreateMcpTokenResponse, Status> {
        let owner = self.mcp()?;
        let token = tokio::task::spawn_blocking(move || owner.issue())
            .await
            .map_err(|_| Status::unavailable("MCP authority unavailable"))??;
        Ok(h::CreateMcpTokenResponse { token })
    }
    async fn rotate_key(
        &self,
        _: h::RotateMcpTokenKeyRequest,
    ) -> Result<h::RotateMcpTokenKeyResponse, Status> {
        let owner = self.mcp()?;
        let operation_id = tokio::task::spawn_blocking(move || owner.rotate())
            .await
            .map_err(|_| Status::unavailable("MCP authority unavailable"))??;
        Ok(h::RotateMcpTokenKeyResponse { operation_id })
    }
    async fn plan(
        &self,
        req: h::PlanLocalDisposalRequest,
    ) -> Result<h::PlanLocalDisposalResponse, Status> {
        Ok(h::PlanLocalDisposalResponse {
            plan: Some(self.disposals.plan(req)?),
        })
    }
    async fn execute(
        &self,
        req: h::ExecuteLocalDisposalRequest,
    ) -> Result<h::ExecuteLocalDisposalResponse, Status> {
        let op = self.disposals.execute(req)?;
        self.wake.notify_one();
        Ok(h::ExecuteLocalDisposalResponse {
            operation: Some(op),
        })
    }
    async fn operation(
        &self,
        req: h::GetOperationRequest,
    ) -> Result<h::GetOperationResponse, Status> {
        Ok(h::GetOperationResponse {
            operation: Some(self.disposals.operation(&req.operation_id)?),
        })
    }
}

// Dispatch the local operator methods on their existing protobuf service paths.
#[derive(Clone)]
pub struct LocalOperatorServer(pub Arc<LocalOperator>);

use tonic::codegen::{Body, BoxFuture, Service, StdError, http};
impl tonic::server::NamedService for LocalOperatorServer {
    const NAME: &'static str = "agentvmm.host.v1.OperatorService";
}
impl<B> Service<http::Request<B>> for LocalOperatorServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        macro_rules! unary {
            ($name:ident,$request:ty,$response:ty) => {{
                struct Handler(Arc<LocalOperator>);
                impl tonic::server::UnaryService<$request> for Handler {
                    type Response = $response;
                    type Future = BoxFuture<Response<Self::Response>, Status>;
                    fn call(&mut self, req: Request<$request>) -> Self::Future {
                        let service = self.0.clone();
                        Box::pin(
                            async move { service.$name(req.into_inner()).await.map(Response::new) },
                        )
                    }
                }
                let handler = Handler(self.0.clone());
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .max_decoding_message_size(16 << 20)
                        .max_encoding_message_size(16 << 20);
                    Ok(grpc.unary(handler, req).await)
                })
            }};
        }
        match req
            .uri()
            .path()
            .strip_prefix("/agentvmm.host.v1.OperatorService/")
        {
            Some("CreateMCPToken") => unary!(
                issue_token,
                h::CreateMcpTokenRequest,
                h::CreateMcpTokenResponse
            ),
            Some("RotateMCPTokenKey") => unary!(
                rotate_key,
                h::RotateMcpTokenKeyRequest,
                h::RotateMcpTokenKeyResponse
            ),
            Some("GetVM") => unary!(vm, h::GetVmRequest, h::GetVmResponse),
            Some("GetSystemHealthSummary") => unary!(
                health,
                h::GetSystemHealthSummaryRequest,
                h::GetSystemHealthSummaryResponse
            ),
            Some("GetLocalDiskSummary") => unary!(
                disk,
                h::GetLocalDiskSummaryRequest,
                h::GetLocalDiskSummaryResponse
            ),
            Some("ListLocalEnvironmentPage") => unary!(
                environments,
                h::ListLocalEnvironmentPageRequest,
                h::ListLocalEnvironmentPageResponse
            ),
            Some("ListLocalRegistrationPage") => unary!(
                registrations,
                h::ListLocalRegistrationPageRequest,
                h::ListLocalRegistrationPageResponse
            ),
            Some("PlanLocalDisposal") => unary!(
                plan,
                h::PlanLocalDisposalRequest,
                h::PlanLocalDisposalResponse
            ),
            Some("ExecuteLocalDisposal") => unary!(
                execute,
                h::ExecuteLocalDisposalRequest,
                h::ExecuteLocalDisposalResponse
            ),
            Some("GetOperation") => {
                unary!(operation, h::GetOperationRequest, h::GetOperationResponse)
            }
            _ => {
                Box::pin(async { Ok(Status::unimplemented("unknown operator method").into_http()) })
            }
        }
    }
}
