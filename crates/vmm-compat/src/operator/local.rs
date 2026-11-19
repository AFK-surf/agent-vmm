//! The local management socket stays available while the runtime backend is offline.
use super::{DiskSample, LocalDisposals, LocalInventory, RegistrationControl, h};
use std::{collections::HashSet, sync::Arc, time::SystemTime};
use tokio::sync::Notify;
use vmm_core::{Core, Store, backend::Dom0Backend};
use vmm_rpc::{Request, Response, Status};

pub struct LocalOperator {
    pub(super) inventory: LocalInventory,
    registrations: Arc<dyn RegistrationControl>,
    disposals: LocalDisposals,
    pub backend: Option<Arc<dyn Dom0Backend>>,
    pub containers: Option<crate::Containers>,
    pub guest: Option<Arc<dyn crate::guest::GuestDriver>>,
    pub vm_control: Option<Arc<dyn super::VmControl>>,
    pub(super) vm_ops: Arc<super::vm::VmOperations>,
    pub management: Option<Arc<dyn super::RegistrationManagement>>,
    pub trust: Option<Arc<dyn super::DeviceTrust>>,
    pub(super) wake: Notify,
    pub appliances: Option<super::ApplianceOperations>,
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
            vm_ops: Arc::new(super::vm::VmOperations::open(path)?),
            vm_control: backend.clone().map(|b| {
                Arc::new(super::vm::NativeVmControl(Core::new(store.clone(), b)))
                    as Arc<dyn super::VmControl>
            }),
            disposals: LocalDisposals::open(path, store.clone(), registrations.clone())?,
            inventory: LocalInventory::new(store),
            registrations,
            backend,
            containers: None,
            guest: None,
            management: None,
            trust: None,
            wake: Notify::new(),
            mcp: None,
            appliances: None,
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
        let mut cleanup_cursor = String::new();
        loop {
            let _ = core.project_host_sockets().await;
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
            if let Ok(page) = core.store.pending_cleanups(&cleanup_cursor) {
                for id in &page {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(120),
                        core.drive_cleanup(id),
                    )
                    .await;
                }
                cleanup_cursor = page
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

    async fn vm(&self, _: h::GetVmRequest) -> Result<h::GetVmResponse, Status> {
        Ok(h::GetVmResponse {
            vm: Some(self.snapshot().await?),
        })
    }
    async fn impact(
        &self,
        _: h::GetVmImpactSummaryRequest,
    ) -> Result<h::GetVmImpactSummaryResponse, Status> {
        let vm = self.vm(h::GetVmRequest {}).await?.vm;
        Ok(self.inventory.impact(vm))
    }
    async fn health(
        &self,
        _: h::GetSystemHealthSummaryRequest,
    ) -> Result<h::GetSystemHealthSummaryResponse, Status> {
        let now = Some(SystemTime::now().into());
        let vm = self.snapshot().await.unwrap_or_else(|err| h::Vm {
            state: h::VmState::Stopped as i32,
            error: err.message().to_owned(),
            ..Default::default()
        });
        let (state, reason) = self.backend_health(&vm).await;
        Ok(h::GetSystemHealthSummaryResponse {
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
            vm: Some(vm),
            ..Default::default()
        })
    }
    async fn backend_health<'a>(&self, vm: &'a h::Vm) -> (h::HealthState, &'a str) {
        match &self.backend {
            _ if vm.state() != h::VmState::Running => (
                h::HealthState::Unavailable,
                if vm.error.is_empty() {
                    "VM is not running"
                } else {
                    vm.error.as_str()
                },
            ),
            None => (h::HealthState::Unavailable, "backend offline"),
            Some(b) => {
                match tokio::time::timeout(std::time::Duration::from_secs(4), b.list_live_vms())
                    .await
                {
                    Ok(Ok(_)) => (h::HealthState::Healthy, ""),
                    _ => (h::HealthState::Degraded, "backend observation unavailable"),
                }
            }
        }
    }

    pub async fn remote_observation(&self) -> (h::HealthState, Option<DiskSample>, bool) {
        let configured = self.disk_sample.is_some();
        let Ok(vm) = self.snapshot().await else {
            return (h::HealthState::Unavailable, None, configured);
        };
        let ((health, _), disk) =
            tokio::join!(self.backend_health(&vm), self.sample_disk(vm.state()));
        (health, disk, configured)
    }

    async fn environments(
        &self,
        req: h::ListLocalEnvironmentPageRequest,
    ) -> Result<h::ListLocalEnvironmentPageResponse, Status> {
        let vm_state = self
            .snapshot()
            .await
            .map_or(h::VmState::Stopped, |vm| vm.state());
        let mut active = HashSet::new();
        if vm_state == h::VmState::Running
            && let Some(b) = &self.backend
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
        self.inventory.environments(req, vm_state, &active)
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
        let vm_state = self
            .snapshot()
            .await
            .map_or(h::VmState::Stopped, |vm| vm.state());
        Ok(self
            .inventory
            .disk(vm_state, self.sample_disk(vm_state).await))
    }

    async fn sample_disk(&self, vm_state: h::VmState) -> Option<DiskSample> {
        if vm_state == h::VmState::Running
            && let Some(sample) = self.disk_sample.clone()
        {
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
        }
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
        if let Some(op) = self
            .inventory
            .store
            .cleanup_operation(&req.operation_id)
            .map_err(crate::error::core_status)?
        {
            return Ok(h::GetOperationResponse {
                operation: Some(super::cleanup::operation(op)),
            });
        }
        Ok(h::GetOperationResponse {
            operation: Some(match self.vm_ops.operation(&req.operation_id)? {
                Some(op) => op,
                None => self.disposals.operation(&req.operation_id)?,
            }),
        })
    }
}

// Dispatch the local operator methods on their existing protobuf service paths.
#[derive(Clone)]
pub struct LocalOperatorServer(pub Arc<LocalOperator>);

#[vmm_rpc::async_trait]
impl vmm_rpc::Service for LocalOperatorServer {
    fn name(&self) -> &'static str {
        "agentvmm.host.v1.OperatorService"
    }
    async fn call(
        &self,
        method: &str,
        req: Request<vmm_rpc::Body>,
    ) -> Result<vmm_rpc::Body, Status> {
        match method {
            "GetRemoteRegistration" => Ok(Response::new(
                self.0
                    .get_registration(
                        req.unary::<h::GetRemoteRegistrationRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "ApplyRemoteRegistration" => Ok(Response::new(
                self.0
                    .apply_registration(
                        req.unary::<h::ApplyRemoteRegistrationRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "SetRemoteRegistrationState" => Ok(Response::new(
                self.0
                    .set_registration(
                        req.unary::<h::SetRemoteRegistrationStateRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "BeginRemoteRevocation" => Ok(Response::new(
                self.0
                    .begin_revocation(
                        req.unary::<h::BeginRemoteRevocationRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "CompleteRemoteRevocation" => Ok(Response::new(
                self.0
                    .complete_revocation(
                        req.unary::<h::CompleteRemoteRevocationRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "GetDeviceIdentity" => Ok(Response::new(
                self.0
                    .device_identity(
                        req.unary::<h::GetDeviceIdentityRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "SignRegistrationRecovery" => Ok(Response::new(
                self.0
                    .sign_recovery(
                        req.unary::<h::SignRegistrationRecoveryRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "SetHostSocket" => Ok(Response::new(
                self.0
                    .set_socket(req.unary::<h::SetHostSocketRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "RemoveHostSocket" => Ok(Response::new(
                self.0
                    .remove_socket(
                        req.unary::<h::RemoveHostSocketRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "ListHostSockets" => Ok(Response::new(
                self.0
                    .list_sockets(req.unary::<h::ListHostSocketsRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "CreateMCPToken" => Ok(Response::new(
                self.0
                    .issue_token(req.unary::<h::CreateMcpTokenRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "RotateMCPTokenKey" => Ok(Response::new(
                self.0
                    .rotate_key(
                        req.unary::<h::RotateMcpTokenKeyRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "GetVMImpactSummary" => Ok(Response::new(
                self.0
                    .impact(
                        req.unary::<h::GetVmImpactSummaryRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "PlanResetEnvironment" => Ok(Response::new(
                self.0
                    .plan_reset(
                        req.unary::<h::PlanResetEnvironmentRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "PlanDiscardEnvironment" => Ok(Response::new(
                self.0
                    .plan_discard(
                        req.unary::<h::PlanDiscardEnvironmentRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "ExecuteCleanup" => Ok(Response::new(
                self.0
                    .execute_cleanup(req.unary::<h::ExecuteCleanupRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "GetEnvironment" => Ok(Response::new(
                self.0
                    .environment(
                        req.unary::<h::OperatorServiceGetEnvironmentRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "StartVM" => Ok(Response::new(
                self.0
                    .start_vm(req.unary::<h::StartVmRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "StopVM" => Ok(Response::new(
                self.0
                    .stop_vm(req.unary::<h::StopVmRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "GetVM" => Ok(Response::new(
                self.0
                    .vm(req.unary::<h::GetVmRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "GetSystemHealthSummary" => Ok(Response::new(
                self.0
                    .health(
                        req.unary::<h::GetSystemHealthSummaryRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "GetLocalDiskSummary" => Ok(Response::new(
                self.0
                    .disk(
                        req.unary::<h::GetLocalDiskSummaryRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "ListLocalEnvironmentPage" => Ok(Response::new(
                self.0
                    .environments(
                        req.unary::<h::ListLocalEnvironmentPageRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "ListLocalRegistrationPage" => Ok(Response::new(
                self.0
                    .registrations(
                        req.unary::<h::ListLocalRegistrationPageRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "PlanLocalDisposal" => Ok(Response::new(
                self.0
                    .plan(
                        req.unary::<h::PlanLocalDisposalRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "ExecuteLocalDisposal" => Ok(Response::new(
                self.0
                    .execute(
                        req.unary::<h::ExecuteLocalDisposalRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "GetOperation" => Ok(Response::new(
                self.0
                    .operation(req.unary::<h::GetOperationRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "Logs" => super::logs::logs(&self.0, req.unary().await?.into_inner()).await,
            _ => Err(Status::unimplemented("unknown operator method")),
        }
    }
}
