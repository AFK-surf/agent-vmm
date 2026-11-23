use super::{Call, CallError, MemorySummary, OperatorBackend};
use agent_vmm_proto::{
    appliance::v1::{self as a, appliance_admin_service_client::ApplianceAdminServiceClient},
    host::v1::{self as h, operator_service_client::OperatorServiceClient},
};
use futures::{
    FutureExt, StreamExt,
    channel::{mpsc, oneshot},
    future::BoxFuture,
    stream::BoxStream,
};
use std::path::PathBuf;

/// The operator socket of the Host: `AGENT_VMM_HOST_SOCKET`, or the socket in
/// the Host's application data directory.
pub fn default_socket_path() -> PathBuf {
    if let Some(path) = std::env::var_os("AGENT_VMM_HOST_SOCKET").filter(|path| !path.is_empty()) {
        return path.into();
    }
    let home = std::env::var_os("HOME").unwrap_or_default();
    PathBuf::from(home).join("Library/Application Support/AgentVMM/operator.sock")
}

type Job = BoxFuture<'static, ()>;

/// Calls the Host over its operator socket. The RPC client needs a Tokio
/// reactor, which runs on one thread of its own; answers come back through
/// channels the UI executor can await.
pub struct UdsBackend {
    socket: PathBuf,
    operator: OperatorServiceClient,
    appliances: ApplianceAdminServiceClient,
    jobs: tokio::sync::mpsc::UnboundedSender<Job>,
}

impl UdsBackend {
    pub fn new(socket: PathBuf) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        let (jobs, mut queue) = tokio::sync::mpsc::unbounded_channel::<Job>();
        std::thread::Builder::new()
            .name("operator-rpc".into())
            .spawn(move || {
                runtime.block_on(async move {
                    while let Some(job) = queue.recv().await {
                        tokio::spawn(job);
                    }
                });
            })?;
        let channel = vmm_rpc::Client::unix(&socket);
        Ok(Self {
            socket,
            operator: OperatorServiceClient::new(channel.clone()),
            appliances: ApplianceAdminServiceClient::new(channel),
            jobs,
        })
    }

    fn run<T: Send + 'static>(
        &self,
        call: impl Future<Output = Result<T, vmm_rpc::Status>> + Send + 'static,
    ) -> Call<T> {
        let (answer, answered) = oneshot::channel();
        let job = async move {
            let _ = answer.send(call.await.map_err(CallError::from));
        };
        let sent = self.jobs.send(job.boxed()).is_ok();
        async move {
            let stopped = || CallError {
                code: vmm_rpc::Code::Unavailable,
                message: "the RPC thread stopped".into(),
            };
            if !sent {
                return Err(stopped());
            }
            answered.await.map_err(|_| stopped())?
        }
        .boxed()
    }
}

/// Calls one unary operator method and takes `$field` out of its response.
macro_rules! unary {
    ($self:ident . $client:ident . $method:ident ( $request:expr ) => $pick:expr) => {{
        let mut client = $self.$client.clone();
        let request = $request;
        $self.run(async move {
            let response = client.$method(request).await?.into_inner();
            #[allow(clippy::redundant_closure_call)]
            ($pick)(response)
        })
    }};
}

fn required<T>(value: Option<T>, what: &'static str) -> Result<T, vmm_rpc::Status> {
    value.ok_or_else(|| vmm_rpc::Status::data_loss(format!("the response has no {what}")))
}

impl OperatorBackend for UdsBackend {
    fn endpoint(&self) -> String {
        self.socket.display().to_string()
    }

    fn vm(&self) -> Call<h::Vm> {
        unary!(self.operator.get_vm(h::GetVmRequest {})
            => |r: h::GetVmResponse| required(r.vm, "VM"))
    }

    fn health(&self) -> Call<h::GetSystemHealthSummaryResponse> {
        unary!(self.operator.get_system_health_summary(h::GetSystemHealthSummaryRequest {}) => Ok)
    }

    fn disk(&self) -> Call<h::GetLocalDiskSummaryResponse> {
        unary!(self.operator.get_local_disk_summary(h::GetLocalDiskSummaryRequest {}) => Ok)
    }

    fn memory(&self) -> Call<MemorySummary> {
        futures::future::ready(Err(CallError::unimplemented(
            "this Host does not serve GetMemorySummary",
        )))
        .boxed()
    }

    fn vm_impact(&self) -> Call<h::GetVmImpactSummaryResponse> {
        unary!(self.operator.get_vm_impact_summary(h::GetVmImpactSummaryRequest {}) => Ok)
    }

    fn start_vm(&self, request_id: String) -> Call<h::Operation> {
        let request = h::StartVmRequest {
            request_id,
            fence: None,
        };
        unary!(self.operator.start_vm(request)
            => |r: h::StartVmResponse| required(r.operation, "operation"))
    }

    fn stop_vm(&self, request_id: String, fence: h::VmMutationFence) -> Call<h::Operation> {
        let request = h::StopVmRequest {
            request_id,
            force: false,
            fence: Some(fence),
        };
        unary!(self.operator.stop_vm(request)
            => |r: h::StopVmResponse| required(r.operation, "operation"))
    }

    fn operation(&self, operation_id: String) -> Call<h::Operation> {
        unary!(self.operator.get_operation(h::GetOperationRequest { operation_id })
            => |r: h::GetOperationResponse| required(r.operation, "operation"))
    }

    fn environment_page(&self, after_id: String) -> Call<h::ListLocalEnvironmentPageResponse> {
        unary!(self.operator.list_local_environment_page(
            h::ListLocalEnvironmentPageRequest { after_id }) => Ok)
    }

    fn environment(&self, environment_id: String) -> Call<h::AgentEnvironment> {
        let request = h::OperatorServiceGetEnvironmentRequest { environment_id };
        unary!(self.operator.get_environment(request)
            => |r: h::OperatorServiceGetEnvironmentResponse| required(r.environment, "environment"))
    }

    fn plan_cleanup(&self, kind: h::CleanupKind, environment_id: String) -> Call<h::CleanupPlan> {
        if kind == h::CleanupKind::Discard {
            unary!(self.operator.plan_discard_environment(
                h::PlanDiscardEnvironmentRequest { environment_id })
                => |r: h::PlanDiscardEnvironmentResponse| required(r.plan, "plan"))
        } else {
            unary!(self.operator.plan_reset_environment(
                h::PlanResetEnvironmentRequest { environment_id })
                => |r: h::PlanResetEnvironmentResponse| required(r.plan, "plan"))
        }
    }

    fn execute_cleanup(&self, request_id: String, plan: h::CleanupPlan) -> Call<h::Operation> {
        let confirmed_volume_ids = super::irreversible_volume_ids(&plan);
        let request = h::ExecuteCleanupRequest {
            request_id,
            plan: Some(plan),
            confirmed_volume_ids,
        };
        unary!(self.operator.execute_cleanup(request)
            => |r: h::ExecuteCleanupResponse| required(r.operation, "operation"))
    }

    fn plan_disposal(&self, environment_id: String) -> Call<h::LocalDisposalPlan> {
        let request = h::PlanLocalDisposalRequest {
            target_kind: h::LocalDisposalTargetKind::Environment as i32,
            target_id: environment_id,
        };
        unary!(self.operator.plan_local_disposal(request)
            => |r: h::PlanLocalDisposalResponse| required(r.plan, "plan"))
    }

    fn execute_disposal(
        &self,
        request_id: String,
        plan: h::LocalDisposalPlan,
    ) -> Call<h::Operation> {
        let request = h::ExecuteLocalDisposalRequest {
            request_id,
            plan: Some(plan),
            irreversible_confirmation: true,
        };
        unary!(self.operator.execute_local_disposal(request)
            => |r: h::ExecuteLocalDisposalResponse| required(r.operation, "operation"))
    }

    fn logs(
        &self,
        request: h::OperatorServiceLogsRequest,
    ) -> BoxStream<'static, Result<h::OperatorServiceLogsResponse, CallError>> {
        let (records, received) = mpsc::unbounded();
        let mut client = self.operator.clone();
        // The stream ends when the view drops `received`: the next send fails
        // and the task drops the connection.
        let job = async move {
            let mut stream = match client.logs(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let _ = records.unbounded_send(Err(status.into()));
                    return;
                }
            };
            while let Some(record) = stream.next().await {
                if records
                    .unbounded_send(record.map_err(CallError::from))
                    .is_err()
                {
                    return;
                }
            }
        };
        let _ = self.jobs.send(job.boxed());
        received.boxed()
    }

    fn device_identity(&self) -> Call<h::GetDeviceIdentityResponse> {
        unary!(self.operator.get_device_identity(h::GetDeviceIdentityRequest {}) => Ok)
    }

    fn registration_page(&self, after_id: String) -> Call<h::ListLocalRegistrationPageResponse> {
        unary!(self.operator.list_local_registration_page(
            h::ListLocalRegistrationPageRequest { after_id }) => Ok)
    }

    fn host_sockets(&self) -> Call<Vec<h::HostSocket>> {
        unary!(self.operator.list_host_sockets(h::ListHostSocketsRequest::default())
            => |r: h::ListHostSocketsResponse| Ok(r.host_sockets))
    }

    fn appliances(&self) -> Call<Vec<a::ApplianceStatus>> {
        unary!(self.appliances.list_appliances(a::ListAppliancesRequest {})
            => |r: a::ListAppliancesResponse| Ok(r.appliances))
    }

    fn appliance_status(&self, installation_id: String) -> Call<a::ApplianceStatus> {
        unary!(self.appliances.get_appliance_status(
            a::GetApplianceStatusRequest { installation_id })
            => |r: a::GetApplianceStatusResponse| required(r.status, "status"))
    }

    fn create_mcp_token(&self) -> Call<String> {
        unary!(self.operator.create_mcp_token(h::CreateMcpTokenRequest {})
            => |r: h::CreateMcpTokenResponse| Ok(r.token))
    }

    fn rotate_mcp_token_key(&self) -> Call<String> {
        unary!(self.operator.rotate_mcp_token_key(h::RotateMcpTokenKeyRequest {})
            => |r: h::RotateMcpTokenKeyResponse| Ok(r.operation_id))
    }
}
