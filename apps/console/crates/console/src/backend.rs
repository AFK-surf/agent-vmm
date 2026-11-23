//! The Host operator API as the console uses it.
//!
//! [`UdsBackend`] calls the Host over its operator socket. [`FixtureBackend`]
//! answers from a fixed data set, so offscreen renders and tests are
//! deterministic.

mod fixture;
mod uds;

use agent_vmm_proto::{appliance::v1 as a, host::v1 as h};
use futures::{future::BoxFuture, stream::BoxStream};
use prost_types::Timestamp;

pub use fixture::FixtureBackend;
pub use uds::{UdsBackend, default_socket_path};

/// The result of one operator call.
pub type Call<T> = BoxFuture<'static, Result<T, CallError>>;

/// A failed operator call: the RPC status, or a transport failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallError {
    pub code: vmm_rpc::Code,
    pub message: String,
}

impl CallError {
    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self {
            code: vmm_rpc::Code::Unimplemented,
            message: message.into(),
        }
    }

    pub fn is_unimplemented(&self) -> bool {
        self.code == vmm_rpc::Code::Unimplemented
    }
}

impl From<vmm_rpc::Status> for CallError {
    fn from(status: vmm_rpc::Status) -> Self {
        Self {
            code: status.code,
            message: status.message,
        }
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

/// Physical memory by layer, from the carrier process down to each workload VM.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemorySummary {
    pub sampled_at: Option<Timestamp>,
    /// `phys_footprint` of the carrier process: what macOS charges for the VM.
    pub carrier_footprint_bytes: u64,
    /// Guest physical memory `l4h-memd` manages, by block state.
    pub pool: MemoryPool,
    pub dom0_limit_bytes: u64,
    pub dom0_used_bytes: u64,
    pub environments: Vec<EnvironmentMemory>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryPool {
    /// Handed over at boot and never allocated: not resident on the host.
    pub clean_bytes: u64,
    /// Released by a guest and kept for reuse: resident.
    pub free_bytes: u64,
    /// Returned to macOS through the balloon: not resident.
    pub inflated_bytes: u64,
    /// Plugged into dom0 and the workload VMs.
    pub guest_plugged_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnvironmentMemory {
    pub environment_id: String,
    pub boot_bytes: u64,
    pub grant_bytes: u64,
    pub plugged_bytes: u64,
    pub zram_bytes: u64,
    /// How often the pool refused a hot-plug request of this VM.
    pub ebusy_count: u64,
}

pub trait OperatorBackend: Send + Sync + 'static {
    /// Where the calls go, as shown to the operator.
    fn endpoint(&self) -> String;

    fn vm(&self) -> Call<h::Vm>;
    fn health(&self) -> Call<h::GetSystemHealthSummaryResponse>;
    fn disk(&self) -> Call<h::GetLocalDiskSummaryResponse>;
    /// A Host without `GetMemorySummary` answers `Unimplemented`.
    fn memory(&self) -> Call<MemorySummary>;
    fn vm_impact(&self) -> Call<h::GetVmImpactSummaryResponse>;
    fn start_vm(&self, request_id: String) -> Call<h::Operation>;
    fn stop_vm(&self, request_id: String, fence: h::VmMutationFence) -> Call<h::Operation>;
    fn operation(&self, operation_id: String) -> Call<h::Operation>;

    fn environment_page(&self, after_id: String) -> Call<h::ListLocalEnvironmentPageResponse>;
    fn environment(&self, environment_id: String) -> Call<h::AgentEnvironment>;
    fn plan_cleanup(&self, kind: h::CleanupKind, environment_id: String) -> Call<h::CleanupPlan>;
    fn execute_cleanup(&self, request_id: String, plan: h::CleanupPlan) -> Call<h::Operation>;
    fn plan_disposal(&self, environment_id: String) -> Call<h::LocalDisposalPlan>;
    fn execute_disposal(
        &self,
        request_id: String,
        plan: h::LocalDisposalPlan,
    ) -> Call<h::Operation>;

    fn logs(
        &self,
        request: h::OperatorServiceLogsRequest,
    ) -> BoxStream<'static, Result<h::OperatorServiceLogsResponse, CallError>>;

    fn device_identity(&self) -> Call<h::GetDeviceIdentityResponse>;
    fn registration_page(&self, after_id: String) -> Call<h::ListLocalRegistrationPageResponse>;
    fn host_sockets(&self) -> Call<Vec<h::HostSocket>>;
    fn appliances(&self) -> Call<Vec<a::ApplianceStatus>>;
    fn appliance_status(&self, installation_id: String) -> Call<a::ApplianceStatus>;
    fn create_mcp_token(&self) -> Call<String>;
    /// Answers the ID of the rotation operation.
    fn rotate_mcp_token_key(&self) -> Call<String>;
}

/// The volumes of `plan` whose deletion the operator confirms with the plan.
fn irreversible_volume_ids(plan: &h::CleanupPlan) -> Vec<String> {
    plan.impacts
        .iter()
        .filter(|impact| impact.irreversible)
        .filter_map(|impact| impact.resource.as_ref())
        .filter(|resource| resource.kind == "volume")
        .map(|resource| resource.resource_id.clone())
        .collect()
}
