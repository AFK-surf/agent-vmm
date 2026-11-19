//! The state that the RPC server acts on.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use vmm_guest_proto::Secret;
use vmm_guest_proto::auth::boot_token_proof;
use vmm_guest_proto::rpc::{
    ErrorCode, HealthReply, HealthRequest, PowerOffReply, PowerOffRequest, RpcError,
};

use crate::health::Sampler;
use crate::process_manager::ProcessManager;
use crate::workspace_server::WorkspaceServer;

/// The machine operations of `PowerOff`.
pub trait System: Send + Sync {
    /// Trims, syncs, and unmounts the filesystems. Runs after each workload process stopped.
    fn prepare_power_off(&self) -> PowerOffReply;
    /// Powers the machine off. Runs after the reply was sent.
    fn power_off(&self);
}

pub struct Runtime {
    pub instance_id: String,
    pub token: Secret,
    pub processes: ProcessManager,
    pub workspace: WorkspaceServer,
    pub sampler: Sampler,
    pub system: Box<dyn System>,
    started: Instant,
    powering_off: AtomicBool,
}

impl Runtime {
    pub fn new(
        instance_id: String,
        token: Secret,
        processes: ProcessManager,
        workspace: WorkspaceServer,
        sampler: Sampler,
        system: Box<dyn System>,
    ) -> Self {
        Self {
            instance_id,
            token,
            processes,
            workspace,
            sampler,
            system,
            started: Instant::now(),
            powering_off: AtomicBool::new(false),
        }
    }

    pub fn health(&self, request: &HealthRequest) -> HealthReply {
        HealthReply {
            instance_id: self.instance_id.clone(),
            boot_token_proof: boot_token_proof(&self.token, &self.instance_id, &request.nonce),
            main: self.processes.main.state(),
            quiesced: self.processes.is_quiesced(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            orphans_reaped: self.processes.reaper().orphans_reaped(),
            usage: self.sampler.sample(),
        }
    }

    /// Stops the workload and prepares the filesystems. The caller sends the
    /// reply, then calls [`System::power_off`].
    pub fn prepare_power_off(&self, request: &PowerOffRequest) -> Result<PowerOffReply, RpcError> {
        if self.powering_off.swap(true, Ordering::SeqCst) {
            return Err(RpcError::new(
                ErrorCode::FailedPrecondition,
                "power off is in progress",
            ));
        }
        self.processes
            .stop_all(Duration::from_millis(request.grace_ms.into()));
        Ok(self.system.prepare_power_off())
    }
}
