//! Physical memory of the l4h host, read from the control plane and dom0.
//! Sampling sends no request to a workload VM.
use crate::{backend::LocalDom0Backend, extent::ctl_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use vmm_core::{Error, Result};
use vmm_l4hctl::VmState;

/// Bytes of host memory.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Dom0MemorySummary {
    /// Free memory that no guest used after boot.
    pub clean_bytes: u64,
    /// Free memory that a guest released and the hypervisor still backs.
    pub free_bytes: u64,
    /// Free memory that `l4h-memd` returned to the hypervisor.
    pub inflated_bytes: u64,
    /// Boot memory and plugged memory of dom0 and all other VMs.
    pub guest_plugged_bytes: u64,
    pub dom0_limit_bytes: u64,
    pub dom0_used_bytes: u64,
    /// In environment ID order.
    pub environments: Vec<EnvironmentMemory>,
}

/// The running workload VMs of one environment, added.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentMemory {
    pub environment_id: String,
    pub boot_bytes: u64,
    /// Boot memory and the hotplug target.
    pub grant_bytes: u64,
    /// Boot memory and plugged hotplug memory.
    pub plugged_bytes: u64,
}

impl LocalDom0Backend {
    pub fn memory_summary(&self) -> Result<Dom0MemorySummary> {
        let (host_free, dom0, vms, memory) = {
            let ctl = self.ctl.lock().map_err(|_| Error::Interrupted)?;
            let vms = ctl.list_vms().map_err(ctl_error)?;
            // A VM can stop between the list and its memory call.
            let memory = vms
                .iter()
                .filter(|vm| vm.id != 0)
                .filter_map(|vm| Some((vm.id, ctl.vm_mem_info(vm.id).ok()?)))
                .collect::<BTreeMap<_, _>>();
            (
                ctl.host_mem_info().map_err(ctl_error)?.0,
                ctl.vm_mem_info(0).map_err(ctl_error)?,
                vms,
                memory,
            )
        };
        let dom0_limit_bytes = dom0.boot_bytes + dom0.plugged_bytes;
        let mut environments = BTreeMap::<String, EnvironmentMemory>::new();
        for boot in self.catalog().boots()? {
            if boot.retiring || boot.retired || boot.helper {
                continue;
            }
            let Ok(Some(vm)) = self.present(&boot, &vms) else {
                continue;
            };
            if vm.state != VmState::Running {
                continue;
            }
            let Some(info) = memory.get(&vm.id) else {
                continue;
            };
            let id = boot.identity.sandbox.scope_id;
            let row = environments
                .entry(id.clone())
                .or_insert_with(|| EnvironmentMemory {
                    environment_id: id,
                    ..Default::default()
                });
            row.boot_bytes += info.boot_bytes;
            row.grant_bytes += info.boot_bytes + info.target_bytes;
            row.plugged_bytes += info.boot_bytes + info.plugged_bytes;
        }
        Ok(Dom0MemorySummary {
            clean_bytes: dom0.clean_bytes,
            // The host counts clean and inflated memory as free.
            free_bytes: host_free.saturating_sub(dom0.clean_bytes + dom0.reported_bytes),
            inflated_bytes: dom0.reported_bytes,
            guest_plugged_bytes: dom0_limit_bytes
                + memory
                    .values()
                    .map(|vm| vm.boot_bytes + vm.plugged_bytes)
                    .sum::<u64>(),
            dom0_limit_bytes,
            dom0_used_bytes: dom0_used_bytes(&std::fs::read_to_string(&self.cfg.meminfo)?)?,
            environments: environments.into_values().collect(),
        })
    }
}

/// `MemTotal - MemAvailable` of a Linux `/proc/meminfo` text.
fn dom0_used_bytes(meminfo: &str) -> Result<u64> {
    let field = |name: &str| {
        meminfo
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
            .and_then(|value| value.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())
            .ok_or(Error::Invalid("dom0 memory information"))
    };
    Ok(field("MemTotal")?.saturating_sub(field("MemAvailable")?) << 10)
}
