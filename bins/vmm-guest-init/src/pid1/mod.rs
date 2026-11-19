//! Boot sequence of PID 1: kernel filesystems, network, boot configuration,
//! disks, the workload root, then the workload and the RPC server.

mod bootfetch;
mod cmdline;
mod dm;
mod mounts;
mod net;
mod shutdown;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ffi::CString;
use std::fs::{self, File};
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

use vmm_guest_proto::boot::{BootConfig, SLOT_PREFIX_LEN};

use self::shutdown::Machine;
use crate::health::Sampler;
use crate::process_manager::ProcessManager;
use crate::rpc_server::Server;
use crate::runtime::Runtime;
use crate::spawn::SpawnContext;
use crate::workspace_server::WorkspaceServer;

const INTERFACE: &str = "eth0";
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const WORKLOAD_CGROUP: &str = "/sys/fs/cgroup/workload";

/// Runs the machine. Never returns: a failure powers the VM off.
pub fn run() -> ! {
    match boot() {
        Ok(never) => match never {},
        Err(message) => shutdown::fail(&message),
    }
}

/// Gives PID 1 the console. The kernel passes no open descriptor when the
/// initramfs has no `/dev/console` node.
fn attach_console() {
    if let Ok(console) = File::options().read(true).write(true).open("/dev/console") {
        for descriptor in 0..=2 {
            // SAFETY: `dup2` takes no pointers. Descriptors 0 to 2 are reserved in `boot`.
            unsafe { libc::dup2(console.as_raw_fd(), descriptor) };
        }
    }
}

/// Creates the workload cgroup and returns its `cgroup.procs` path.
fn workload_cgroup(pids_max: Option<u64>) -> Result<CString, String> {
    // A controller that the kernel lacks must not hide the others.
    for controller in ["+cpu", "+memory", "+pids", "+io"] {
        let _ = fs::write(format!("{CGROUP_ROOT}/cgroup.subtree_control"), controller);
    }
    fs::create_dir_all(WORKLOAD_CGROUP).map_err(|error| format!("create the cgroup: {error}"))?;
    if let Some(pids_max) = pids_max {
        fs::write(format!("{WORKLOAD_CGROUP}/pids.max"), pids_max.to_string())
            .map_err(|error| format!("set pids.max: {error}"))?;
    }
    Ok(CString::new(format!("{WORKLOAD_CGROUP}/cgroup.procs")).expect("the path has no NUL byte"))
}

fn boot() -> Result<Infallible, String> {
    // Keeps later descriptors out of the standard numbers until the console is attached.
    while let Ok(placeholder) = File::open("/") {
        if placeholder.as_raw_fd() > 2 {
            break;
        }
        std::mem::forget(placeholder);
    }
    mounts::kernel_filesystems()?;
    attach_console();

    let text = fs::read_to_string("/proc/cmdline").map_err(|error| error.to_string())?;
    let cmdline = cmdline::parse(&text)?;
    let network = |what: &str, result: std::io::Result<()>| {
        result.map_err(|error| format!("configure {what}: {error}"))
    };
    network("lo", net::configure("lo", None))?;
    network(
        INTERFACE,
        net::configure(INTERFACE, Some((cmdline.address, SLOT_PREFIX_LEN))),
    )?;

    let config: BootConfig =
        bootfetch::fetch(vmm_guest_proto::boot::DEFAULT_GATEWAY, cmdline.boot_port)?;
    let address = config.network.address;
    if address != cmdline.address {
        return Err(format!(
            "the boot configuration is for {address}, not this slot"
        ));
    }
    let prefix_len = config.network.prefix_len;
    network(
        INTERFACE,
        net::configure(INTERFACE, Some((address, prefix_len))),
    )?;
    let gateway = config.network.gateway;
    network(
        "the default route",
        net::add_default_route(INTERFACE, address, prefix_len, gateway),
    )?;
    if let Some(hostname) = &config.network.hostname {
        network("the hostname", net::set_hostname(hostname))?;
    }

    let (root, assembled) = mounts::assemble(&config)?;
    mounts::enter_root(&root)?;
    let cgroup_procs = workload_cgroup(config.workload.pids_max)?;

    // The listener exists before any workload process can take the port.
    let listener = TcpListener::bind(SocketAddr::from((address, config.rpc_port)))
        .map_err(|error| format!("listen for RPC: {error}"))?;
    let context = SpawnContext {
        etc_dir: "/etc".into(),
        cgroup_procs: Some(cgroup_procs),
        base_env: config.workload.env.clone(),
        default_workdir: config
            .workload
            .workdir
            .clone()
            .unwrap_or_else(|| "/".into()),
        default_user: config.workload.user.clone(),
    };
    let volumes: BTreeMap<String, PathBuf> = config
        .volumes
        .iter()
        .map(|volume| (volume.name.clone(), PathBuf::from(&volume.mount_path)))
        .collect();
    let sampler = Sampler {
        cgroup_dir: Some(WORKLOAD_CGROUP.into()),
        network_statistics_dir: Some(format!("/sys/class/net/{INTERFACE}/statistics").into()),
        overlay_path: Some("/".into()),
        volume_paths: volumes.clone().into_iter().collect(),
    };
    let processes = ProcessManager::new(context, config.workload.log_limit_bytes as usize);
    let argv = config.workload.argv();
    let runtime = Arc::new(Runtime::new(
        config.instance_id.clone(),
        config.rpc_auth_token.clone(),
        processes,
        WorkspaceServer::new(config.workspace_path.as_ref().map(PathBuf::from), volumes),
        sampler,
        Box::new(Machine::new(assembled)),
    ));
    // The keys and the token copy of the configuration are wiped here.
    drop(config);

    if !argv.is_empty() {
        runtime.processes.start_main(argv);
    }
    let error = Server::new(runtime, Some(IpAddr::V4(gateway))).serve(listener);
    Err(format!("the RPC listener failed: {error}"))
}
