use crate::{control, host::HostState};
use clap::Parser;
use std::{error::Error, fs::DirBuilder, os::unix::fs::DirBuilderExt, path::PathBuf};
use tokio::signal::unix::{SignalKind, signal};

/// Native host control process.
#[derive(Parser)]
struct Options {
    #[arg(long)]
    database: PathBuf,
    /// Attach the local dom0 backend using an already provisioned pool.
    #[cfg(target_os = "linux")]
    #[arg(long)]
    dom0_config: Option<PathBuf>,
    #[arg(long)]
    operator_socket: Option<PathBuf>,
    #[arg(long)]
    carrier_socket: Option<PathBuf>,
    #[arg(long)]
    session_directory: Option<PathBuf>,
    /// TCP port of the authenticated loopback MCP frontend.
    #[arg(long, default_value_t = 8788)]
    mcp_port: u16,
    #[arg(long)]
    remote_compute_socket: Option<PathBuf>,
    #[arg(long)]
    bundle: Option<PathBuf>,
    #[arg(long)]
    instance: Option<PathBuf>,
    #[arg(long)]
    serial_log: Option<PathBuf>,
    #[arg(long)]
    data_worker: Option<PathBuf>,
    #[arg(long)]
    state_size: Option<u64>,
    #[arg(long)]
    cpus: Option<u32>,
    #[arg(long)]
    memory: Option<u64>,
    #[arg(long)]
    trust_socket: Option<PathBuf>,
    #[arg(long)]
    service_tunnel_socket: Option<PathBuf>,
    #[arg(long)]
    mesh_control_socket: Option<PathBuf>,
    #[arg(long)]
    data_worker_socket: Option<PathBuf>,
    #[arg(long)]
    disable_personal_mesh_pairing: bool,
}

pub fn main_from_args<S: AsRef<std::ffi::OsStr>>(args: &[S]) -> i32 {
    let args = args.iter().map(AsRef::as_ref);
    if args.clone().eq(["version", "--json"]) {
        let release = option_env!("AGENT_VMM_RELEASE_ID").unwrap_or(env!("CARGO_PKG_VERSION"));
        println!(
            "{}",
            serde_json::json!({
                "contractVersion": 1, "component": "agent-vmm-host", "version": release,
                "release_id": release, "currentSchema": 2, "minimumSchema": 1
            })
        );
        return 0;
    }
    let options = match Options::try_parse_from(
        std::iter::once(std::ffi::OsStr::new("agent-vmm-host")).chain(args),
    ) {
        Ok(options) => options,
        Err(error) => {
            let code = error.exit_code();
            let _ = error.print();
            return code;
        }
    };
    match run(options) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("agent-vmm-host: {error}");
            1
        }
    }
}

#[tokio::main]
async fn run(options: Options) -> Result<(), Box<dyn Error>> {
    let directory = options
        .database
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)?;
    let mut host = HostState::open(&options.database)?;
    if let Some(size) = options.state_size {
        host.state_size = size;
    }
    let path = options
        .operator_socket
        .unwrap_or_else(|| directory.join("operator.sock"));
    let instance = options.instance.as_deref().unwrap_or(directory);
    let mut term = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let shutdown = async move {
        tokio::select! { _ = term.recv() => {}, _ = interrupt.recv() => {} }
    };
    #[cfg(target_os = "linux")]
    if let Some(config) = match options.dom0_config {
        Some(explicit) => Some(explicit),
        None => match options.instance.as_ref().map(|p| p.join("dom0.json")) {
            Some(path) if path.try_exists()? => Some(path),
            _ => None,
        },
    } {
        return run_local(host, &config, instance, &path, options.mcp_port, shutdown).await;
    }
    #[cfg(target_os = "macos")]
    if let Some(socket) = options.carrier_socket {
        let client = vmm_carrier_macos::client::CarrierClient::new(socket);
        let staging = std::sync::Arc::new(crate::carrier::CarrierImageStaging::new(
            client,
            host.staging_dir().to_path_buf(),
        ));
        return run_carrier(host, instance, &path, options.mcp_port, staging, shutdown).await;
    }
    let api = host.local_operator(instance, None)?;
    control::serve_local(&path, api, None, shutdown).await
}

#[cfg(target_os = "macos")]
async fn run_carrier(
    host: HostState,
    instance: &std::path::Path,
    path: &std::path::Path,
    mcp_port: u16,
    staging: std::sync::Arc<crate::carrier::CarrierImageStaging>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), Box<dyn Error>> {
    let client = staging.client.clone();
    let backend = std::sync::Arc::new(client.dom0());
    let core = vmm_core::Core::new(host.store.clone(), backend.clone());
    // The Carrier can launch after the Host. Operator registration and identity
    // stay available while its socket or the guest control relay is offline.
    let running = matches!( tokio::time::timeout(
        std::time::Duration::from_secs(4),
        client.control(vmm_carrier_macos::protocol::Request::Status),
    )
    .await,Ok(Ok(status)) if status.state == vmm_carrier_macos::protocol::State::Running);
    if running {
        core.reconcile_after_restart().await?;
    }
    let (mut api, registrations) = host.operator_parts(instance, None)?;
    api.backend = Some(backend);
    let adapter = std::sync::Arc::new(crate::carrier::CarrierClientAdapter(client.clone()));
    host.attach_guest(&mut api, adapter.clone())?;
    let (disk, memory) = (staging.clone(), staging.clone());
    api = api
        .with_disk_sample(move || disk.disk_sample())
        .with_memory_sample(move || memory.memory_sample());
    let (runtime, builds) = host.online_runtime(core.clone(), &api, adapter, staging)?;
    if running {
        builds.recover(&core.store).await?;
    }
    api.vm_control = Some(std::sync::Arc::new(crate::carrier::MacVmControl {
        client,
        core: core.clone(),
        builds,
    }));
    control::serve_online(
        path,
        api,
        registrations,
        runtime,
        mcp_port,
        host.state_size.saturating_sub(13 << 30),
        shutdown,
    )
    .await
}

#[cfg(all(test, target_os = "macos"))]
#[path = "carrier_disk_tests.rs"]
mod carrier_disk_tests;

#[cfg(target_os = "linux")]
async fn run_local(
    host: HostState,
    config: &std::path::Path,
    instance: &std::path::Path,
    operator_socket: &std::path::Path,
    mcp_port: u16,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), Box<dyn Error>> {
    let mut dom0 = vmm_dom0::local::open(config, false)
        .await
        .map_err(|err| -> Box<dyn Error> { err })?;
    let (core, report) = host.attach(dom0.backend.clone()).await?;
    if !report.mismatched.is_empty() || !report.unowned.is_empty() {
        eprintln!(
            "dom0 reconciliation found {} mismatched and {} unowned VM(s)",
            report.mismatched.len(),
            report.unowned.len(),
        );
    }
    let (api, registrations) = host.operator_parts(instance, Some(dom0.backend.clone()))?;
    let adapter = std::sync::Arc::new(crate::Dom0Workspaces(dom0.backend.clone()));
    let (runtime, builds) = host.online_runtime(core, &api, adapter.clone(), adapter)?;
    builds.recover(&host.store).await?;
    tokio::select! {
        result = control::serve_online(operator_socket,api,registrations,runtime,mcp_port,host.state_size.saturating_sub(13 << 30),shutdown) => result,
        result = dom0.wait() => match result {
            Err(err) => Err(err),
            Ok(()) => Err(std::io::Error::other("dom0 background service stopped").into()),
        },
    }
}
