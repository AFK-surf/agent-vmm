use agent_vmm_host::{control, host::HostState};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::args_os().skip(1).eq(["version", "--json"]) {
        let release = option_env!("AGENT_VMM_RELEASE_ID").unwrap_or(env!("CARGO_PKG_VERSION"));
        println!(
            "{}",
            serde_json::json!({
                "contractVersion": 1, "component": "agent-vmm-host", "version": release,
                "release_id": release, "currentSchema": 2, "minimumSchema": 1
            })
        );
        return Ok(());
    }
    let options = Options::parse();
    let directory = options
        .database
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)?;
    let host = HostState::open(&options.database)?;
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
    if let Some(config) = options.dom0_config {
        return run_local(host, &config, instance, &path, shutdown).await;
    }
    let api = host.local_operator(instance, None)?;
    control::serve_local(&path, api, None, shutdown).await
}

#[cfg(target_os = "linux")]
async fn run_local(
    host: HostState,
    config: &std::path::Path,
    instance: &std::path::Path,
    operator_socket: &std::path::Path,
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
    let api = host.local_operator(instance, Some(dom0.backend.clone()))?;
    tokio::select! {
        result = control::serve_local(operator_socket, api, Some(core), shutdown) => result,
        result = dom0.wait() => match result {
            Err(err) => Err(err),
            Ok(()) => Err(std::io::Error::other("dom0 background service stopped").into()),
        },
    }
}
