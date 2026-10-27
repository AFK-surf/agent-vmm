mod pairing_runtime;

use agent_vmm_data::{ALPNS, Accounting, Command, MAX_STREAMS, SERVICE_TUNNEL_ALPN, probe};
use anyhow::{Context, Result};
use iroh::{Endpoint, endpoint::presets};
use pairing_runtime::{PAIRING_INCOMING_LIMIT, PairingSessions};
use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Mutex, Semaphore},
};
use zeroize::Zeroizing;

#[tokio::main]
async fn main() -> Result<()> {
    let (socket, service_socket, mesh_control_socket) = socket_arguments()?;
    let host_exited = watch_lifeline();
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if socket.exists() {
        tokio::fs::remove_file(&socket).await?;
    }

    let endpoint = Endpoint::builder(presets::N0)
        .alpns(ALPNS.iter().map(|value| value.to_vec()).collect())
        .bind()
        .await
        .context("bind iroh endpoint")?;
    let generation = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
    let accounting = Arc::new(Accounting::default());
    let pairing_sessions: PairingSessions = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let semaphore = Arc::new(Semaphore::new(MAX_STREAMS));
    let listener = UnixListener::bind(&socket).context("bind local control socket")?;
    let socket_identity = file_identity(&socket).context("read local control socket identity")?;

    let ipc = tokio::spawn(serve_ipc(
        listener,
        endpoint.clone(),
        accounting.clone(),
        pairing_sessions.clone(),
        generation,
    ));
    let incoming = tokio::spawn(serve_incoming(
        endpoint.clone(),
        accounting.clone(),
        semaphore,
        service_socket,
        mesh_control_socket,
        pairing_sessions,
    ));
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = host_exited => {}
    }
    accounting.drain();
    endpoint.close().await;
    ipc.abort();
    incoming.abort();
    remove_owned_socket(&socket, socket_identity);
    Ok(())
}

fn file_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    let metadata = std::fs::symlink_metadata(path)?;
    Ok((metadata.dev(), metadata.ino()))
}

/// A replacement worker may already have bound the same path; remove only the
/// socket that this worker bound.
fn remove_owned_socket(path: &Path, identity: (u64, u64)) {
    if file_identity(path).is_ok_and(|current| current == identity) {
        let _ = std::fs::remove_file(path);
    }
}

/// The Host holds the write end of stdin. EOF means that the Host exited, so
/// the worker must not outlive it.
fn watch_lifeline() -> tokio::sync::oneshot::Receiver<()> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut byte = [0u8; 1];
        loop {
            match stdin.read(&mut byte) {
                Ok(0) => break,
                Err(error) if error.kind() != std::io::ErrorKind::Interrupted => break,
                _ => {}
            }
        }
        let _ = sender.send(());
    });
    receiver
}

fn socket_arguments() -> Result<(PathBuf, PathBuf, PathBuf)> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [
            control_flag,
            control,
            service_flag,
            service,
            mesh_flag,
            mesh,
        ] if control_flag == "--socket"
            && service_flag == "--service-socket"
            && mesh_flag == "--mesh-control-socket" =>
        {
            let control = PathBuf::from(control);
            let service = PathBuf::from(service);
            let mesh = PathBuf::from(mesh);
            ensure_absolute(&control, "control socket")?;
            ensure_absolute(&service, "service socket")?;
            ensure_absolute(&mesh, "mesh control socket")?;
            Ok((control, service, mesh))
        }
        _ => anyhow::bail!(
            "usage: agent-vmm-data --socket <absolute-path> --service-socket <absolute-path> --mesh-control-socket <absolute-path>"
        ),
    }
}

fn ensure_absolute(path: &std::path::Path, label: &str) -> Result<()> {
    anyhow::ensure!(path.is_absolute(), "{label} must be absolute");
    Ok(())
}

async fn serve_ipc(
    listener: UnixListener,
    endpoint: Endpoint,
    accounting: Arc<Accounting>,
    pairing_sessions: PairingSessions,
    generation: u64,
) {
    while let Ok((stream, _)) = listener.accept().await {
        let accounting = accounting.clone();
        let endpoint = endpoint.clone();
        let pairing_sessions = pairing_sessions.clone();
        tokio::spawn(async move {
            let _ = handle_ipc(stream, endpoint, accounting, pairing_sessions, generation).await;
        });
    }
}

async fn handle_ipc(
    mut stream: UnixStream,
    endpoint: Endpoint,
    accounting: Arc<Accounting>,
    pairing_sessions: PairingSessions,
    generation: u64,
) -> Result<()> {
    let mut input = Zeroizing::new(Vec::with_capacity(1024));
    {
        let mut reader = BufReader::new(&mut stream).take(4097);
        reader.read_until(b'\n', &mut input).await?;
    }
    anyhow::ensure!(
        input.ends_with(b"\n"),
        "local command must end with newline"
    );
    let response = match serde_json::from_slice::<Command>(&input)? {
        Command::Status => Some(serde_json::to_vec(
            &accounting.status(endpoint.addr(), generation),
        )?),
        Command::Drain => {
            accounting.drain();
            Some(serde_json::to_vec(
                &accounting.status(endpoint.addr(), generation),
            )?)
        }
        Command::Probe {
            endpoint_addr,
            payload,
        } => {
            let echoed = probe(&endpoint, endpoint_addr, payload.as_bytes()).await?;
            accounting.add_out(payload.len() as u64);
            accounting.add_in(echoed.len() as u64);
            let mut status = accounting.status(endpoint.addr(), generation);
            status.probe_payload = Some(String::from_utf8(echoed)?);
            Some(serde_json::to_vec(&status)?)
        }
        Command::OpenServiceTunnel { endpoint_addr } => {
            anyhow::ensure!(
                accounting.stream_opened(),
                "data worker is draining or full"
            );
            let result =
                open_service_tunnel(&mut stream, &endpoint, endpoint_addr, &accounting).await;
            accounting.stream_closed();
            result?;
            None
        }
        Command::OpenMeshControlTunnel { endpoint_addr } => {
            anyhow::ensure!(
                accounting.stream_opened(),
                "data worker is draining or full"
            );
            let result = open_tunnel(
                &mut stream,
                &endpoint,
                endpoint_addr,
                agent_vmm_data::MESH_CONTROL_ALPN,
                &accounting,
            )
            .await;
            accounting.stream_closed();
            result?;
            None
        }
        command => {
            Some(pairing_runtime::handle_command(command, &endpoint, &pairing_sessions).await?)
        }
    };
    if let Some(response) = response {
        stream.write_all(&response).await?;
        stream.write_all(b"\n").await?;
    }
    Ok(())
}

async fn open_service_tunnel(
    local: &mut UnixStream,
    endpoint: &Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    accounting: &Accounting,
) -> Result<()> {
    open_tunnel(
        local,
        endpoint,
        endpoint_addr,
        SERVICE_TUNNEL_ALPN,
        accounting,
    )
    .await
}

async fn open_tunnel(
    local: &mut UnixStream,
    endpoint: &Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    alpn: &[u8],
    accounting: &Accounting,
) -> Result<()> {
    let connection = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        endpoint.connect(endpoint_addr, alpn).await
    })
    .await
    .context("service tunnel connect timed out")??;
    let (send, receive) = connection.open_bi().await?;
    local.write_all(b"{\"ready\":true}\n").await?;
    bridge(local, send, receive, accounting).await?;
    connection.close(0u32.into(), b"service tunnel complete");
    Ok(())
}

async fn serve_incoming(
    endpoint: Endpoint,
    accounting: Arc<Accounting>,
    semaphore: Arc<Semaphore>,
    service_socket: PathBuf,
    mesh_control_socket: PathBuf,
    pairing_sessions: PairingSessions,
) {
    let pairing_semaphore = Arc::new(Semaphore::new(PAIRING_INCOMING_LIMIT));
    while let Some(incoming) = endpoint.accept().await {
        if !accounting.accepting() {
            continue;
        }
        let accounting = accounting.clone();
        let semaphore = semaphore.clone();
        let service_socket = service_socket.clone();
        let mesh_control_socket = mesh_control_socket.clone();
        let pairing_sessions = pairing_sessions.clone();
        let pairing_semaphore = pairing_semaphore.clone();
        tokio::spawn(async move {
            if let Ok(connection) = incoming.await {
                if connection.alpn() == agent_vmm_data::PAIRING_ALPN {
                    let _ = pairing_runtime::handle_incoming(
                        connection,
                        pairing_sessions,
                        pairing_semaphore,
                    )
                    .await;
                    return;
                }
                let target = match connection.alpn() {
                    SERVICE_TUNNEL_ALPN => Some(service_socket),
                    agent_vmm_data::MESH_CONTROL_ALPN => Some(mesh_control_socket),
                    _ => None,
                };
                let Some(target) = target else { return };
                while let Ok((send, recv)) = connection.accept_bi().await {
                    let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                        break;
                    };
                    if !accounting.stream_opened() {
                        break;
                    }
                    let accounting = accounting.clone();
                    let target = target.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Ok(mut local) = UnixStream::connect(target).await {
                            let _ = bridge(&mut local, send, recv, &accounting).await;
                        }

                        accounting.stream_closed();
                    });
                }
            }
        });
    }
}

async fn bridge(
    local: &mut UnixStream,
    mut send: iroh::endpoint::SendStream,
    mut receive: iroh::endpoint::RecvStream,
    accounting: &Accounting,
) -> Result<()> {
    let (mut local_read, mut local_write) = local.split();
    let (outbound, inbound) = tokio::join!(
        async {
            let count = tokio::io::copy(&mut local_read, &mut send).await?;
            send.finish()?;
            Result::<u64>::Ok(count)
        },
        async {
            let count = tokio::io::copy(&mut receive, &mut local_write).await?;
            local_write.shutdown().await?;
            Result::<u64>::Ok(count)
        },
    );
    let outbound = outbound?;
    let inbound = inbound?;
    accounting.add_out(outbound);
    accounting.add_in(inbound);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{file_identity, remove_owned_socket};

    #[test]
    fn socket_removal_spares_a_replacement_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.sock");
        let previous = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let previous_identity = file_identity(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let replacement = std::os::unix::net::UnixListener::bind(&path).unwrap();
        remove_owned_socket(&path, previous_identity);
        assert!(path.exists());
        remove_owned_socket(&path, file_identity(&path).unwrap());
        assert!(!path.exists());
        drop((previous, replacement));
    }
}
