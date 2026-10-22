use agent_vmm_data::{
    ALPNS, Accounting, Command, MAX_LOCAL_CAPABILITY_BYTES, MAX_STREAMS, PairingSecretKind,
    SERVICE_PREFACE_MAGIC, SERVICE_TUNNEL_ALPN,
    pairing::{PairingRecordLayer, PairingSecret},
    pairing_session::{PairingSession, PairingSessionState},
    pairing_wire::{
        ConfirmationRole, PairingWireResult, open_local_confirmation, receive_confirmation_ack,
        receive_manager_hello, receive_peer_confirmation, run_joiner, run_manager_after_hello,
        send_confirmation_ack, send_local_confirmation,
    },
    probe,
};
use anyhow::{Context, Result};
use iroh::{Endpoint, endpoint::presets};
use std::{
    collections::HashMap,
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

type PairingSessions = Arc<Mutex<HashMap<Vec<u8>, PairingEntry>>>;

struct PairingEntry {
    session: PairingSession,
    role: &'static str,
    acceptance: Zeroizing<Vec<u8>>,
    result: Option<PairingWireResult>,
    record_layer: Option<PairingRecordLayer>,
    connection: Option<iroh::endpoint::Connection>,
}

#[derive(serde::Serialize)]
struct PairingStatusResponse<'a> {
    session_id: &'a [u8],
    role: &'static str,
    state: PairingSessionState,
    attempts_remaining: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    sas: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_payload: Option<&'a [u8]>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let (socket, service_socket, mesh_control_socket, service_capability_file) =
        socket_arguments()?;
    let host_exited = watch_lifeline();
    let service_capability = tokio::fs::read(service_capability_file).await?;
    anyhow::ensure!(
        service_capability.len() >= 32 && service_capability.len() <= MAX_LOCAL_CAPABILITY_BYTES,
        "service capability is invalid"
    );
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
    let pairing_sessions: PairingSessions = Arc::new(Mutex::new(HashMap::new()));
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
        Arc::new(service_capability),
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

fn socket_arguments() -> Result<(PathBuf, PathBuf, PathBuf, PathBuf)> {
    let mut args = std::env::args().skip(1);
    match (
        args.next().as_deref(),
        args.next(),
        args.next().as_deref(),
        args.next(),
        args.next().as_deref(),
        args.next(),
        args.next().as_deref(),
        args.next(),
        args.next(),
    ) {
        (
            Some("--socket"),
            Some(socket),
            Some("--service-socket"),
            Some(service),
            Some("--mesh-control-socket"),
            Some(mesh_control),
            Some("--service-capability-file"),
            Some(capability),
            None,
        ) => {
            let socket = PathBuf::from(socket);
            let service = PathBuf::from(service);
            let mesh_control = PathBuf::from(mesh_control);
            let capability = PathBuf::from(capability);
            ensure_absolute(&socket, "control socket")?;
            ensure_absolute(&service, "service socket")?;
            ensure_absolute(&mesh_control, "mesh control socket")?;
            ensure_absolute(&capability, "service capability file")?;
            Ok((socket, service, mesh_control, capability))
        }
        _ => anyhow::bail!(
            "usage: agent-vmm-data --socket <absolute-path> --service-socket <absolute-path> --mesh-control-socket <absolute-path> --service-capability-file <absolute-path>"
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
        Command::PreparePairingManager {
            context,
            secret_kind,
            secret,
            acceptance,
        } => {
            anyhow::ensure!(
                context.session_id.len() == 16,
                "pairing session id must contain 128 bits"
            );
            anyhow::ensure!(
                !acceptance.is_empty() && acceptance.len() <= 64 * 1024,
                "pairing acceptance is outside bounds"
            );
            let secret = pairing_secret(secret_kind, secret)?;
            let key = context.session_id.clone();
            let session = PairingSession::new(context, secret, std::time::Instant::now())?;
            let mut sessions = pairing_sessions.lock().await;
            anyhow::ensure!(
                !sessions.contains_key(&key),
                "pairing session already exists"
            );
            sessions.insert(
                key.clone(),
                PairingEntry {
                    session,
                    role: "manager",
                    acceptance: Zeroizing::new(acceptance),
                    result: None,
                    record_layer: None,
                    connection: None,
                },
            );
            Some(pairing_status_json(
                sessions.get(&key).expect("inserted pairing"),
            )?)
        }
        Command::JoinPairing {
            endpoint_addr,
            context,
            secret_kind,
            secret,
            proposal,
        } => {
            anyhow::ensure!(
                context.session_id.len() == 16,
                "pairing session id must contain 128 bits"
            );
            let key = context.session_id.clone();
            let secret = pairing_secret(secret_kind, secret)?;
            let mut session = PairingSession::new(context, secret, std::time::Instant::now())?;
            let connection = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                endpoint.connect(endpoint_addr, agent_vmm_data::PAIRING_ALPN),
            )
            .await
            .context("pairing connect timed out")??;
            let (mut send, mut receive) = connection.open_bi().await?;
            let mut result = run_joiner(
                &mut send,
                &mut receive,
                &mut session,
                &proposal,
                std::time::Instant::now(),
            )
            .await?;
            let _ = send.finish();
            let record_layer = result.take_record_layer()?;
            let entry = PairingEntry {
                session,
                role: "joiner",
                acceptance: Zeroizing::new(Vec::new()),
                result: Some(result),
                record_layer: Some(record_layer),
                connection: Some(connection.clone()),
            };
            let response = pairing_status_json(&entry)?;
            pairing_sessions.lock().await.insert(key, entry);
            tokio::spawn(serve_pairing_confirmations(
                connection,
                pairing_sessions.clone(),
            ));
            Some(response)
        }
        Command::PairingStatus { session_id } => {
            let mut sessions = pairing_sessions.lock().await;
            let entry = sessions
                .get_mut(&session_id)
                .context("pairing session not found")?;
            entry.session.expire_if_needed(std::time::Instant::now());
            Some(pairing_status_json(entry)?)
        }
        Command::ConfirmPairing { session_id } => {
            let connection = {
                let sessions = pairing_sessions.lock().await;
                sessions
                    .get(&session_id)
                    .and_then(|entry| entry.connection.clone())
                    .context("pairing connection is unavailable")?
            };
            let (mut send, mut receive) = connection.open_bi().await?;
            {
                let mut sessions = pairing_sessions.lock().await;
                let entry = sessions
                    .get_mut(&session_id)
                    .context("pairing session not found")?;
                let role = if entry.role == "manager" {
                    entry.session.confirm_manager(std::time::Instant::now())?;
                    ConfirmationRole::Manager
                } else {
                    entry.session.confirm_joiner(std::time::Instant::now())?;
                    ConfirmationRole::Joiner
                };
                send_local_confirmation(
                    &mut send,
                    &session_id,
                    role,
                    entry
                        .record_layer
                        .as_mut()
                        .context("pairing confirmation channel is unavailable")?,
                )
                .await?;
            }
            send.finish()?;
            if let Err(error) = receive_confirmation_ack(&mut receive).await {
                if let Some(entry) = pairing_sessions.lock().await.get_mut(&session_id) {
                    entry.session.reject();
                    entry.record_layer = None;
                    entry.connection = None;
                }
                return Err(error);
            }
            let sessions = pairing_sessions.lock().await;
            Some(pairing_status_json(
                sessions
                    .get(&session_id)
                    .context("pairing session not found")?,
            )?)
        }
        Command::CommitPairing {
            session_id,
            registry_cas_succeeded,
        } => {
            let mut sessions = pairing_sessions.lock().await;
            let entry = sessions
                .get_mut(&session_id)
                .context("pairing session not found")?;
            if registry_cas_succeeded {
                entry
                    .session
                    .registry_committed(std::time::Instant::now())?;
            } else {
                entry.session.reject();
            }
            entry.record_layer = None;
            entry.connection = None;
            Some(pairing_status_json(entry)?)
        }
        Command::RejectPairing { session_id } => {
            let mut sessions = pairing_sessions.lock().await;
            let entry = sessions
                .get_mut(&session_id)
                .context("pairing session not found")?;
            entry.session.reject();
            entry.record_layer = None;
            entry.connection = None;
            Some(pairing_status_json(entry)?)
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
    service_capability: Arc<Vec<u8>>,
    pairing_sessions: PairingSessions,
) {
    while let Some(incoming) = endpoint.accept().await {
        if !accounting.accepting() {
            continue;
        }
        let accounting = accounting.clone();
        let semaphore = semaphore.clone();
        let service_socket = service_socket.clone();
        let mesh_control_socket = mesh_control_socket.clone();
        let service_capability = service_capability.clone();
        let pairing_sessions = pairing_sessions.clone();
        tokio::spawn(async move {
            if let Ok(connection) = incoming.await {
                if connection.alpn() == agent_vmm_data::PAIRING_ALPN {
                    let _ = handle_incoming_pairing(
                        connection,
                        pairing_sessions,
                        accounting,
                        semaphore,
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
                    let service_capability = service_capability.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Ok(mut local) = UnixStream::connect(target).await {
                            let length = (service_capability.len() as u32).to_be_bytes();
                            if local.write_all(SERVICE_PREFACE_MAGIC).await.is_ok()
                                && local.write_all(&length).await.is_ok()
                                && local.write_all(&service_capability).await.is_ok()
                            {
                                let _ = bridge(&mut local, send, recv, &accounting).await;
                            }
                        }
                        accounting.stream_closed();
                    });
                }
            }
        });
    }
}

fn pairing_secret(kind: PairingSecretKind, secret: Vec<u8>) -> Result<PairingSecret> {
    match kind {
        PairingSecretKind::Pin => PairingSecret::from_pin_owned(secret),
        PairingSecretKind::Qr => PairingSecret::from_qr_secret_owned(secret),
    }
}

fn pairing_status_json(entry: &PairingEntry) -> Result<Vec<u8>> {
    serde_json::to_vec(&PairingStatusResponse {
        session_id: &entry.session.context().session_id,
        role: entry.role,
        state: entry.session.state(),
        attempts_remaining: entry.session.attempts_remaining(),
        sas: entry.result.as_ref().map(|result| result.sas.as_str()),
        peer_payload: entry
            .result
            .as_ref()
            .map(|result| result.peer_payload.as_slice()),
    })
    .context("encode pairing status")
}

async fn handle_incoming_pairing(
    connection: iroh::endpoint::Connection,
    pairing_sessions: PairingSessions,
    accounting: Arc<Accounting>,
    semaphore: Arc<Semaphore>,
) -> Result<()> {
    let _permit = semaphore
        .try_acquire_owned()
        .context("pairing stream limit reached")?;
    anyhow::ensure!(
        accounting.stream_opened(),
        "data worker is draining or full"
    );
    let outcome = async {
        let (mut send, mut receive) = connection.accept_bi().await?;
        let hello = receive_manager_hello(&mut receive).await?;
        let key = hello.session_id().to_vec();
        let mut entry = pairing_sessions
            .lock()
            .await
            .remove(&key)
            .context("pairing invite is unavailable")?;
        let result = run_manager_after_hello(
            &mut send,
            &mut receive,
            &mut entry.session,
            hello,
            &entry.acceptance,
            std::time::Instant::now(),
        )
        .await;
        let result = match result {
            Ok(mut result) => {
                let record_layer = result.take_record_layer()?;
                entry.result = Some(result);
                entry.record_layer = Some(record_layer);
                entry.connection = Some(connection.clone());
                Ok(())
            }
            Err(error) => Err(error),
        };
        pairing_sessions.lock().await.insert(key, entry);
        let _ = send.finish();
        let _ = receive.read_to_end(1).await;
        if result.is_ok() {
            serve_pairing_confirmations(connection, pairing_sessions.clone()).await;
        }
        result
    }
    .await;
    accounting.stream_closed();
    outcome
}

async fn serve_pairing_confirmations(
    connection: iroh::endpoint::Connection,
    pairing_sessions: PairingSessions,
) {
    while let Ok((mut send, mut receive)) = connection.accept_bi().await {
        let sessions = pairing_sessions.clone();
        tokio::spawn(async move {
            let _ = handle_peer_confirmation(&mut send, &mut receive, sessions).await;
            let _ = send.finish();
        });
    }
}

async fn handle_peer_confirmation(
    send: &mut iroh::endpoint::SendStream,
    receive: &mut iroh::endpoint::RecvStream,
    pairing_sessions: PairingSessions,
) -> Result<()> {
    let confirmation = receive_peer_confirmation(receive).await?;
    let session_id = confirmation.session_id().to_vec();
    {
        let mut sessions = pairing_sessions.lock().await;
        let entry = sessions
            .get_mut(&session_id)
            .context("pairing session not found")?;
        let role = open_local_confirmation(
            confirmation,
            &session_id,
            entry
                .record_layer
                .as_mut()
                .context("pairing confirmation channel is unavailable")?,
        )?;
        match role {
            ConfirmationRole::Manager => entry.session.confirm_manager(std::time::Instant::now())?,
            ConfirmationRole::Joiner => entry.session.confirm_joiner(std::time::Instant::now())?,
        }
    }
    send_confirmation_ack(send).await
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
