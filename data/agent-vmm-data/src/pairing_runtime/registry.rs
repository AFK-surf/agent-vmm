use super::*;
use agent_vmm_data::{Command, PAIRING_ALPN, PairingSecretKind, pairing::PairingSecret};
use std::collections::HashMap;
use tokio::sync::Mutex;

type SessionId = [u8; 16];
pub type PairingSessions = Arc<Mutex<HashMap<SessionId, PairingHandle>>>;

pub struct PairingHandle {
    pub(super) commands: mpsc::Sender<PairingCommand>,
    pub(super) status: watch::Receiver<PairingStatus>,
}

pub async fn handle_command(
    command: Command,
    endpoint: &Endpoint,
    sessions: &PairingSessions,
) -> Result<Vec<u8>> {
    match command {
        Command::PreparePairingManager {
            context,
            secret_kind,
            secret,
            acceptance,
        } => {
            ensure!(
                !acceptance.is_empty() && acceptance.len() <= 64 * 1024,
                "pairing acceptance is outside bounds"
            );
            let session = PairingSession::new(
                context,
                pairing_secret(secret_kind, secret)?,
                Instant::now(),
            )?;
            let (receiver, publisher, mut status) =
                register(sessions, &session, PairingRole::Manager).await?;
            let reply = serde_json::to_vec(&status)?;
            tokio::spawn(async move {
                run_manager_task(
                    session,
                    Zeroizing::new(acceptance),
                    receiver,
                    publisher,
                    &mut status,
                )
                .await;
            });
            Ok(reply)
        }
        Command::JoinPairing {
            endpoint_addr,
            context,
            secret_kind,
            secret,
            proposal,
        } => {
            let session = PairingSession::new(
                context,
                pairing_secret(secret_kind, secret)?,
                Instant::now(),
            )?;
            let key = session_id(&session.context().session_id)?;
            let (receiver, publisher, mut status) =
                register(sessions, &session, PairingRole::Joiner).await?;
            let mut observer = {
                sessions
                    .lock()
                    .await
                    .get(&key)
                    .expect("registered pairing")
                    .status
                    .clone()
            };
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let mut session = session;
                let mut receiver = receiver;
                status.state = PairingSessionState::Handshaking;
                status.attempts_remaining = status.attempts_remaining.saturating_sub(1);
                publisher.send_replace(status.clone());
                let deadline = Instant::now() + PAIRING_UNAUTHENTICATED_TIMEOUT;
                let handshake = async {
                    let connection = tokio::time::timeout(
                        Duration::from_secs(10),
                        endpoint.connect(endpoint_addr, PAIRING_ALPN),
                    )
                    .await
                    .context("pairing connection timed out")??;
                    let (mut send, mut receive) = connection.open_bi().await?;
                    let mut result = run_joiner(
                        &mut send,
                        &mut receive,
                        &mut session,
                        &proposal,
                        Instant::now(),
                    )
                    .await?;
                    let records = result.take_record_layer()?;
                    Ok::<_, anyhow::Error>((
                        Channel {
                            connection,
                            send,
                            receive,
                            records,
                        },
                        result,
                    ))
                };
                match wait_handshake(handshake, &mut receiver, deadline).await {
                    HandshakeOutcome::Complete(Ok((channel, result))) => {
                        status.authenticated(&session, &result);
                        status.update(&session);
                        publisher.send_replace(status.clone());
                        run_connected(&mut session, channel, receiver, &publisher, &mut status)
                            .await;
                    }
                    HandshakeOutcome::Rejected(mismatch, reply) => {
                        session.reject(mismatch.then_some(PairingFailure::ContextMismatch));
                        status.update(&session);
                        publisher.send_replace(status);
                        if let Some(reply) = reply {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    failure => {
                        session.reject(Some(if matches!(failure, HandshakeOutcome::TimedOut) {
                            PairingFailure::Timeout
                        } else {
                            PairingFailure::AuthenticationFailed
                        }));
                        status.update(&session);
                        publisher.send_replace(status);
                    }
                }
            });
            while !observer.borrow().terminal()
                && observer.borrow().state != PairingSessionState::AwaitingConfirm
            {
                if observer.changed().await.is_err() {
                    break;
                }
            }
            Ok(serde_json::to_vec(&*observer.borrow())?)
        }
        Command::PairingStatus { session_id: id } => status_json(sessions, &id).await,
        Command::ConfirmPairing { session_id: id } => {
            send_command(sessions, &id, PairingCommand::Confirm).await
        }
        Command::CommitPairing {
            session_id: id,
            registry_cas_succeeded,
        } => {
            send_command(sessions, &id, |reply| {
                PairingCommand::Commit(registry_cas_succeeded, reply)
            })
            .await
        }
        Command::RejectPairing {
            session_id: id,
            context_mismatch,
        } => {
            send_command(sessions, &id, |reply| {
                PairingCommand::Reject(context_mismatch, reply)
            })
            .await
        }
        _ => anyhow::bail!("command is not a pairing command"),
    }
}

pub(super) async fn register(
    sessions: &PairingSessions,
    session: &PairingSession,
    role: PairingRole,
) -> Result<(
    mpsc::Receiver<PairingCommand>,
    watch::Sender<PairingStatus>,
    PairingStatus,
)> {
    let key = session_id(&session.context().session_id)?;
    let status = PairingStatus::new(session, role);
    let (publisher, observer) = watch::channel(status.clone());
    let (sender, receiver) = mpsc::channel(4);
    let mut registry = sessions.lock().await;
    ensure!(
        !registry.contains_key(&key),
        "pairing session already exists"
    );
    registry.insert(
        key,
        PairingHandle {
            commands: sender,
            status: observer,
        },
    );
    drop(registry);
    let sessions = sessions.clone();
    let deadline = session.deadline();
    tokio::spawn(async move {
        tokio::time::sleep_until(deadline.into()).await;
        sessions.lock().await.remove(&key);
    });
    Ok((receiver, publisher, status))
}

pub(super) async fn status_json(sessions: &PairingSessions, id: &[u8]) -> Result<Vec<u8>> {
    let key = session_id(id)?;
    let registry = sessions.lock().await;
    match registry.get(&key) {
        Some(handle) => Ok(serde_json::to_vec(&*handle.status.borrow())?),
        None => Ok(br#"{"error":"pairing_not_found"}"#.to_vec()),
    }
}

pub(super) async fn send_command(
    sessions: &PairingSessions,
    id: &[u8],
    command: impl FnOnce(oneshot::Sender<Result<()>>) -> PairingCommand,
) -> Result<Vec<u8>> {
    let key = session_id(id)?;
    let handle = {
        sessions
            .lock()
            .await
            .get(&key)
            .map(|handle| (handle.commands.clone(), handle.status.clone()))
    };
    let Some((sender, observer)) = handle else {
        return Ok(br#"{"error":"pairing_not_found"}"#.to_vec());
    };
    if observer.borrow().terminal() {
        return Ok(serde_json::to_vec(&*observer.borrow())?);
    }
    let (reply, result) = oneshot::channel();
    sender
        .send(command(reply))
        .await
        .context("pairing task is unavailable")?;
    result.await.context("pairing task stopped")??;
    Ok(serde_json::to_vec(&*observer.borrow())?)
}

pub(super) fn session_id(id: &[u8]) -> Result<SessionId> {
    id.try_into()
        .context("pairing session ID must contain 128 bits")
}
fn pairing_secret(kind: PairingSecretKind, secret: Vec<u8>) -> Result<PairingSecret> {
    match kind {
        PairingSecretKind::Pin => PairingSecret::from_pin_owned(secret),
        PairingSecretKind::Qr => PairingSecret::from_qr_secret_owned(secret),
    }
}
