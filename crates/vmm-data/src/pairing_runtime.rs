//! Pairing tasks own their connection and secrets. The registry holds command and status handles.

use crate::{
    pairing::{PairingRecordLayer, PairingRole},
    pairing_session::{PairingFailure, PairingSession, PairingSessionState},
    pairing_wire::{
        ManagerHello, PairingRecord, PairingWireResult, receive_manager_hello, receive_record,
        run_joiner, run_manager_after_hello, send_record,
    },
};
use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint,
    endpoint::{Connection, RecvStream, SendStream},
};
use serde::Serialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use zeroize::Zeroizing;

mod registry;
use registry::session_id;
pub use registry::{PairingSessions, handle_command};

pub const PAIRING_INCOMING_LIMIT: usize = 4;
pub const PAIRING_UNAUTHENTICATED_TIMEOUT: Duration = Duration::from_secs(30);
#[derive(Clone, Serialize)]
pub struct PairingStatus {
    session_id: Vec<u8>,
    role: PairingRole,
    state: PairingSessionState,
    attempts_remaining: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_failure: Option<PairingFailure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_payload: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_identity: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_root_key: Option<Vec<u8>>,
}

impl PairingStatus {
    fn new(session: &PairingSession, role: PairingRole) -> Self {
        Self {
            session_id: session.context().session_id.clone(),
            role,
            state: session.state(),
            attempts_remaining: session.attempts_remaining(),
            last_failure: None,
            sas: None,
            peer_payload: None,
            peer_identity: None,
            peer_root_key: None,
        }
    }
    fn update(&mut self, session: &PairingSession) {
        self.state = session.state();
        self.attempts_remaining = session.attempts_remaining();
        self.last_failure = session.last_failure();
        if session.terminal() {
            self.sas = None;
            self.peer_payload = None;
            self.peer_identity = None;
            self.peer_root_key = None;
        }
    }
    fn authenticated(&mut self, session: &PairingSession, result: &PairingWireResult) {
        let context = session.context();
        let (identity, root_key) = match self.role {
            PairingRole::Manager => (&context.joiner_identity, &context.joiner_root_key),
            PairingRole::Joiner => (&context.manager_identity, &context.manager_root_key),
        };
        self.peer_identity = Some(identity.clone());
        self.peer_root_key = Some(root_key.clone());
        self.sas = Some(result.sas.clone());
        self.peer_payload = Some(result.peer_payload.clone());
    }
    fn terminal(&self) -> bool {
        matches!(
            self.state,
            PairingSessionState::Committed
                | PairingSessionState::Rejected
                | PairingSessionState::Expired
        )
    }
}

enum PairingCommand {
    Incoming {
        connection: Connection,
        send: SendStream,
        receive: RecvStream,
        hello: Box<ManagerHello>,
        permit: OwnedSemaphorePermit,
        deadline: Instant,
    },
    Confirm(oneshot::Sender<Result<()>>),
    Commit(bool, oneshot::Sender<Result<()>>),
    Reject(bool, oneshot::Sender<Result<()>>),
}

struct Channel {
    connection: Connection,
    send: SendStream,
    receive: RecvStream,
    records: PairingRecordLayer,
}

async fn run_manager_task(
    mut session: PairingSession,
    acceptance: Zeroizing<Vec<u8>>,
    mut commands: mpsc::Receiver<PairingCommand>,
    publisher: watch::Sender<PairingStatus>,
    status: &mut PairingStatus,
) {
    loop {
        let command = tokio::select! {
            command = commands.recv() => command,
            _ = tokio::time::sleep_until(session.deadline().into()) => { session.expire_if_needed(Instant::now()); None }
        };
        match command {
            Some(PairingCommand::Incoming {
                connection,
                mut send,
                mut receive,
                hello,
                permit,
                deadline,
            }) => {
                status.state = PairingSessionState::Handshaking;
                status.attempts_remaining = session.attempts_remaining().saturating_sub(1);
                publisher.send_replace(status.clone());
                let deadline = deadline.min(session.deadline());
                let outcome = wait_handshake(
                    run_manager_after_hello(
                        &mut send,
                        &mut receive,
                        &mut session,
                        *hello,
                        &acceptance,
                        Instant::now(),
                    ),
                    &mut commands,
                    deadline,
                )
                .await;
                drop(permit);
                match outcome {
                    HandshakeOutcome::Complete(Ok(mut result)) => {
                        status.authenticated(&session, &result);
                        let records = result
                            .take_record_layer()
                            .expect("completed handshake records");
                        status.update(&session);
                        publisher.send_replace(status.clone());
                        run_connected(
                            &mut session,
                            Channel {
                                connection,
                                send,
                                receive,
                                records,
                            },
                            commands,
                            &publisher,
                            status,
                        )
                        .await;
                        return;
                    }
                    HandshakeOutcome::Rejected(mismatch, reply) => {
                        session.reject(mismatch.then_some(PairingFailure::ContextMismatch));
                        status.update(&session);
                        publisher.send_replace(status.clone());
                        if let Some(reply) = reply {
                            let _ = reply.send(Ok(()));
                        }
                        connection.close(0u32.into(), b"pairing rejected");
                    }
                    failure => {
                        if matches!(failure, HandshakeOutcome::TimedOut) {
                            session.attempt_failed(PairingFailure::Timeout, Instant::now());
                        }
                        connection.close(0u32.into(), b"pairing attempt failed");
                    }
                }
            }
            Some(PairingCommand::Reject(context_mismatch, reply)) => {
                session.reject(context_mismatch.then_some(PairingFailure::ContextMismatch));
                status.update(&session);
                publisher.send_replace(status.clone());
                let _ = reply.send(Ok(()));
            }
            Some(PairingCommand::Confirm(reply) | PairingCommand::Commit(_, reply)) => {
                let _ = reply.send(Err(anyhow::anyhow!("pairing key is not confirmed")));
            }
            None => {
                session.expire_if_needed(Instant::now());
                if !session.terminal() {
                    session.reject(None);
                }
            }
        }
        status.update(&session);
        publisher.send_replace(status.clone());
        if session.terminal() {
            return;
        }
    }
}

async fn run_connected(
    session: &mut PairingSession,
    mut channel: Channel,
    mut commands: mpsc::Receiver<PairingCommand>,
    publisher: &watch::Sender<PairingStatus>,
    status: &mut PairingStatus,
) {
    enum Event {
        Command(Option<PairingCommand>),
        Record(Result<PairingRecord>),
        Deadline,
    }
    let mut buffered = Vec::new();
    while !session.terminal() {
        let event = tokio::select! {
            command = commands.recv() => Event::Command(command),
            record = receive_record(&mut channel.receive, &mut channel.records, &mut buffered) => Event::Record(record),
            _ = tokio::time::sleep_until(session.deadline().into()) => Event::Deadline,
        };
        let mut reply = None;
        let outcome = match event {
            Event::Command(Some(PairingCommand::Confirm(sender))) => {
                reply = Some(sender);
                match session.confirm_local(Instant::now()) {
                    Ok(()) => {
                        match tokio::time::timeout_at(
                            session.deadline().into(),
                            send_record(
                                &mut channel.send,
                                PairingRecord::Confirmed,
                                &mut channel.records,
                            ),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => {
                                session.expire_if_needed(Instant::now());
                                Err(anyhow::anyhow!("pairing confirmation timed out"))
                            }
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            Event::Command(Some(PairingCommand::Commit(success, sender))) => {
                reply = Some(sender);
                if status.role != PairingRole::Manager {
                    Err(anyhow::anyhow!("only the manager can commit pairing"))
                } else if success {
                    session.registry_committed(Instant::now())
                } else {
                    session.reject(None);
                    Ok(())
                }
            }
            Event::Command(Some(PairingCommand::Reject(mismatch, sender))) => {
                reply = Some(sender);
                session.reject(mismatch.then_some(PairingFailure::ContextMismatch));
                Ok(())
            }
            Event::Command(Some(PairingCommand::Incoming { connection, .. })) => {
                connection.close(0u32.into(), b"pairing is already active");
                Ok(())
            }
            Event::Record(Ok(PairingRecord::Confirmed)) => session.confirm_peer(Instant::now()),
            Event::Record(Ok(PairingRecord::Committed)) if status.role == PairingRole::Joiner => {
                if session.registry_committed(Instant::now()).is_err() {
                    session.reject(Some(PairingFailure::ProtocolViolation));
                }
                Ok(())
            }
            Event::Record(Ok(PairingRecord::Rejected)) => {
                session.reject(None);
                Ok(())
            }
            Event::Record(Ok(_)) => {
                session.reject(Some(PairingFailure::ProtocolViolation));
                Ok(())
            }
            Event::Record(Err(_)) | Event::Command(None) => {
                session.reject(Some(PairingFailure::ConnectionLost));
                Ok(())
            }
            Event::Deadline => {
                session.expire_if_needed(Instant::now());
                Ok(())
            }
        };
        if session.terminal() {
            let record = if session.state() == PairingSessionState::Committed {
                PairingRecord::Committed
            } else {
                PairingRecord::Rejected
            };
            if status.role == PairingRole::Manager || record == PairingRecord::Rejected {
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    send_record(&mut channel.send, record, &mut channel.records),
                )
                .await;
            }
            let _ = channel.send.finish();
        }
        status.update(session);
        publisher.send_replace(status.clone());
        if let Some(reply) = reply {
            let _ = reply.send(outcome);
        }
    }
    // Wait for QUIC to deliver the terminal record before closing the connection.
    let _ = tokio::time::timeout(Duration::from_secs(1), channel.send.stopped()).await;
    channel.connection.close(0u32.into(), b"pairing complete");
}

pub async fn handle_incoming(
    connection: Connection,
    sessions: PairingSessions,
    semaphore: Arc<Semaphore>,
) -> Result<()> {
    let permit = semaphore
        .try_acquire_owned()
        .context("pairing incoming limit reached")?;
    let deadline = Instant::now() + PAIRING_UNAUTHENTICATED_TIMEOUT;
    let handshake = async {
        let (send, mut receive) = connection.accept_bi().await?;
        let hello = receive_manager_hello(&mut receive).await?;
        let key = session_id(hello.session_id())?;
        let sender = {
            let registry = sessions.lock().await;
            let handle = registry
                .get(&key)
                .context("pairing invitation is unavailable")?;
            ensure!(
                handle.status.borrow().state == PairingSessionState::Invited,
                "pairing is already active"
            );
            handle.commands.clone()
        };
        sender
            .try_send(PairingCommand::Incoming {
                connection: connection.clone(),
                send,
                receive,
                hello: Box::new(hello),
                permit,
                deadline,
            })
            .context("pairing invitation is busy")?;
        Ok::<_, anyhow::Error>(())
    };
    let outcome = tokio::time::timeout(PAIRING_UNAUTHENTICATED_TIMEOUT, handshake)
        .await
        .context("pairing hello timed out")
        .and_then(|value| value);
    if outcome.is_err() {
        connection.close(0u32.into(), b"pairing unavailable");
    }
    outcome
}

#[cfg(test)]
mod tests;

enum HandshakeOutcome<T> {
    Complete(Result<T>),
    TimedOut,
    Rejected(bool, Option<oneshot::Sender<Result<()>>>),
}

async fn wait_handshake<T>(
    handshake: impl std::future::Future<Output = Result<T>>,
    commands: &mut mpsc::Receiver<PairingCommand>,
    deadline: Instant,
) -> HandshakeOutcome<T> {
    tokio::pin!(handshake);
    loop {
        tokio::select! {
            result = &mut handshake => return HandshakeOutcome::Complete(result),
            _ = tokio::time::sleep_until(deadline.into()) => return HandshakeOutcome::TimedOut,
            command = commands.recv() => match command {
                Some(PairingCommand::Reject(mismatch, reply)) => return HandshakeOutcome::Rejected(mismatch, Some(reply)),
                Some(PairingCommand::Incoming { connection, .. }) => connection.close(0u32.into(), b"pairing is already active"),
                Some(PairingCommand::Confirm(reply) | PairingCommand::Commit(_, reply)) => {
                    let _ = reply.send(Err(anyhow::anyhow!("pairing key is not confirmed")));
                }
                None => return HandshakeOutcome::Rejected(false, None),
            }
        }
    }
}
