//! Bounded wire adapter for `agent-vmm/pairing/1`.
//!
//! The wire carries only PAKE shares, confirmation MACs, and PAKE-encrypted
//! application payloads. PINs, QR secrets, PAKE scalars, and derived keys never
//! leave the in-memory pairing owner.

use crate::{
    MAX_FRAME_BYTES,
    pairing::{PairingRole, finish_initiator, finish_responder},
    pairing_session::PairingSession,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientHello {
    context: crate::pairing::OwnedPairingContext,
    share: Vec<u8>,
}

pub struct ManagerHello(ClientHello);

impl ManagerHello {
    pub fn session_id(&self) -> &[u8] {
        &self.0.context.session_id
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerHello {
    share: Vec<u8>,
    confirmation_mac: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfirmation {
    confirmation_mac: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncryptedRecord {
    ciphertext: Vec<u8>,
}

pub struct PairingWireResult {
    pub sas: String,
    pub peer_payload: Vec<u8>,
    record_layer: Option<crate::pairing::PairingRecordLayer>,
}

impl PairingWireResult {
    pub fn take_record_layer(&mut self) -> Result<crate::pairing::PairingRecordLayer> {
        self.record_layer
            .take()
            .context("pairing confirmation channel is unavailable")
    }
}

impl core::fmt::Debug for PairingWireResult {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("PairingWireResult")
            .field("sas", &"[REDACTED]")
            .field("peer_payload_bytes", &self.peer_payload.len())
            .finish()
    }
}

pub async fn run_joiner<W, R>(
    writer: &mut W,
    reader: &mut R,
    session: &mut PairingSession,
    proposal: &[u8],
    now: std::time::Instant,
) -> Result<PairingWireResult>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let result = run_joiner_inner(writer, reader, session, proposal, now).await;
    if result.is_err() {
        let _ = writer.shutdown().await;
        session.attempt_failed(now);
    }
    result
}

async fn run_joiner_inner<W, R>(
    writer: &mut W,
    reader: &mut R,
    session: &mut PairingSession,
    proposal: &[u8],
    now: std::time::Instant,
) -> Result<PairingWireResult>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    ensure_payload(proposal)?;
    let (share, state) = session.begin_initiator(now)?;
    send_frame(
        writer,
        &ClientHello {
            context: session.context().clone(),
            share,
        },
    )
    .await?;

    let hello: ServerHello = receive_frame(reader).await?;
    let pending = finish_initiator(state, &hello.share)?;
    let local_mac = pending.confirmation_mac().to_vec();
    let confirmed = pending.confirm(&hello.confirmation_mac)?;
    send_frame(
        writer,
        &ClientConfirmation {
            confirmation_mac: local_mac,
        },
    )
    .await?;
    session.key_confirmed(now)?;
    let sas = confirmed.sas(&session.context().session_id)?;
    let mut records = confirmed.into_record_layer(PairingRole::Joiner)?;
    send_frame(
        writer,
        &EncryptedRecord {
            ciphertext: records.seal(proposal)?,
        },
    )
    .await?;
    let response: EncryptedRecord = receive_frame(reader).await?;
    let peer_payload = records.open(&response.ciphertext)?;
    Ok(PairingWireResult {
        sas,
        peer_payload,
        record_layer: Some(records),
    })
}

pub async fn run_manager<W, R>(
    writer: &mut W,
    reader: &mut R,
    session: &mut PairingSession,
    acceptance: &[u8],
    now: std::time::Instant,
) -> Result<PairingWireResult>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let hello = match receive_manager_hello(reader).await {
        Ok(hello) => hello,
        Err(error) => return Err(error),
    };
    run_manager_after_hello(writer, reader, session, hello, acceptance, now).await
}

pub async fn receive_manager_hello<R: AsyncRead + Unpin>(reader: &mut R) -> Result<ManagerHello> {
    receive_frame(reader).await.map(ManagerHello)
}

pub async fn run_manager_after_hello<W, R>(
    writer: &mut W,
    reader: &mut R,
    session: &mut PairingSession,
    hello: ManagerHello,
    acceptance: &[u8],
    now: std::time::Instant,
) -> Result<PairingWireResult>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let result = run_manager_inner(writer, reader, session, hello.0, acceptance, now).await;
    if result.is_err() {
        let _ = writer.shutdown().await;
        session.attempt_failed(now);
    }
    result
}

async fn run_manager_inner<W, R>(
    writer: &mut W,
    reader: &mut R,
    session: &mut PairingSession,
    hello: ClientHello,
    acceptance: &[u8],
    now: std::time::Instant,
) -> Result<PairingWireResult>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    ensure_payload(acceptance)?;
    ensure!(
        hello.context.session_id == session.context().session_id,
        "pairing session does not match invite"
    );
    session.bind_joiner_context(&hello.context)?;
    let (share, state) = session.begin_responder(now)?;
    let pending = finish_responder(state, &hello.share)?;
    send_frame(
        writer,
        &ServerHello {
            share,
            confirmation_mac: pending.confirmation_mac().to_vec(),
        },
    )
    .await?;
    let peer: ClientConfirmation = receive_frame(reader).await?;
    let confirmed = pending.confirm(&peer.confirmation_mac)?;
    session.key_confirmed(now)?;
    let sas = confirmed.sas(&session.context().session_id)?;
    let mut records = confirmed.into_record_layer(PairingRole::Manager)?;
    let proposal: EncryptedRecord = receive_frame(reader).await?;
    let peer_payload = records.open(&proposal.ciphertext)?;
    send_frame(
        writer,
        &EncryptedRecord {
            ciphertext: records.seal(acceptance)?,
        },
    )
    .await?;
    Ok(PairingWireResult {
        sas,
        peer_payload,
        record_layer: Some(records),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationRole {
    Manager,
    Joiner,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmationRecord {
    session_id: Vec<u8>,
    ciphertext: Vec<u8>,
}

pub struct PeerConfirmation(ConfirmationRecord);

impl PeerConfirmation {
    pub fn session_id(&self) -> &[u8] {
        &self.0.session_id
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmationAck {
    recorded: bool,
}

pub async fn send_local_confirmation<W: AsyncWrite + Unpin>(
    writer: &mut W,
    session_id: &[u8],
    role: ConfirmationRole,
    records: &mut crate::pairing::PairingRecordLayer,
) -> Result<()> {
    let plaintext = match role {
        ConfirmationRole::Manager => b"manager-local-confirmed".as_slice(),
        ConfirmationRole::Joiner => b"joiner-local-confirmed".as_slice(),
    };
    send_frame(
        writer,
        &ConfirmationRecord {
            session_id: session_id.to_vec(),
            ciphertext: records.seal(plaintext)?,
        },
    )
    .await
}

pub async fn receive_local_confirmation<R: AsyncRead + Unpin>(
    reader: &mut R,
    expected_session_id: &[u8],
    records: &mut crate::pairing::PairingRecordLayer,
) -> Result<ConfirmationRole> {
    let frame = receive_peer_confirmation(reader).await?;
    open_local_confirmation(frame, expected_session_id, records)
}

pub async fn receive_peer_confirmation<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<PeerConfirmation> {
    receive_frame(reader).await.map(PeerConfirmation)
}

pub fn open_local_confirmation(
    frame: PeerConfirmation,
    expected_session_id: &[u8],
    records: &mut crate::pairing::PairingRecordLayer,
) -> Result<ConfirmationRole> {
    ensure!(
        frame.0.session_id == expected_session_id,
        "pairing confirmation session mismatch"
    );
    match records.open(&frame.0.ciphertext)?.as_slice() {
        b"manager-local-confirmed" => Ok(ConfirmationRole::Manager),
        b"joiner-local-confirmed" => Ok(ConfirmationRole::Joiner),
        _ => anyhow::bail!("invalid pairing local confirmation"),
    }
}

pub async fn send_confirmation_ack<W: AsyncWrite + Unpin>(writer: &mut W) -> Result<()> {
    send_frame(writer, &ConfirmationAck { recorded: true }).await
}

pub async fn receive_confirmation_ack<R: AsyncRead + Unpin>(reader: &mut R) -> Result<()> {
    let ack: ConfirmationAck = receive_frame(reader).await?;
    ensure!(ack.recorded, "pairing confirmation was not recorded");
    Ok(())
}

fn ensure_payload(payload: &[u8]) -> Result<()> {
    ensure!(
        !payload.is_empty() && payload.len() <= crate::pairing::MAX_ENCRYPTED_RECORD_BYTES,
        "pairing application payload is outside bounds"
    );
    Ok(())
}

async fn send_frame<W: AsyncWrite + Unpin, T: Serialize>(writer: &mut W, value: &T) -> Result<()> {
    let encoded = serde_json::to_vec(value).context("encode pairing frame")?;
    ensure!(
        !encoded.is_empty() && encoded.len() <= MAX_FRAME_BYTES,
        "pairing frame is outside bounds"
    );
    writer
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

async fn receive_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(reader: &mut R) -> Result<T> {
    let length = reader.read_u32().await? as usize;
    ensure!(
        length > 0 && length <= MAX_FRAME_BYTES,
        "pairing frame is outside bounds"
    );
    let mut encoded = vec![0u8; length];
    reader.read_exact(&mut encoded).await?;
    serde_json::from_slice(&encoded).context("decode pairing frame")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pairing::{OwnedPairingContext, PairingSecret};
    use crate::pairing_session::{MAX_PAIRING_ATTEMPTS, PairingSessionState};
    use std::time::Instant;

    fn context(revision: u64) -> OwnedPairingContext {
        OwnedPairingContext {
            session_id: vec![9; 16],
            mesh_id: b"mesh-1".to_vec(),
            invite_revision: revision,
            manager_identity: b"manager".to_vec(),
            joiner_identity: b"joiner".to_vec(),
            manager_root_key: vec![2; 33],
            joiner_root_key: vec![3; 33],
            roles_and_features: b"use-services".to_vec(),
        }
    }

    fn session(secret: &[u8], revision: u64, now: Instant) -> PairingSession {
        PairingSession::new(
            context(revision),
            PairingSecret::from_pin(secret).unwrap(),
            now,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn real_duplex_exchange_confirms_and_encrypts_payloads() {
        let now = Instant::now();
        let mut manager_session = session(b"12345678", 7, now);
        let mut joiner_session = session(b"12345678", 7, now);
        let (manager_io, joiner_io) = tokio::io::duplex(256 * 1024);
        let (mut manager_read, mut manager_write) = tokio::io::split(manager_io);
        let (mut joiner_read, mut joiner_write) = tokio::io::split(joiner_io);

        let (manager, joiner) = tokio::join!(
            run_manager(
                &mut manager_write,
                &mut manager_read,
                &mut manager_session,
                b"manager-acceptance",
                now
            ),
            run_joiner(
                &mut joiner_write,
                &mut joiner_read,
                &mut joiner_session,
                b"signed-identity-permission-proposal",
                now
            )
        );
        let manager = manager.unwrap();
        let joiner = joiner.unwrap();
        assert_eq!(manager.sas, joiner.sas);
        assert_eq!(manager.peer_payload, b"signed-identity-permission-proposal");
        assert_eq!(joiner.peer_payload, b"manager-acceptance");
        assert_eq!(
            manager_session.state(),
            PairingSessionState::AwaitingLocalConfirmation
        );
        assert_eq!(
            joiner_session.state(),
            PairingSessionState::AwaitingLocalConfirmation
        );
    }

    #[tokio::test]
    async fn wrong_pin_consumes_one_online_attempt_without_payload_release() {
        let now = Instant::now();
        let mut manager_session = session(b"12345678", 7, now);
        let mut joiner_session = session(b"87654321", 7, now);
        let (manager_io, joiner_io) = tokio::io::duplex(256 * 1024);
        let (mut manager_read, mut manager_write) = tokio::io::split(manager_io);
        let (mut joiner_read, mut joiner_write) = tokio::io::split(joiner_io);
        let (manager, joiner) = tokio::join!(
            run_manager(
                &mut manager_write,
                &mut manager_read,
                &mut manager_session,
                b"manager-acceptance",
                now
            ),
            run_joiner(
                &mut joiner_write,
                &mut joiner_read,
                &mut joiner_session,
                b"must-not-be-released",
                now
            )
        );
        assert!(manager.is_err() || joiner.is_err());
        assert_eq!(
            manager_session.attempts_remaining(),
            MAX_PAIRING_ATTEMPTS - 1
        );
        assert_eq!(
            joiner_session.attempts_remaining(),
            MAX_PAIRING_ATTEMPTS - 1
        );
    }

    #[tokio::test]
    async fn transcript_mismatch_fails_before_encrypted_proposal() {
        let now = Instant::now();
        let mut manager_session = session(b"12345678", 7, now);
        let mut joiner_session = session(b"12345678", 8, now);
        let (manager_io, joiner_io) = tokio::io::duplex(256 * 1024);
        let (mut manager_read, mut manager_write) = tokio::io::split(manager_io);
        let (mut joiner_read, mut joiner_write) = tokio::io::split(joiner_io);
        let (manager, joiner) = tokio::join!(
            run_manager(
                &mut manager_write,
                &mut manager_read,
                &mut manager_session,
                b"manager-acceptance",
                now
            ),
            run_joiner(
                &mut joiner_write,
                &mut joiner_read,
                &mut joiner_session,
                b"must-not-be-released",
                now
            )
        );
        assert!(manager.is_err() || joiner.is_err());
        assert_ne!(manager_session.state(), PairingSessionState::KeyConfirmed);
        assert_ne!(joiner_session.state(), PairingSessionState::KeyConfirmed);
    }

    #[tokio::test]
    async fn real_iroh_pairing_alpn_carries_only_the_bounded_wire_protocol() {
        use iroh::{Endpoint, endpoint::presets};

        let now = Instant::now();
        let manager_endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![crate::PAIRING_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let joiner_endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![crate::PAIRING_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let manager_addr = manager_endpoint.addr();
        let mut manager_session = session(b"12345678", 7, now);
        let mut joiner_session = session(b"12345678", 7, now);

        let manager = async {
            let incoming = manager_endpoint.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            assert_eq!(connection.alpn(), crate::PAIRING_ALPN);
            let (mut send, mut receive) = connection.accept_bi().await.unwrap();
            let result = run_manager(
                &mut send,
                &mut receive,
                &mut manager_session,
                b"manager-acceptance",
                now,
            )
            .await;
            let _ = send.finish();
            let _ = receive.read_to_end(1).await;
            result
        };
        let joiner = async {
            let connection = joiner_endpoint
                .connect(manager_addr, crate::PAIRING_ALPN)
                .await
                .unwrap();
            let (mut send, mut receive) = connection.open_bi().await.unwrap();
            let result = run_joiner(
                &mut send,
                &mut receive,
                &mut joiner_session,
                b"signed-identity-permission-proposal",
                now,
            )
            .await;
            let _ = send.finish();
            result
        };

        let (manager, joiner) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(manager, joiner)
        })
        .await
        .unwrap();
        let manager = manager.unwrap();
        let joiner = joiner.unwrap();
        assert_eq!(manager.sas, joiner.sas);
        assert_eq!(manager.peer_payload, b"signed-identity-permission-proposal");
        assert_eq!(joiner.peer_payload, b"manager-acceptance");
        manager_endpoint.close().await;
        joiner_endpoint.close().await;
    }
}
