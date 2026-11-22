//! Bounded wire adapter for `agent-vmm/pairing/2`.
//!
//! The wire carries only PAKE shares, confirmation MACs, and PAKE-encrypted
//! application payloads. PINs, QR secrets, PAKE scalars, and derived keys never
//! leave the in-memory pairing owner.

use crate::{
    MAX_FRAME_BYTES,
    pairing::{PairingRole, finish_initiator, finish_responder},
    pairing_session::{PairingFailure, PairingSession},
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
        if session.state() == crate::pairing_session::PairingSessionState::Handshaking {
            session.attempt_failed(
                PairingFailure::AuthenticationFailed,
                std::time::Instant::now(),
            );
        }
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
    session.key_confirmed(std::time::Instant::now())?;
    Ok(PairingWireResult {
        sas,
        peer_payload,
        record_layer: Some(records),
    })
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
        if session.state() == crate::pairing_session::PairingSessionState::Handshaking {
            session.attempt_failed(
                PairingFailure::AuthenticationFailed,
                std::time::Instant::now(),
            );
        }
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
    session.begin_manager_attempt(&hello.context, now)?;
    let (share, state) = session.begin_responder()?;
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
    session.key_confirmed(std::time::Instant::now())?;
    Ok(PairingWireResult {
        sas,
        peer_payload,
        record_layer: Some(records),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairingRecord {
    Confirmed,
    Committed,
    Rejected,
}

pub async fn send_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    record: PairingRecord,
    records: &mut crate::pairing::PairingRecordLayer,
) -> Result<()> {
    let plaintext: &[u8] = match record {
        PairingRecord::Confirmed => b"confirmed",
        PairingRecord::Committed => b"committed",
        PairingRecord::Rejected => b"rejected",
    };
    send_frame(
        writer,
        &EncryptedRecord {
            ciphertext: records.seal(plaintext)?,
        },
    )
    .await
}

pub async fn receive_record<R: AsyncRead + Unpin>(
    reader: &mut R,
    records: &mut crate::pairing::PairingRecordLayer,
    buffered: &mut Vec<u8>,
) -> Result<PairingRecord> {
    let frame = loop {
        if buffered.len() >= 4 {
            let length =
                u32::from_be_bytes(buffered[..4].try_into().expect("frame length")) as usize;
            ensure!(
                length > 0 && length <= MAX_FRAME_BYTES,
                "pairing frame is outside bounds"
            );
            if buffered.len() >= 4 + length {
                let frame: EncryptedRecord = serde_json::from_slice(&buffered[4..4 + length])
                    .context("decode pairing record")?;
                buffered.drain(..4 + length);
                break frame;
            }
        }
        let mut chunk = [0u8; 4096];
        let count = reader.read(&mut chunk).await?;
        ensure!(count != 0, "pairing connection closed");
        buffered.extend_from_slice(&chunk[..count]);
    };
    match records.open(&frame.ciphertext)?.as_slice() {
        b"confirmed" => Ok(PairingRecord::Confirmed),
        b"committed" => Ok(PairingRecord::Committed),
        b"rejected" => Ok(PairingRecord::Rejected),
        _ => anyhow::bail!("invalid pairing record"),
    }
}

fn ensure_payload(payload: &[u8]) -> Result<()> {
    ensure!(
        !payload.is_empty() && payload.len() <= crate::pairing::MAX_ENCRYPTED_RECORD_BYTES,
        "pairing payload is outside bounds"
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
            async {
                let hello = receive_manager_hello(&mut manager_read).await?;
                run_manager_after_hello(
                    &mut manager_write,
                    &mut manager_read,
                    &mut manager_session,
                    hello,
                    b"manager-acceptance",
                    now,
                )
                .await
            },
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
            PairingSessionState::AwaitingConfirm
        );
        assert_eq!(joiner_session.state(), PairingSessionState::AwaitingConfirm);
    }

    #[tokio::test]
    async fn a_partial_record_survives_an_interleaved_local_command() {
        let now = Instant::now();
        let mut manager_session = session(b"12345678", 7, now);
        let mut joiner_session = session(b"12345678", 7, now);
        let (manager_io, joiner_io) = tokio::io::duplex(256 * 1024);
        let (mut manager_read, mut manager_write) = tokio::io::split(manager_io);
        let (mut joiner_read, mut joiner_write) = tokio::io::split(joiner_io);
        let (manager, joiner) = tokio::join!(
            async {
                let hello = receive_manager_hello(&mut manager_read).await?;
                run_manager_after_hello(
                    &mut manager_write,
                    &mut manager_read,
                    &mut manager_session,
                    hello,
                    b"acceptance",
                    now,
                )
                .await
            },
            run_joiner(
                &mut joiner_write,
                &mut joiner_read,
                &mut joiner_session,
                b"proposal",
                now
            )
        );
        let mut manager_records = manager.unwrap().take_record_layer().unwrap();
        let mut joiner_records = joiner.unwrap().take_record_layer().unwrap();
        let encoded = serde_json::to_vec(&EncryptedRecord {
            ciphertext: joiner_records.seal(b"confirmed").unwrap(),
        })
        .unwrap();
        let mut frame = (encoded.len() as u32).to_be_bytes().to_vec();
        frame.extend(encoded);
        joiner_write.write_all(&frame[..6]).await.unwrap();
        let mut buffered = Vec::new();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                receive_record(&mut manager_read, &mut manager_records, &mut buffered)
            )
            .await
            .is_err()
        );
        manager_session.confirm_local(Instant::now()).unwrap();
        joiner_write.write_all(&frame[6..]).await.unwrap();
        assert_eq!(
            receive_record(&mut manager_read, &mut manager_records, &mut buffered)
                .await
                .unwrap(),
            PairingRecord::Confirmed
        );
        manager_session.confirm_peer(Instant::now()).unwrap();
        assert_eq!(manager_session.state(), PairingSessionState::ReadyToCommit);
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
            async {
                let hello = receive_manager_hello(&mut manager_read).await?;
                run_manager_after_hello(
                    &mut manager_write,
                    &mut manager_read,
                    &mut manager_session,
                    hello,
                    b"manager-acceptance",
                    now,
                )
                .await
            },
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
            async {
                let hello = receive_manager_hello(&mut manager_read).await?;
                run_manager_after_hello(
                    &mut manager_write,
                    &mut manager_read,
                    &mut manager_session,
                    hello,
                    b"manager-acceptance",
                    now,
                )
                .await
            },
            run_joiner(
                &mut joiner_write,
                &mut joiner_read,
                &mut joiner_session,
                b"must-not-be-released",
                now
            )
        );
        assert!(manager.is_err() || joiner.is_err());
        assert_ne!(
            manager_session.state(),
            PairingSessionState::AwaitingConfirm
        );
        assert_ne!(joiner_session.state(), PairingSessionState::AwaitingConfirm);
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
            let result = async {
                let hello = receive_manager_hello(&mut receive).await?;
                run_manager_after_hello(
                    &mut send,
                    &mut receive,
                    &mut manager_session,
                    hello,
                    b"manager-acceptance",
                    now,
                )
                .await
            }
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
