//! Ephemeral SPAKE2 pairing for the connector-owned data worker.
//!
//! The password, PAKE scalar, protocol state, and derived key remain inside
//! this process. Callers exchange only public shares and confirmation MACs.

use anyhow::{Context, Result, ensure};
use chacha20poly1305::{
    ChaCha20Poly1305, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use pakery_core::{
    SharedSecret,
    crypto::{CpaceGroup, Hash, Kdf},
};
use pakery_crypto::{HkdfSha256, P256Group, Sha256Hash, Spake2P256};
use pakery_spake2::{PartyA, PartyAState, PartyB, PartyBState, Spake2Output};
use rand_core::{OsRng, UnwrapErr};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

const APPLICATION_LABEL: &[u8] = b"agent-vmm/pairing";
const PROTOCOL_VERSION: &[u8] = b"1";
const SAS_LABEL: &[u8] = b"agent-vmm/pairing/1/sas";
const RECORD_LABEL: &[u8] = b"agent-vmm/pairing/1/record";
const MAX_CONTEXT_FIELD_BYTES: usize = 4096;
pub const MAX_ENCRYPTED_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_PAIRING_RECORDS_PER_DIRECTION: u64 = 16;

/// Public values bound into the SPAKE2 transcript.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedPairingContext {
    pub session_id: Vec<u8>,
    pub mesh_id: Vec<u8>,
    pub invite_revision: u64,
    pub manager_identity: Vec<u8>,
    pub joiner_identity: Vec<u8>,
    pub manager_root_key: Vec<u8>,
    pub joiner_root_key: Vec<u8>,
    pub roles_and_features: Vec<u8>,
}

impl OwnedPairingContext {
    pub fn as_borrowed(&self) -> PairingContext<'_> {
        PairingContext {
            session_id: &self.session_id,
            mesh_id: &self.mesh_id,
            invite_revision: self.invite_revision,
            manager_identity: &self.manager_identity,
            joiner_identity: &self.joiner_identity,
            manager_root_key: &self.manager_root_key,
            joiner_root_key: &self.joiner_root_key,
            roles_and_features: &self.roles_and_features,
        }
    }
}

pub struct PairingContext<'a> {
    pub session_id: &'a [u8],
    pub mesh_id: &'a [u8],
    pub invite_revision: u64,
    pub manager_identity: &'a [u8],
    pub joiner_identity: &'a [u8],
    pub manager_root_key: &'a [u8],
    pub joiner_root_key: &'a [u8],
    pub roles_and_features: &'a [u8],
}

impl PairingContext<'_> {
    pub(crate) fn validate(&self) -> Result<()> {
        self.aad().map(|_| ())
    }

    fn aad(&self) -> Result<Vec<u8>> {
        ensure!(
            !self.session_id.is_empty(),
            "pairing session id is required"
        );
        ensure!(!self.mesh_id.is_empty(), "pairing mesh id is required");

        let mut encoded = Vec::new();
        append_field(&mut encoded, APPLICATION_LABEL)?;
        append_field(&mut encoded, PROTOCOL_VERSION)?;
        append_field(&mut encoded, self.session_id)?;
        append_field(&mut encoded, self.mesh_id)?;
        encoded.extend_from_slice(&self.invite_revision.to_be_bytes());
        append_field(&mut encoded, self.manager_identity)?;
        append_field(&mut encoded, self.joiner_identity)?;
        append_field(&mut encoded, self.manager_root_key)?;
        append_field(&mut encoded, self.joiner_root_key)?;
        append_field(&mut encoded, self.roles_and_features)?;
        Ok(encoded)
    }
}

fn append_field(target: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    ensure!(
        value.len() <= MAX_CONTEXT_FIELD_BYTES,
        "pairing transcript field exceeds size limit"
    );
    let len = u32::try_from(value.len()).context("pairing transcript field length overflow")?;
    target.extend_from_slice(&len.to_be_bytes());
    target.extend_from_slice(value);
    Ok(())
}

pub struct PairingSecret(Zeroizing<Vec<u8>>);

impl core::fmt::Debug for PairingSecret {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("PairingSecret([REDACTED])")
    }
}

impl PairingSecret {
    pub fn from_pin(pin: &[u8]) -> Result<Self> {
        ensure!(
            pin.len() == 8 && pin.iter().all(u8::is_ascii_digit),
            "pairing PIN must contain exactly 8 digits"
        );
        Ok(Self(Zeroizing::new(pin.to_vec())))
    }

    pub fn from_qr_secret(secret: &[u8]) -> Result<Self> {
        ensure!(
            secret.len() == 16,
            "pairing QR secret must contain exactly 128 bits"
        );
        Ok(Self(Zeroizing::new(secret.to_vec())))
    }

    pub fn from_pin_owned(pin: Vec<u8>) -> Result<Self> {
        let pin = Zeroizing::new(pin);
        ensure!(
            pin.len() == 8 && pin.iter().all(u8::is_ascii_digit),
            "pairing PIN must contain exactly 8 digits"
        );
        Ok(Self(pin))
    }

    pub fn from_qr_secret_owned(secret: Vec<u8>) -> Result<Self> {
        let secret = Zeroizing::new(secret);
        ensure!(
            secret.len() == 16,
            "pairing QR secret must contain exactly 128 bits"
        );
        Ok(Self(secret))
    }
}

fn password_scalar(secret: &PairingSecret) -> Result<<P256Group as CpaceGroup>::Scalar> {
    let digest = Zeroizing::new(Sha256Hash::digest(&secret.0));
    let mut wide = Zeroizing::new([0u8; 64]);
    wide[32..].copy_from_slice(&digest);
    P256Group::scalar_from_wide_bytes(&wide[..])
        .context("failed to reduce pairing secret into P-256 scalar")
}

/// Initiator state. Dropping it erases the PAKE scalar and password scalar via
/// the upstream state type's `ZeroizeOnDrop` implementation.
pub struct InitiatorState {
    state: PartyAState<Spake2P256>,
    aad: Vec<u8>,
    initiator_share: Vec<u8>,
}

impl core::fmt::Debug for InitiatorState {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("InitiatorState([REDACTED])")
    }
}

/// Responder state before it receives the initiator share.
pub struct ResponderState {
    state: PartyBState<Spake2P256>,
    aad: Vec<u8>,
    responder_share: Vec<u8>,
}

impl core::fmt::Debug for ResponderState {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("ResponderState([REDACTED])")
    }
}

/// A completed key exchange awaiting explicit peer key confirmation.
pub struct PendingConfirmation {
    output: Spake2Output,
    aad: Vec<u8>,
    initiator_share: Vec<u8>,
    responder_share: Vec<u8>,
    is_initiator: bool,
}

impl core::fmt::Debug for PendingConfirmation {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("PendingConfirmation([REDACTED])")
    }
}

impl PendingConfirmation {
    pub fn confirmation_mac(&self) -> &[u8] {
        &self.output.confirmation_mac
    }

    /// The session key is unavailable until the peer proves possession.
    pub fn confirm(self, peer_confirmation_mac: &[u8]) -> Result<ConfirmedPairing> {
        self.output
            .verify_peer_confirmation(peer_confirmation_mac)
            .context("SPAKE2 peer key confirmation failed")?;
        let mut binding = Vec::new();
        append_field(&mut binding, &self.aad)?;
        append_field(&mut binding, &self.initiator_share)?;
        append_field(&mut binding, &self.responder_share)?;
        if self.is_initiator {
            append_field(&mut binding, &self.output.confirmation_mac)?;
            append_field(&mut binding, peer_confirmation_mac)?;
        } else {
            append_field(&mut binding, peer_confirmation_mac)?;
            append_field(&mut binding, &self.output.confirmation_mac)?;
        }
        let transcript_digest = Sha256Hash::digest(&binding);
        Ok(ConfirmedPairing {
            session_key: self.output.into_session_key(),
            transcript_digest,
        })
    }
}

/// Confirmed ephemeral pairing key. Debug output is redacted and the value is
/// automatically zeroized when dropped.
pub struct ConfirmedPairing {
    session_key: SharedSecret,
    transcript_digest: Vec<u8>,
}

impl core::fmt::Debug for ConfirmedPairing {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("ConfirmedPairing([REDACTED])")
    }
}

impl ConfirmedPairing {
    /// Derive the six-digit short authentication string shown on both devices.
    pub fn sas(&self, session_id: &[u8]) -> Result<String> {
        ensure!(
            !session_id.is_empty(),
            "pairing session id is required for SAS"
        );
        let prk = HkdfSha256::extract(SAS_LABEL, self.session_key.as_bytes());
        let mut info = self.transcript_digest.clone();
        append_field(&mut info, session_id)?;
        let bytes = HkdfSha256::expand(&prk, &info, 4).context("failed to derive pairing SAS")?;
        let value = u32::from_be_bytes(bytes.as_slice().try_into().expect("fixed HKDF length"));
        Ok(format!("{:06}", value % 1_000_000))
    }

    #[cfg(test)]
    fn key_bytes(&self) -> &[u8] {
        self.session_key.as_bytes()
    }

    pub fn into_record_layer(self, role: PairingRole) -> Result<PairingRecordLayer> {
        let prk = HkdfSha256::extract(RECORD_LABEL, self.session_key.as_bytes());
        let manager_to_joiner =
            derive_direction_key(&prk, &self.transcript_digest, b"manager-to-joiner")?;
        let joiner_to_manager =
            derive_direction_key(&prk, &self.transcript_digest, b"joiner-to-manager")?;
        let (outbound_key, inbound_key, outbound_direction, inbound_direction) = match role {
            PairingRole::Manager => (manager_to_joiner, joiner_to_manager, 0, 1),
            PairingRole::Joiner => (joiner_to_manager, manager_to_joiner, 1, 0),
        };
        Ok(PairingRecordLayer {
            outbound_key,
            inbound_key,
            transcript_digest: self.transcript_digest,
            outbound_direction,
            inbound_direction,
            outbound_sequence: 0,
            inbound_sequence: 0,
            failed: false,
        })
    }
}

fn derive_direction_key(
    prk: &[u8],
    transcript_digest: &[u8],
    direction: &[u8],
) -> Result<SharedSecret> {
    let mut info = transcript_digest.to_vec();
    append_field(&mut info, direction)?;
    Ok(SharedSecret::new(
        HkdfSha256::expand(prk, &info, 32)
            .context("failed to derive pairing record key")?
            .to_vec(),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairingRole {
    Manager,
    Joiner,
}

pub struct PairingRecordLayer {
    outbound_key: SharedSecret,
    inbound_key: SharedSecret,
    transcript_digest: Vec<u8>,
    outbound_direction: u8,
    inbound_direction: u8,
    outbound_sequence: u64,
    inbound_sequence: u64,
    failed: bool,
}

impl core::fmt::Debug for PairingRecordLayer {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("PairingRecordLayer([REDACTED])")
    }
}

impl PairingRecordLayer {
    pub fn seal(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        ensure!(!self.failed, "pairing record layer is terminated");
        ensure!(!plaintext.is_empty(), "pairing record must not be empty");
        ensure!(
            plaintext.len() <= MAX_ENCRYPTED_RECORD_BYTES,
            "pairing record exceeds plaintext size limit"
        );
        let sequence = self.outbound_sequence;
        ensure!(
            sequence < MAX_PAIRING_RECORDS_PER_DIRECTION,
            "pairing outbound record limit reached"
        );
        let nonce = record_nonce(sequence);
        let aad = record_aad(&self.transcript_digest, self.outbound_direction, sequence);
        let cipher = ChaCha20Poly1305::new_from_slice(self.outbound_key.as_bytes())
            .context("invalid pairing record key")?;
        let ciphertext = match cipher.encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        ) {
            Ok(ciphertext) => ciphertext,
            Err(_) => {
                self.failed = true;
                anyhow::bail!("failed to encrypt pairing record");
            }
        };
        self.outbound_sequence = sequence + 1;
        Ok(ciphertext)
    }

    pub fn open(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        ensure!(!self.failed, "pairing record layer is terminated");
        ensure!(
            ciphertext.len() > 16 && ciphertext.len() <= MAX_ENCRYPTED_RECORD_BYTES + 16,
            "pairing ciphertext is outside size bounds"
        );
        let sequence = self.inbound_sequence;
        ensure!(
            sequence < MAX_PAIRING_RECORDS_PER_DIRECTION,
            "pairing inbound record limit reached"
        );
        let nonce = record_nonce(sequence);
        let aad = record_aad(&self.transcript_digest, self.inbound_direction, sequence);
        let cipher = ChaCha20Poly1305::new_from_slice(self.inbound_key.as_bytes())
            .context("invalid pairing record key")?;
        let plaintext = match cipher.decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        ) {
            Ok(plaintext) => plaintext,
            Err(_) => {
                self.failed = true;
                anyhow::bail!("pairing record authentication failed");
            }
        };
        self.inbound_sequence = sequence + 1;
        Ok(plaintext)
    }
}

fn record_nonce(sequence: u64) -> Nonce {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&sequence.to_be_bytes());
    nonce.into()
}

fn record_aad(transcript_digest: &[u8], direction: u8, sequence: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(transcript_digest.len() + 9);
    aad.extend_from_slice(transcript_digest);
    aad.push(direction);
    aad.extend_from_slice(&sequence.to_be_bytes());
    aad
}

pub fn begin_initiator(
    secret: &PairingSecret,
    context: &PairingContext<'_>,
) -> Result<(Vec<u8>, InitiatorState)> {
    let w = password_scalar(secret)?;
    let aad = context.aad()?;
    let mut rng = UnwrapErr(OsRng);
    let (share, state) = PartyA::<Spake2P256>::start(
        &w,
        context.manager_identity,
        context.joiner_identity,
        &aad,
        &mut rng,
    )
    .context("failed to start SPAKE2 initiator")?;
    Ok((
        share.clone(),
        InitiatorState {
            state,
            aad,
            initiator_share: share,
        },
    ))
}

pub fn begin_responder(
    secret: &PairingSecret,
    context: &PairingContext<'_>,
) -> Result<(Vec<u8>, ResponderState)> {
    let w = password_scalar(secret)?;
    let aad = context.aad()?;
    let mut rng = UnwrapErr(OsRng);
    let (share, state) = PartyB::<Spake2P256>::start(
        &w,
        context.manager_identity,
        context.joiner_identity,
        &aad,
        &mut rng,
    )
    .context("failed to start SPAKE2 responder")?;
    Ok((
        share.clone(),
        ResponderState {
            state,
            aad,
            responder_share: share,
        },
    ))
}

pub fn finish_initiator(
    state: InitiatorState,
    responder_share: &[u8],
) -> Result<PendingConfirmation> {
    let output = state
        .state
        .finish(responder_share)
        .context("failed to finish SPAKE2 initiator")?;
    Ok(PendingConfirmation {
        output,
        aad: state.aad,
        initiator_share: state.initiator_share,
        responder_share: responder_share.to_vec(),
        is_initiator: true,
    })
}

pub fn finish_responder(
    state: ResponderState,
    initiator_share: &[u8],
) -> Result<PendingConfirmation> {
    let output = state
        .state
        .finish(initiator_share)
        .context("failed to finish SPAKE2 responder")?;
    Ok(PendingConfirmation {
        output,
        aad: state.aad,
        initiator_share: initiator_share.to_vec(),
        responder_share: state.responder_share,
        is_initiator: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context<'a>(session_id: &'a [u8], revision: u64) -> PairingContext<'a> {
        PairingContext {
            session_id,
            mesh_id: b"mesh-01",
            invite_revision: revision,
            manager_identity: b"device-manager",
            joiner_identity: b"device-joiner",
            manager_root_key: b"manager-p256-root-public-key",
            joiner_root_key: b"joiner-p256-root-public-key",
            roles_and_features: b"manager,host;service-tunnel-v1",
        }
    }

    fn exchange(
        secret_a: &[u8],
        secret_b: &[u8],
        context_a: &PairingContext<'_>,
        context_b: &PairingContext<'_>,
    ) -> Result<(PendingConfirmation, PendingConfirmation)> {
        let secret_a = PairingSecret::from_pin(secret_a)?;
        let secret_b = PairingSecret::from_pin(secret_b)?;
        let (share_a, state_a) = begin_initiator(&secret_a, context_a)?;
        let (share_b, state_b) = begin_responder(&secret_b, context_b)?;
        Ok((
            finish_initiator(state_a, &share_b)?,
            finish_responder(state_b, &share_a)?,
        ))
    }

    #[test]
    fn context_digest_matches_go_host_vector() {
        let digest = Sha256Hash::digest(&context(b"session-00000001", 7).aad().unwrap());
        assert_eq!(
            hex(&digest),
            "7b9129ee42293b8037afcbd0442e03796d46909987bcbad116c741f66217a004"
        );
    }

    #[test]
    fn matching_secret_and_context_produce_confirmed_key_and_sas() {
        let ctx = context(b"0123456789abcdef", 42);
        let (pending_a, pending_b) = exchange(b"12345678", b"12345678", &ctx, &ctx).unwrap();
        let mac_a = pending_a.confirmation_mac().to_vec();
        let mac_b = pending_b.confirmation_mac().to_vec();
        let confirmed_a = pending_a.confirm(&mac_b).unwrap();
        let confirmed_b = pending_b.confirm(&mac_a).unwrap();
        assert_eq!(confirmed_a.key_bytes(), confirmed_b.key_bytes());
        assert_eq!(
            confirmed_a.sas(ctx.session_id).unwrap(),
            confirmed_b.sas(ctx.session_id).unwrap()
        );
        assert_eq!(confirmed_a.sas(ctx.session_id).unwrap().len(), 6);
    }

    #[test]
    fn wrong_secret_fails_key_confirmation() {
        let ctx = context(b"0123456789abcdef", 42);
        let (pending_a, pending_b) = exchange(b"12345678", b"87654321", &ctx, &ctx).unwrap();
        assert!(pending_a.confirm(pending_b.confirmation_mac()).is_err());
    }

    #[test]
    fn transcript_revision_mismatch_fails_key_confirmation() {
        let ctx_a = context(b"0123456789abcdef", 42);
        let ctx_b = context(b"0123456789abcdef", 43);
        let (pending_a, pending_b) = exchange(b"12345678", b"12345678", &ctx_a, &ctx_b).unwrap();
        assert!(pending_a.confirm(pending_b.confirmation_mac()).is_err());
    }

    #[test]
    fn invalid_and_identity_peer_points_are_rejected() {
        let ctx = context(b"0123456789abcdef", 42);
        let secret = PairingSecret::from_pin(b"12345678").unwrap();
        let (_, state) = begin_initiator(&secret, &ctx).unwrap();
        assert!(finish_initiator(state, &[0xff; 33]).is_err());

        let (_, state) = begin_initiator(&secret, &ctx).unwrap();
        assert!(finish_initiator(state, &[0; 33]).is_err());
    }

    #[test]
    fn debug_never_exposes_ephemeral_state_or_key() {
        let ctx = context(b"0123456789abcdef", 42);
        let secret = PairingSecret::from_pin(b"12345678").unwrap();
        assert_eq!(format!("{secret:?}"), "PairingSecret([REDACTED])");
        let (_, state) = begin_initiator(&secret, &ctx).unwrap();
        assert_eq!(format!("{state:?}"), "InitiatorState([REDACTED])");
    }

    #[test]
    fn confirmed_record_layer_is_directional_ordered_and_authenticated() {
        let ctx = context(b"0123456789abcdef", 42);
        let (pending_manager, pending_joiner) =
            exchange(b"12345678", b"12345678", &ctx, &ctx).unwrap();
        let manager_mac = pending_manager.confirmation_mac().to_vec();
        let joiner_mac = pending_joiner.confirmation_mac().to_vec();
        let mut manager = pending_manager
            .confirm(&joiner_mac)
            .unwrap()
            .into_record_layer(PairingRole::Manager)
            .unwrap();
        let mut joiner = pending_joiner
            .confirm(&manager_mac)
            .unwrap()
            .into_record_layer(PairingRole::Joiner)
            .unwrap();

        let proposal = manager.seal(b"identity-and-permission-proposal").unwrap();
        assert_eq!(
            joiner.open(&proposal).unwrap(),
            b"identity-and-permission-proposal"
        );
        let acceptance = joiner.seal(b"joiner-acceptance").unwrap();
        assert_eq!(manager.open(&acceptance).unwrap(), b"joiner-acceptance");
        assert!(
            joiner.open(&proposal).is_err(),
            "replay must fail at next sequence"
        );
        assert!(joiner.seal(b"must-not-continue").is_err());
    }

    #[test]
    fn record_tampering_and_wrong_direction_fail_closed() {
        let ctx = context(b"0123456789abcdef", 42);
        let (pending_a, pending_b) = exchange(b"12345678", b"12345678", &ctx, &ctx).unwrap();
        let mac_a = pending_a.confirmation_mac().to_vec();
        let mac_b = pending_b.confirmation_mac().to_vec();
        let mut manager = pending_a
            .confirm(&mac_b)
            .unwrap()
            .into_record_layer(PairingRole::Manager)
            .unwrap();
        let mut wrong_role = pending_b
            .confirm(&mac_a)
            .unwrap()
            .into_record_layer(PairingRole::Manager)
            .unwrap();
        let mut ciphertext = manager.seal(b"proposal").unwrap();
        ciphertext[0] ^= 1;
        assert!(wrong_role.open(&ciphertext).is_err());
        assert!(wrong_role.open(&ciphertext).is_err());
    }

    #[test]
    fn pairing_secret_formats_are_strict() {
        assert!(PairingSecret::from_pin(b"12345678").is_ok());
        assert!(PairingSecret::from_pin(b"1234567").is_err());
        assert!(PairingSecret::from_pin(b"1234567x").is_err());
        assert!(PairingSecret::from_qr_secret(&[7; 16]).is_ok());
        assert!(PairingSecret::from_qr_secret(&[7; 15]).is_err());
    }

    #[test]
    fn record_count_is_bounded_before_nonce_reuse() {
        let ctx = context(b"0123456789abcdef", 42);
        let (pending_a, pending_b) = exchange(b"12345678", b"12345678", &ctx, &ctx).unwrap();
        let mac_a = pending_a.confirmation_mac().to_vec();
        let mac_b = pending_b.confirmation_mac().to_vec();
        let mut manager = pending_a
            .confirm(&mac_b)
            .unwrap()
            .into_record_layer(PairingRole::Manager)
            .unwrap();
        let mut joiner = pending_b
            .confirm(&mac_a)
            .unwrap()
            .into_record_layer(PairingRole::Joiner)
            .unwrap();
        for _ in 0..MAX_PAIRING_RECORDS_PER_DIRECTION {
            let record = manager.seal(b"bounded-record").unwrap();
            assert_eq!(joiner.open(&record).unwrap(), b"bounded-record");
        }
        assert!(manager.seal(b"one-too-many").is_err());
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn deterministic_suite_vector() {
        let ctx = context(b"0123456789abcdef", 42);
        let secret = PairingSecret::from_pin(b"12345678").unwrap();
        let w = password_scalar(&secret).unwrap();
        let x = P256Group::scalar_from_wide_bytes(&[0x11; 64]).unwrap();
        let y = P256Group::scalar_from_wide_bytes(&[0x22; 64]).unwrap();
        let aad = ctx.aad().unwrap();
        let (share_a, state_a) = PartyA::<Spake2P256>::start_with_scalar(
            &w,
            &x,
            ctx.manager_identity,
            ctx.joiner_identity,
            &aad,
        )
        .unwrap();
        let (share_b, state_b) = PartyB::<Spake2P256>::start_with_scalar(
            &w,
            &y,
            ctx.manager_identity,
            ctx.joiner_identity,
            &aad,
        )
        .unwrap();
        let pending_a = PendingConfirmation {
            output: state_a.finish(&share_b).unwrap(),
            aad: aad.clone(),
            initiator_share: share_a.clone(),
            responder_share: share_b.clone(),
            is_initiator: true,
        };
        let pending_b = PendingConfirmation {
            output: state_b.finish(&share_a).unwrap(),
            aad,
            initiator_share: share_a.clone(),
            responder_share: share_b.clone(),
            is_initiator: false,
        };
        let mac_a = pending_a.confirmation_mac().to_vec();
        let mac_b = pending_b.confirmation_mac().to_vec();
        let confirmed_a = pending_a.confirm(&mac_b).unwrap();
        let confirmed_b = pending_b.confirm(&mac_a).unwrap();
        assert_eq!(confirmed_a.key_bytes(), confirmed_b.key_bytes());

        assert_eq!(
            hex(&share_a),
            "04a710d33644a8522f5ae7efa0aeb2b200fef399a52c9b43f8acd474801602c4bef4eef3e04ca610f2b6e90d581c884c1f5b7ed9da8df635cb4e8f543a46b0fbd2"
        );
        assert_eq!(
            hex(&share_b),
            "043bdde474770790f4163b5a03b582865a7dcc35c0c6ac159b38a30e5695e9a20dba1a7a7281b1d93b92d861bb25d3ca9de7ec03cd50b129b54a484e2d2ce978be"
        );
        assert_eq!(
            hex(&mac_a),
            "0dd1ef7bf8374534e4bd89db14d3c7159adc45ee72ca1ed5f1c4b22467105c77"
        );
        assert_eq!(
            hex(&mac_b),
            "15a2c887959e9ebe1b7b540b906f09c246cdd846f2e14aafeed73ae39843dea4"
        );
        assert_eq!(
            hex(confirmed_a.key_bytes()),
            "6aaf6dd7812528c67dddf99e259e8c2c"
        );
        assert_eq!(confirmed_a.sas(ctx.session_id).unwrap(), "108203");
    }
}
