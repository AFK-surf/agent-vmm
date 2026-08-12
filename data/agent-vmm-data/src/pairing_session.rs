//! In-memory lifecycle owner for one personal-mesh pairing invite.
//!
//! This state is deliberately not serializable. A worker restart drops the
//! PIN/QR secret, PAKE attempt state, derived key, and local confirmations.

use crate::pairing::{
    InitiatorState, OwnedPairingContext, PairingSecret, ResponderState, begin_initiator,
    begin_responder,
};
use anyhow::{Result, ensure};
use serde::Serialize;
use std::time::{Duration, Instant};

pub const PAIRING_TTL: Duration = Duration::from_secs(10 * 60);
pub const MAX_PAIRING_ATTEMPTS: u8 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairingSessionState {
    Idle,
    PakePending,
    KeyConfirmed,
    AwaitingLocalConfirmation,
    ReadyToCommit,
    Committed,
    Rejected,
    Expired,
}

pub struct PairingSession {
    context: OwnedPairingContext,
    secret: Option<PairingSecret>,
    deadline: Instant,
    attempts: u8,
    state: PairingSessionState,
    manager_confirmed: bool,
    joiner_confirmed: bool,
}

impl core::fmt::Debug for PairingSession {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("PairingSession")
            .field("session_id", &hex::encode(&self.context.session_id))
            .field("attempts", &self.attempts)
            .field("state", &self.state)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl PairingSession {
    pub fn new(context: OwnedPairingContext, secret: PairingSecret, now: Instant) -> Result<Self> {
        context.as_borrowed().validate()?;
        Ok(Self {
            context,
            secret: Some(secret),
            deadline: now + PAIRING_TTL,
            attempts: 0,
            state: PairingSessionState::Idle,
            manager_confirmed: false,
            joiner_confirmed: false,
        })
    }

    pub fn state(&self) -> PairingSessionState {
        self.state
    }

    pub fn context(&self) -> &OwnedPairingContext {
        &self.context
    }

    pub fn attempts_remaining(&self) -> u8 {
        MAX_PAIRING_ATTEMPTS.saturating_sub(self.attempts)
    }

    /// Bind the joiner's public identity before the manager computes its PAKE
    /// share. The fixed invite fields must already match; an invite can adopt
    /// a joiner only once and the resulting full context is PAKE-bound.
    pub fn bind_joiner_context(&mut self, offered: &OwnedPairingContext) -> Result<()> {
        ensure!(
            self.state == PairingSessionState::Idle,
            "pairing session is not idle"
        );
        ensure!(
            self.context.session_id == offered.session_id
                && self.context.mesh_id == offered.mesh_id
                && self.context.invite_revision == offered.invite_revision
                && self.context.manager_identity == offered.manager_identity
                && self.context.manager_root_key == offered.manager_root_key,
            "pairing invite context does not match manager"
        );
        ensure!(
            !offered.joiner_identity.is_empty() && !offered.joiner_root_key.is_empty(),
            "pairing joiner identity is required"
        );
        if self.context.joiner_identity.is_empty() && self.context.joiner_root_key.is_empty() {
            self.context.joiner_identity = offered.joiner_identity.clone();
            self.context.joiner_root_key = offered.joiner_root_key.clone();
            self.context.roles_and_features = offered.roles_and_features.clone();
        } else {
            ensure!(
                self.context.joiner_identity == offered.joiner_identity
                    && self.context.joiner_root_key == offered.joiner_root_key
                    && self.context.roles_and_features == offered.roles_and_features,
                "pairing invite is already bound to another joiner"
            );
        }
        self.context.as_borrowed().validate()
    }

    pub fn begin_initiator(&mut self, now: Instant) -> Result<(Vec<u8>, InitiatorState)> {
        self.begin(now)?;
        begin_initiator(
            self.secret.as_ref().expect("active pairing has secret"),
            &self.context.as_borrowed(),
        )
    }

    pub fn begin_responder(&mut self, now: Instant) -> Result<(Vec<u8>, ResponderState)> {
        self.begin(now)?;
        begin_responder(
            self.secret.as_ref().expect("active pairing has secret"),
            &self.context.as_borrowed(),
        )
    }

    fn begin(&mut self, now: Instant) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::Idle,
            "pairing session is not idle"
        );
        ensure!(
            self.attempts < MAX_PAIRING_ATTEMPTS,
            "pairing attempt budget exhausted"
        );
        self.attempts += 1;
        self.state = PairingSessionState::PakePending;
        Ok(())
    }

    pub fn attempt_failed(&mut self, now: Instant) {
        self.expire_if_needed(now);
        if self.state == PairingSessionState::Expired {
            return;
        }
        if self.attempts >= MAX_PAIRING_ATTEMPTS {
            self.terminate(PairingSessionState::Rejected);
        } else {
            self.state = PairingSessionState::Idle;
        }
    }

    pub fn key_confirmed(&mut self, now: Instant) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::PakePending,
            "pairing key confirmation is out of order"
        );
        self.state = PairingSessionState::KeyConfirmed;
        self.state = PairingSessionState::AwaitingLocalConfirmation;
        Ok(())
    }

    pub fn confirm_manager(&mut self, now: Instant) -> Result<()> {
        self.confirm(now, true)
    }

    pub fn confirm_joiner(&mut self, now: Instant) -> Result<()> {
        self.confirm(now, false)
    }

    fn confirm(&mut self, now: Instant, manager: bool) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::AwaitingLocalConfirmation,
            "pairing local confirmation is out of order"
        );
        if manager {
            self.manager_confirmed = true;
        } else {
            self.joiner_confirmed = true;
        }
        if self.manager_confirmed && self.joiner_confirmed {
            self.state = PairingSessionState::ReadyToCommit;
        }
        Ok(())
    }

    /// A registry CAS success is the only transition that creates membership.
    pub fn registry_committed(&mut self, now: Instant) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::ReadyToCommit,
            "pairing is not ready for registry CAS"
        );
        self.terminate(PairingSessionState::Committed);
        Ok(())
    }

    pub fn reject(&mut self) {
        self.terminate(PairingSessionState::Rejected);
    }

    pub fn expire_if_needed(&mut self, now: Instant) {
        if now >= self.deadline && !self.terminal() {
            self.terminate(PairingSessionState::Expired);
        }
    }

    fn terminal(&self) -> bool {
        matches!(
            self.state,
            PairingSessionState::Committed
                | PairingSessionState::Rejected
                | PairingSessionState::Expired
        )
    }

    fn terminate(&mut self, state: PairingSessionState) {
        self.secret = None;
        self.manager_confirmed = false;
        self.joiner_confirmed = false;
        self.state = state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> OwnedPairingContext {
        OwnedPairingContext {
            session_id: vec![1; 16],
            mesh_id: b"mesh-1".to_vec(),
            invite_revision: 7,
            manager_identity: b"manager".to_vec(),
            joiner_identity: b"joiner".to_vec(),
            manager_root_key: vec![2; 33],
            joiner_root_key: vec![3; 33],
            roles_and_features: b"use-services".to_vec(),
        }
    }

    fn session(now: Instant) -> PairingSession {
        PairingSession::new(
            context(),
            PairingSecret::from_pin(b"12345678").unwrap(),
            now,
        )
        .unwrap()
    }

    #[test]
    fn five_failed_attempts_destroy_the_secret_and_close_the_session() {
        let now = Instant::now();
        let mut session = session(now);
        for expected_remaining in (0..MAX_PAIRING_ATTEMPTS).rev() {
            session.begin_initiator(now).unwrap();
            assert_eq!(session.attempts_remaining(), expected_remaining);
            session.attempt_failed(now);
        }
        assert_eq!(session.state(), PairingSessionState::Rejected);
        assert!(session.begin_initiator(now).is_err());
        assert!(!format!("{session:?}").contains("12345678"));
    }

    #[test]
    fn timeout_destroys_secret_and_rejects_late_confirmation() {
        let now = Instant::now();
        let mut session = session(now);
        session.begin_responder(now).unwrap();
        session.expire_if_needed(now + PAIRING_TTL);
        assert_eq!(session.state(), PairingSessionState::Expired);
        assert!(session.key_confirmed(now + PAIRING_TTL).is_err());
    }

    #[test]
    fn both_local_confirmations_and_registry_cas_are_required() {
        let now = Instant::now();
        let mut session = session(now);
        session.begin_initiator(now).unwrap();
        session.key_confirmed(now).unwrap();
        session.confirm_manager(now).unwrap();
        assert_eq!(
            session.state(),
            PairingSessionState::AwaitingLocalConfirmation
        );
        assert!(session.registry_committed(now).is_err());
        session.confirm_joiner(now).unwrap();
        assert_eq!(session.state(), PairingSessionState::ReadyToCommit);
        session.registry_committed(now).unwrap();
        assert_eq!(session.state(), PairingSessionState::Committed);
    }

    #[test]
    fn restart_has_no_recoverable_pairing_state() {
        let now = Instant::now();
        let mut before_restart = session(now);
        before_restart.begin_initiator(now).unwrap();
        drop(before_restart);

        // The worker owns no serializer or durable store for PairingSession.
        // A replacement process has to receive a newly generated invite.
        let replacement = session(now);
        assert_eq!(replacement.state(), PairingSessionState::Idle);
        assert_eq!(replacement.attempts_remaining(), MAX_PAIRING_ATTEMPTS);
    }

    #[test]
    fn manager_invite_binds_exactly_one_joiner_before_pake() {
        let now = Instant::now();
        let mut manager_context = context();
        manager_context.joiner_identity.clear();
        manager_context.joiner_root_key.clear();
        manager_context.roles_and_features.clear();
        let mut session = PairingSession::new(
            manager_context,
            PairingSecret::from_pin(b"12345678").unwrap(),
            now,
        )
        .unwrap();
        let offered = context();
        session.bind_joiner_context(&offered).unwrap();
        assert_eq!(session.context(), &offered);
        let mut attacker = offered;
        attacker.joiner_identity = b"attacker".to_vec();
        assert!(session.bind_joiner_context(&attacker).is_err());
    }
}
