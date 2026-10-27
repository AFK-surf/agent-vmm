//! The memory owner for one invitation and its authenticated attempt.

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
    Invited,
    Handshaking,
    AwaitingConfirm,
    ReadyToCommit,
    Committed,
    Rejected,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairingFailure {
    ContextMismatch,
    Timeout,
    AuthenticationFailed,
    ProtocolViolation,
    ConnectionLost,
}

pub struct PairingSession {
    invitation: OwnedPairingContext,
    attempt: Option<OwnedPairingContext>,
    secret: Option<PairingSecret>,
    deadline: Instant,
    attempts: u8,
    state: PairingSessionState,
    local_confirmed: bool,
    peer_confirmed: bool,
    last_failure: Option<PairingFailure>,
}

impl PairingSession {
    pub fn new(context: OwnedPairingContext, secret: PairingSecret, now: Instant) -> Result<Self> {
        context.as_borrowed().validate()?;
        Ok(Self {
            invitation: context,
            attempt: None,
            secret: Some(secret),
            deadline: now + PAIRING_TTL,
            attempts: 0,
            state: PairingSessionState::Invited,
            local_confirmed: false,
            peer_confirmed: false,
            last_failure: None,
        })
    }

    pub fn state(&self) -> PairingSessionState {
        self.state
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn last_failure(&self) -> Option<PairingFailure> {
        self.last_failure
    }
    pub fn context(&self) -> &OwnedPairingContext {
        self.attempt.as_ref().unwrap_or(&self.invitation)
    }
    pub fn attempts_remaining(&self) -> u8 {
        MAX_PAIRING_ATTEMPTS.saturating_sub(self.attempts)
    }

    pub fn begin_manager_attempt(
        &mut self,
        offered: &OwnedPairingContext,
        now: Instant,
    ) -> Result<()> {
        self.begin(now)?;
        if self.invitation.session_id != offered.session_id
            || self.invitation.mesh_id != offered.mesh_id
            || self.invitation.invite_revision != offered.invite_revision
            || self.invitation.manager_identity != offered.manager_identity
            || self.invitation.manager_root_key != offered.manager_root_key
            || self.invitation.roles_and_features != offered.roles_and_features
            || offered.joiner_identity.is_empty()
            || offered.joiner_root_key.is_empty()
        {
            self.attempt_failed(PairingFailure::ContextMismatch, now);
            anyhow::bail!("pairing context does not match invitation");
        }
        offered.as_borrowed().validate()?;
        self.attempt = Some(offered.clone());
        Ok(())
    }

    pub fn begin_initiator(&mut self, now: Instant) -> Result<(Vec<u8>, InitiatorState)> {
        self.begin(now)?;
        begin_initiator(
            self.secret.as_ref().expect("active pairing secret"),
            &self.context().as_borrowed(),
        )
    }

    pub fn begin_responder(&mut self) -> Result<(Vec<u8>, ResponderState)> {
        ensure!(
            self.state == PairingSessionState::Handshaking && self.attempt.is_some(),
            "pairing attempt is unavailable"
        );
        begin_responder(
            self.secret.as_ref().expect("active pairing secret"),
            &self.context().as_borrowed(),
        )
    }

    fn begin(&mut self, now: Instant) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::Invited,
            "pairing is not invited"
        );
        ensure!(
            self.attempts < MAX_PAIRING_ATTEMPTS,
            "pairing attempts exhausted"
        );
        self.attempts += 1;
        self.state = PairingSessionState::Handshaking;
        Ok(())
    }

    pub fn attempt_failed(&mut self, failure: PairingFailure, now: Instant) {
        self.expire_if_needed(now);
        if self.terminal() {
            return;
        }
        self.attempt = None;
        self.last_failure = Some(failure);
        if self.attempts >= MAX_PAIRING_ATTEMPTS {
            self.terminate(PairingSessionState::Rejected);
        } else {
            self.state = PairingSessionState::Invited;
        }
    }

    pub fn key_confirmed(&mut self, now: Instant) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::Handshaking,
            "pairing key confirmation is out of order"
        );
        self.secret = None;
        self.state = PairingSessionState::AwaitingConfirm;
        Ok(())
    }

    pub fn confirm_local(&mut self, now: Instant) -> Result<()> {
        self.confirm(now, true)
    }

    pub fn confirm_peer(&mut self, now: Instant) -> Result<()> {
        if self.peer_confirmed {
            self.reject(Some(PairingFailure::ProtocolViolation));
            anyhow::bail!("duplicate peer confirmation");
        }
        self.confirm(now, false)
    }

    fn confirm(&mut self, now: Instant, local: bool) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::AwaitingConfirm,
            "pairing confirmation is out of order"
        );
        if local {
            ensure!(
                !self.local_confirmed,
                "local pairing confirmation already recorded"
            );
            self.local_confirmed = true;
        } else {
            self.peer_confirmed = true;
        }
        if self.local_confirmed && self.peer_confirmed {
            self.state = PairingSessionState::ReadyToCommit;
        }
        Ok(())
    }

    pub fn registry_committed(&mut self, now: Instant) -> Result<()> {
        self.expire_if_needed(now);
        ensure!(
            self.state == PairingSessionState::ReadyToCommit,
            "pairing is not ready for registry CAS"
        );
        self.terminate(PairingSessionState::Committed);
        Ok(())
    }

    pub fn reject(&mut self, failure: Option<PairingFailure>) {
        self.last_failure = failure;
        self.terminate(PairingSessionState::Rejected);
    }

    pub fn expire_if_needed(&mut self, now: Instant) {
        if now >= self.deadline && !self.terminal() {
            self.last_failure = Some(PairingFailure::Timeout);
            self.terminate(PairingSessionState::Expired);
        }
    }

    pub fn terminal(&self) -> bool {
        matches!(
            self.state,
            PairingSessionState::Committed
                | PairingSessionState::Rejected
                | PairingSessionState::Expired
        )
    }

    fn terminate(&mut self, state: PairingSessionState) {
        self.secret = None;
        self.attempt = None;
        self.local_confirmed = false;
        self.peer_confirmed = false;
        self.state = state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub fn context() -> OwnedPairingContext {
        OwnedPairingContext {
            session_id: vec![1; 16],
            mesh_id: b"mesh".to_vec(),
            invite_revision: 1,
            manager_identity: b"manager".to_vec(),
            manager_root_key: vec![2; 33],
            joiner_identity: b"joiner".to_vec(),
            joiner_root_key: vec![3; 33],
            roles_and_features: vec![1],
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
    fn peer_confirmation_cannot_confirm_the_local_operator() {
        let now = Instant::now();
        let mut session = session(now);
        session.begin_initiator(now).unwrap();
        session.key_confirmed(now).unwrap();
        session.confirm_peer(now).unwrap();
        assert_eq!(session.state(), PairingSessionState::AwaitingConfirm);
        assert!(session.registry_committed(now).is_err());
        assert!(session.confirm_peer(now).is_err());
        assert_eq!(
            session.last_failure(),
            Some(PairingFailure::ProtocolViolation)
        );
        assert_eq!(session.state(), PairingSessionState::Rejected);
    }
    #[test]
    fn failed_attempt_does_not_bind_the_invitation_to_an_attacker() {
        let now = Instant::now();
        let mut invited = context();
        invited.joiner_identity.clear();
        invited.joiner_root_key.clear();
        let mut session = PairingSession::new(
            invited.clone(),
            PairingSecret::from_pin(b"12345678").unwrap(),
            now,
        )
        .unwrap();
        let mut attacker = context();
        attacker.joiner_identity = b"attacker".to_vec();
        session.begin_manager_attempt(&attacker, now).unwrap();
        session.attempt_failed(PairingFailure::AuthenticationFailed, now);
        assert_eq!(session.context(), &invited);
        assert_eq!(session.attempts_remaining(), 4);
        assert_eq!(
            session.last_failure(),
            Some(PairingFailure::AuthenticationFailed)
        );
        session.begin_manager_attempt(&context(), now).unwrap();
        session.key_confirmed(now).unwrap();
        session.confirm_peer(now).unwrap();
        session.confirm_local(now).unwrap();
        session.registry_committed(now).unwrap();
        assert_eq!(session.state(), PairingSessionState::Committed);
    }
    #[test]
    fn permissions_are_fixed_and_attempts_and_time_are_bounded() {
        let now = Instant::now();
        let mut session = session(now);
        let mut offered = context();
        offered.roles_and_features.push(2);
        for _ in 0..MAX_PAIRING_ATTEMPTS {
            assert!(session.begin_manager_attempt(&offered, now).is_err());
        }
        assert_eq!(session.state(), PairingSessionState::Rejected);
        assert_eq!(session.attempts_remaining(), 0);
        let mut session = self::session(now);
        session.expire_if_needed(now + PAIRING_TTL);
        assert_eq!(session.state(), PairingSessionState::Expired);
        assert!(session.begin_initiator(now + PAIRING_TTL).is_err());
    }
}
