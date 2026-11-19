use crate::{Error, IdentitySigner, Result, sign};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde::Deserialize;
use std::fmt::Write as _;

pub const RECOVERY_PURPOSE: &str = "agent-vmm/credential-replace/1";
const TTL_SECONDS: i64 = 120;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryChallenge {
    pub purpose: String,
    pub audience: String,
    pub subject: String,
    pub session_id: String,
    pub controller_binding: String,
    pub environment_id: String,
    pub registration_id: String,
    pub candidate_credential_hash: String,
    pub nonce: String,
    pub revision: u64,
    pub expires_at: i64,
}

impl RecoveryChallenge {
    pub fn parse(json: &[u8], now_unix: i64) -> Result<Self> {
        if json.len() > 4096 {
            return Err(Error::Challenge);
        }
        let challenge: Self = serde_json::from_slice(json).map_err(|_| Error::Challenge)?;
        challenge.validate(now_unix)?;
        Ok(challenge)
    }

    fn bindings(&self) -> [&str; 8] {
        [
            &self.audience,
            &self.subject,
            &self.session_id,
            &self.controller_binding,
            &self.environment_id,
            &self.registration_id,
            &self.candidate_credential_hash,
            &self.nonce,
        ]
    }

    fn validate(&self, now_unix: i64) -> Result<()> {
        if self.purpose != RECOVERY_PURPOSE
            || self.revision == 0
            || self.expires_at <= now_unix
            || self.expires_at > now_unix.saturating_add(TTL_SECONDS)
            || !STANDARD
                .decode(&self.candidate_credential_hash)
                .is_ok_and(|hash| hash.len() == 32)
            || !URL_SAFE_NO_PAD
                .decode(&self.nonce)
                .is_ok_and(|nonce| nonce.len() == 32)
        {
            return Err(Error::Challenge);
        }
        for (index, value) in self.bindings().into_iter().enumerate() {
            let max = if index == 3 { 1500 } else { 500 };
            if value.is_empty() || value.len() > max {
                return Err(Error::Challenge);
            }
        }
        Ok(())
    }

    pub fn canonical_bytes(&self, now_unix: i64) -> Result<Vec<u8>> {
        self.validate(now_unix)?;
        let mut wire = String::from(RECOVERY_PURPOSE);
        wire.push('\n');
        for value in self.bindings() {
            URL_SAFE_NO_PAD.encode_string(value, &mut wire);
            wire.push('\n');
        }
        write!(&mut wire, "{}\n{}\n", self.revision, self.expires_at).expect("writing to String");
        Ok(wire.into_bytes())
    }

    pub fn sign(&self, signer: &dyn IdentitySigner, now_unix: i64) -> Result<[u8; 64]> {
        sign(signer, &self.canonical_bytes(now_unix)?)
    }
}
