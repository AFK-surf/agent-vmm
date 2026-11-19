//! Proof that a Guest holds the token of the boot configuration it serves.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::Secret;

pub const NONCE_LEN: usize = 16;
pub const PROOF_LEN: usize = 32;

/// HMAC-SHA256 over the instance and the caller's nonce, keyed by the token.
///
/// It detects a Guest that did not load this boot configuration, such as a
/// previous occupant of a reused address. It does not resist a peer that
/// already received the token in a request.
pub fn boot_token_proof(
    token: &Secret,
    instance_id: &str,
    nonce: &[u8; NONCE_LEN],
) -> [u8; PROOF_LEN] {
    let mut mac = Hmac::<Sha256>::new_from_slice(token.expose().as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(b"vmm-guest-init/health/v1\0");
    mac.update(instance_id.as_bytes());
    mac.update(&[0]);
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

pub fn verify_boot_token_proof(
    token: &Secret,
    instance_id: &str,
    nonce: &[u8; NONCE_LEN],
    proof: &[u8; PROOF_LEN],
) -> bool {
    boot_token_proof(token, instance_id, nonce)
        .ct_eq(proof)
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_binds_token_instance_and_nonce() {
        let token = Secret::new("0123456789abcdef0123456789abcdef");
        let proof = boot_token_proof(&token, "vm-1", &[7; NONCE_LEN]);
        assert!(verify_boot_token_proof(
            &token,
            "vm-1",
            &[7; NONCE_LEN],
            &proof
        ));
        assert!(!verify_boot_token_proof(
            &token,
            "vm-2",
            &[7; NONCE_LEN],
            &proof
        ));
        assert!(!verify_boot_token_proof(
            &token,
            "vm-1",
            &[8; NONCE_LEN],
            &proof
        ));
        let other = Secret::new("fedcba9876543210fedcba9876543210");
        assert!(!verify_boot_token_proof(
            &other,
            "vm-1",
            &[7; NONCE_LEN],
            &proof
        ));
    }
}
