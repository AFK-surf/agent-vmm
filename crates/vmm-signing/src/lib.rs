mod canonical;
mod credentials;
mod recovery;

pub use canonical::{SignedKind, SignedMessage};
pub use credentials::*;
pub use recovery::{RECOVERY_PURPOSE, RecoveryChallenge};

use p256::{
    ecdsa::{
        Signature, SigningKey, VerifyingKey,
        signature::hazmat::{PrehashSigner, PrehashVerifier},
    },
    elliptic_curve::rand_core::OsRng,
};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid signed protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),
    #[error("invalid signed protobuf descriptor")]
    Descriptor,
    #[error("unknown protobuf fields in {0}")]
    UnknownFields(String),
    #[error("map field in signed protobuf")]
    MapField,
    #[error("invalid P-256 key")]
    Key,
    #[error("invalid P-256 signature")]
    Signature,
    #[error("non-canonical high-S P-256 signature")]
    HighS,
    #[error("invalid recovery challenge")]
    Challenge,
    #[error("credential storage is unavailable")]
    Unavailable,
    #[error("invalid credential storage configuration")]
    StorageMode,
    #[error("invalid credential size")]
    CredentialSize,
    #[error("credential storage I/O: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Device identity signing over SHA-256. A platform adapter can keep the key non-exportable.
pub trait IdentitySigner: Send + Sync {
    fn public_key(&self) -> Result<[u8; 33]>;
    fn sign_digest(&self, digest: &[u8; 32]) -> Result<[u8; 64]>;
}

pub struct FileIdentityKey(SigningKey);

impl FileIdentityKey {
    pub fn generate() -> Self {
        Self(SigningKey::random(&mut OsRng))
    }

    /// Reads the SEC1 DER encoding used by existing file-backed device roots.
    pub fn from_sec1_der(bytes: &[u8]) -> Result<Self> {
        let secret = p256::SecretKey::from_sec1_der(bytes).map_err(|_| Error::Key)?;
        Ok(Self(SigningKey::from(secret)))
    }

    pub fn to_sec1_der(&self) -> Result<zeroize::Zeroizing<Vec<u8>>> {
        p256::SecretKey::from(&self.0)
            .to_sec1_der()
            .map_err(|_| Error::Key)
    }
}

impl IdentitySigner for FileIdentityKey {
    fn public_key(&self) -> Result<[u8; 33]> {
        self.0
            .verifying_key()
            .to_encoded_point(true)
            .as_bytes()
            .try_into()
            .map_err(|_| Error::Key)
    }

    fn sign_digest(&self, digest: &[u8; 32]) -> Result<[u8; 64]> {
        let signature: Signature = self.0.sign_prehash(digest).map_err(|_| Error::Signature)?;
        Ok(signature
            .normalize_s()
            .unwrap_or(signature)
            .to_bytes()
            .into())
    }
}

pub fn sign(signer: &dyn IdentitySigner, canonical: &[u8]) -> Result<[u8; 64]> {
    let digest = Sha256::digest(canonical).into();
    let signature =
        Signature::from_slice(&signer.sign_digest(&digest)?).map_err(|_| Error::Signature)?;
    Ok(signature
        .normalize_s()
        .unwrap_or(signature)
        .to_bytes()
        .into())
}

pub fn verify(public_key: &[u8], canonical: &[u8], signature: &[u8]) -> Result<()> {
    if public_key.len() != 33 || !matches!(public_key.first(), Some(2 | 3)) {
        return Err(Error::Key);
    }
    let key = VerifyingKey::from_sec1_bytes(public_key).map_err(|_| Error::Key)?;
    let signature = Signature::from_slice(signature).map_err(|_| Error::Signature)?;
    if signature.normalize_s().is_some() {
        return Err(Error::HighS);
    }
    key.verify_prehash(&Sha256::digest(canonical), &signature)
        .map_err(|_| Error::Signature)
}
