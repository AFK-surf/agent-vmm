use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, Payload},
};
use vmm_core::{Error, Result};
use zeroize::Zeroizing;

pub trait KeySealer: Send + Sync {
    fn seal(&self, identity: &[u8], value: &[u8]) -> Result<Vec<u8>>;
    fn open(&self, identity: &[u8], sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>>;
}

pub struct MasterKey(pub Zeroizing<[u8; 32]>);

pub fn random<const N: usize>() -> Result<Zeroizing<[u8; N]>> {
    let mut bytes = Zeroizing::new([0; N]);
    getrandom::getrandom(bytes.as_mut()).map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(bytes)
}

pub fn disk_key() -> Result<Zeroizing<String>> {
    Ok(Zeroizing::new(hex::encode(random::<64>()?.as_ref())))
}

impl KeySealer for MasterKey {
    fn seal(&self, identity: &[u8], value: &[u8]) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new((&*self.0).into());
        let nonce = random::<24>()?;
        let mut sealed = nonce.to_vec();
        sealed.extend(
            cipher
                .encrypt(
                    XNonce::from_slice(nonce.as_ref()),
                    Payload {
                        msg: value,
                        aad: identity,
                    },
                )
                .map_err(|_| Error::Invalid("key sealing"))?,
        );
        Ok(sealed)
    }
    fn open(&self, identity: &[u8], sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        let (nonce, data) = sealed
            .split_at_checked(24)
            .ok_or(Error::Invalid("sealed key"))?;
        let cipher = XChaCha20Poly1305::new((&*self.0).into());
        cipher
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: data,
                    aad: identity,
                },
            )
            .map(Zeroizing::new)
            .map_err(|_| Error::Invalid("sealed key"))
    }
}
