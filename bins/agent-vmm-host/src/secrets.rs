//! Open the installation's recorded credential and MCP signing-key stores.
use std::{io, path::Path, sync::Arc};
use vmm_mcp::{FileSigningKeys, SigningKeys, TokenAuthority};
use vmm_signing::{CredentialStore, Keychain, StorageMode, select_storage};
use zeroize::Zeroizing;

pub struct Secrets {
    pub credentials: Arc<dyn CredentialStore>,
    pub tokens: TokenAuthority,
}
struct KeychainSigningKeys(Arc<dyn Keychain>);
const MCP_ITEM: &str = "mcp-token-key/agent-vmm-mcp";
impl SigningKeys for KeychainSigningKeys {
    fn current(&self) -> io::Result<Zeroizing<[u8; 32]>> {
        let raw = self.0.load(MCP_ITEM).map_err(io_error)?;
        let mut key = Zeroizing::new([0; 32]);
        if raw.len() != key.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid MCP signing key",
            ));
        }
        key.copy_from_slice(raw.as_slice());
        Ok(key)
    }
    fn replace(&self, key: &[u8; 32]) -> io::Result<()> {
        self.0.replace(MCP_ITEM, key).map_err(io_error)
    }
}
fn io_error(e: vmm_signing::Error) -> io::Error {
    match e {
        vmm_signing::Error::Io(e) => e,
        other => io::Error::other(other),
    }
}
impl Secrets {
    pub fn open(instance: &Path) -> io::Result<Self> {
        let namespace = if gui_host() {
            uuid::Uuid::new_v4().simple().to_string()
        } else {
            String::new()
        };
        let (selection, credentials) = select_storage(
            &instance.join("secret-store"),
            instance.join("support/remote-credentials"),
            None,
            &namespace,
            platform_keychain,
        )
        .map_err(io_error)?;
        let keys: Arc<dyn SigningKeys> = match selection.backend {
            StorageMode::File => Arc::new(FileSigningKeys::open(instance)?),
            StorageMode::Keychain => {
                let keys = KeychainSigningKeys(
                    platform_keychain(&selection.namespace)
                        .ok_or_else(|| io::Error::other("Keychain unavailable"))?,
                );
                match keys.current() {
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        let mut new = Zeroizing::new([0; 32]);
                        getrandom::fill(new.as_mut_slice()).map_err(io::Error::other)?;
                        keys.replace(&new)?;
                    }
                    value => {
                        value?;
                    }
                }
                Arc::new(keys)
            }
        };
        Ok(Self {
            credentials: Arc::from(credentials),
            tokens: TokenAuthority::new(keys),
        })
    }
}

#[cfg(target_os = "macos")]
fn gui_host() -> bool {
    let job = format!(
        "gui/{}/com.afk-surf.agent-vmm-host",
        rustix::process::getuid().as_raw()
    );
    let Ok(output) = std::process::Command::new("/bin/launchctl")
        .args(["print", &job])
        .output()
    else {
        return false;
    };
    let expected = std::process::id().to_string();
    output.status.success()
        && String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.split_whitespace().eq(["pid", "=", &expected]))
}
#[cfg(target_os = "macos")]
fn platform_keychain(namespace: &str) -> Option<Arc<dyn Keychain>> {
    gui_host()
        .then(|| Arc::new(crate::keychain::PlatformKeys(namespace.into())) as Arc<dyn Keychain>)
}
#[cfg(not(target_os = "macos"))]
fn gui_host() -> bool {
    false
}
#[cfg(not(target_os = "macos"))]
fn platform_keychain(_: &str) -> Option<Arc<dyn Keychain>> {
    None
}
