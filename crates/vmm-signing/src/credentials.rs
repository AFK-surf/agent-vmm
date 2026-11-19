use crate::{Error, Result};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

pub const MAX_CREDENTIAL_BYTES: usize = 4096;

pub const MAX_BOOTSTRAP_BYTES: usize = 1 << 20;

pub trait CredentialStore: Send + Sync {
    fn load(&self, account: &str) -> Result<Zeroizing<Vec<u8>>>;
    fn replace(&self, account: &str, value: &[u8]) -> Result<()>;
    fn delete(&self, account: &str) -> Result<()>;
    fn load_bootstrap(&self, bootstrap: &str) -> Result<Zeroizing<Vec<u8>>>;
    fn store_bootstrap(&self, bootstrap: &str, value: &[u8]) -> Result<()>;
    fn delete_bootstrap(&self, bootstrap: &str) -> Result<()> {
        self.delete(&format!("appliance-bootstrap/{bootstrap}"))
    }
}

fn name(account: &str) -> String {
    let digest = Sha256::digest(account.as_bytes());
    format!("{}.credential", hex::encode(&digest[..16]))
}

fn check_size(value: &[u8]) -> Result<()> {
    check_bound(value, MAX_CREDENTIAL_BYTES)
}
fn check_bound(value: &[u8], max: usize) -> Result<()> {
    if value.is_empty() || value.len() > max {
        return Err(Error::CredentialSize);
    }
    Ok(())
}

/// The signed macOS host supplies a Keychain adapter bound to its persisted namespace.
/// Backends must return an error if their access group or the user session is unavailable.
pub trait Keychain: Send + Sync {
    fn probe(&self) -> Result<()>;
    fn load(&self, item: &str) -> Result<Zeroizing<Vec<u8>>>;
    fn replace(&self, item: &str, value: &[u8]) -> Result<()>;
    fn delete(&self, item: &str) -> Result<()>;
}

pub struct KeychainCredentialStore<K>(pub K);

impl<K: Keychain> CredentialStore for KeychainCredentialStore<K> {
    fn load(&self, account: &str) -> Result<Zeroizing<Vec<u8>>> {
        let value = self.0.load(&name(account))?;
        check_size(&value)?;
        Ok(value)
    }
    fn replace(&self, account: &str, value: &[u8]) -> Result<()> {
        check_size(value)?;
        self.0.replace(&name(account), value)
    }
    fn load_bootstrap(&self, bootstrap: &str) -> Result<Zeroizing<Vec<u8>>> {
        let raw = self
            .0
            .load(&name(&format!("appliance-bootstrap/{bootstrap}")))?;
        check_bound(&raw, MAX_BOOTSTRAP_BYTES)?;
        Ok(raw)
    }
    fn store_bootstrap(&self, bootstrap: &str, value: &[u8]) -> Result<()> {
        check_bound(value, MAX_BOOTSTRAP_BYTES)?;
        self.0
            .replace(&name(&format!("appliance-bootstrap/{bootstrap}")), value)
    }
    fn delete(&self, account: &str) -> Result<()> {
        self.0.delete(&name(account))
    }
}

#[cfg(unix)]
mod files {
    use super::*;
    use rustix::fs::{Mode, OFlags, open};
    use serde::{Deserialize, Serialize};
    use std::{
        fs::{self, DirBuilder, File},
        io::{ErrorKind, Read, Write},
        os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        sync::Arc,
    };

    pub struct FileCredentialStore {
        directory: PathBuf,
    }

    impl FileCredentialStore {
        pub fn new(directory: impl Into<PathBuf>) -> Self {
            Self {
                directory: directory.into(),
            }
        }
    }

    impl CredentialStore for FileCredentialStore {
        fn load(&self, account: &str) -> Result<Zeroizing<Vec<u8>>> {
            let value = private_read(&self.directory.join(name(account)))?;
            check_size(&value)?;
            Ok(value)
        }
        fn replace(&self, account: &str, value: &[u8]) -> Result<()> {
            check_size(value)?;
            write_private(&self.directory.join(name(account)), value, false)
        }
        fn load_bootstrap(&self, bootstrap: &str) -> Result<Zeroizing<Vec<u8>>> {
            private_read_bounded(
                &self
                    .directory
                    .join(name(&format!("appliance-bootstrap/{bootstrap}"))),
                MAX_BOOTSTRAP_BYTES,
            )
        }
        fn store_bootstrap(&self, bootstrap: &str, value: &[u8]) -> Result<()> {
            check_bound(value, MAX_BOOTSTRAP_BYTES)?;
            write_private(
                &self
                    .directory
                    .join(name(&format!("appliance-bootstrap/{bootstrap}"))),
                value,
                false,
            )
        }
        fn delete(&self, account: &str) -> Result<()> {
            match fs::remove_file(self.directory.join(name(account))) {
                Ok(()) => File::open(&self.directory)?.sync_all().map_err(Into::into),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        }
    }

    /// Uses the existing Go Host filenames and SEC1 encoding for device root keys.
    pub struct FileIdentityStore {
        root: PathBuf,
    }

    impl FileIdentityStore {
        pub fn new(root: impl Into<PathBuf>) -> Self {
            Self { root: root.into() }
        }

        fn path(&self, revision: u64) -> Result<PathBuf> {
            match revision {
                0 => Err(Error::Key),
                1 => Ok(self.root.clone()),
                _ => {
                    let mut path = self.root.as_os_str().to_owned();
                    path.push(format!(".{revision}"));
                    Ok(path.into())
                }
            }
        }

        pub fn load(&self, revision: u64) -> Result<crate::FileIdentityKey> {
            crate::FileIdentityKey::from_sec1_der(&private_read(&self.path(revision)?)?)
        }

        /// Creation races resolve to the same published key. An unreadable key is a hard error.
        pub fn ensure(&self, revision: u64) -> Result<crate::FileIdentityKey> {
            match self.load(revision) {
                Ok(key) => return Ok(key),
                Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            let key = crate::FileIdentityKey::generate();
            match write_private(&self.path(revision)?, &key.to_sec1_der()?, true) {
                Ok(()) => Ok(key),
                Err(Error::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
                    self.load(revision)
                }
                Err(error) => Err(error),
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct StorageSelection {
        pub backend: StorageMode,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub namespace: String,
    }

    #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "lowercase")]
    pub enum StorageMode {
        File,
        Keychain,
    }

    /// Records the initial decision with an atomic create. Existing installations follow the
    /// recorded mode even if a fresh probe would have chosen a different backend.
    pub fn select_storage(
        marker: &Path,
        directory: impl Into<PathBuf>,
        required: Option<StorageMode>,
        fresh_keychain_namespace: &str,
        keychain_for: impl Fn(&str) -> Option<Arc<dyn Keychain>>,
    ) -> Result<(StorageSelection, Box<dyn CredentialStore>)> {
        let selected = match private_read(marker) {
            Ok(data) => read_selection(&data)?,
            Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound => {
                let selection = if required != Some(StorageMode::File)
                    && !fresh_keychain_namespace.is_empty()
                    && keychain_for(fresh_keychain_namespace).is_some_and(|k| k.probe().is_ok())
                {
                    StorageSelection {
                        backend: StorageMode::Keychain,
                        namespace: fresh_keychain_namespace.into(),
                    }
                } else if required == Some(StorageMode::Keychain) {
                    return Err(Error::Unavailable);
                } else {
                    StorageSelection {
                        backend: StorageMode::File,
                        namespace: String::new(),
                    }
                };
                let json = serde_json::to_vec(&selection).map_err(|_| Error::StorageMode)?;
                match write_private(marker, &json, true) {
                    Ok(()) => selection,
                    Err(Error::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
                        read_selection(&private_read(marker)?)?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        if required.is_some_and(|mode| mode != selected.backend) {
            return Err(Error::StorageMode);
        }
        let store: Box<dyn CredentialStore> = match selected.backend {
            StorageMode::File => Box::new(FileCredentialStore::new(directory)),
            StorageMode::Keychain => {
                let backend = keychain_for(&selected.namespace).ok_or(Error::Unavailable)?;
                backend.probe()?;
                Box::new(KeychainCredentialStore(SharedKeychain(backend)))
            }
        };
        Ok((selected, store))
    }

    struct SharedKeychain(Arc<dyn Keychain>);

    impl Keychain for SharedKeychain {
        fn probe(&self) -> Result<()> {
            self.0.probe()
        }
        fn load(&self, item: &str) -> Result<Zeroizing<Vec<u8>>> {
            self.0.load(item)
        }
        fn replace(&self, item: &str, value: &[u8]) -> Result<()> {
            self.0.replace(item, value)
        }
        fn delete(&self, item: &str) -> Result<()> {
            self.0.delete(item)
        }
    }

    fn read_selection(bytes: &[u8]) -> Result<StorageSelection> {
        let selected: StorageSelection =
            serde_json::from_slice(bytes).map_err(|_| Error::StorageMode)?;
        if selected.backend == StorageMode::Keychain && selected.namespace.is_empty() {
            return Err(Error::StorageMode);
        }
        Ok(selected)
    }

    fn private_read(path: &Path) -> Result<Zeroizing<Vec<u8>>> {
        private_read_bounded(path, MAX_CREDENTIAL_BYTES)
    }
    fn private_read_bounded(path: &Path, max: usize) -> Result<Zeroizing<Vec<u8>>> {
        let fd = open(
            path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        let file = File::from(fd);
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.mode() & 0o077 != 0
            || meta.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(Error::Unavailable);
        }
        let mut data = Zeroizing::new(Vec::new());
        file.take(max as u64 + 1).read_to_end(&mut data)?;
        check_bound(&data, max)?;
        Ok(data)
    }

    fn write_private(path: &Path, value: &[u8], create: bool) -> Result<()> {
        let directory = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or(Error::StorageMode)?;
        DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(directory)?;
        let info = fs::symlink_metadata(directory)?;
        if !info.is_dir()
            || info.mode() & 0o077 != 0
            || info.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(Error::Unavailable);
        }
        let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
        temporary.write_all(value)?;
        temporary.as_file().sync_all()?;
        if create {
            temporary
                .persist_noclobber(path)
                .map_err(|error| error.error)?;
        } else {
            temporary.persist(path).map_err(|error| error.error)?;
        }
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

#[cfg(unix)]
pub use files::*;
