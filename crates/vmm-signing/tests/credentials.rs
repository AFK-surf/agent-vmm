#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};
use vmm_signing::{
    CredentialStore, Error, FileCredentialStore, Keychain, Result, StorageMode, select_storage,
};

#[test]
fn atomically_replaces_private_credentials_and_rejects_insecure_files() {
    let directory = tempfile::tempdir().unwrap();
    let credentials = directory.path().join("credentials");
    let store = FileCredentialStore::new(&credentials);
    store.replace("../account", b"first").unwrap();
    store.replace("../account", b"second").unwrap();
    assert_eq!(&**store.load("../account").unwrap(), b"second");
    let files: Vec<_> = fs::read_dir(credentials)
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect();
    assert_eq!(files.len(), 1);
    let path = files[0].path();
    assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o600);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(store.load("../account"), Err(Error::Unavailable)));
    store.delete("../account").unwrap();
    store.delete("../account").unwrap();
    assert!(!path.exists());
}

struct KeychainProbe {
    available: bool,
}

impl Keychain for KeychainProbe {
    fn probe(&self) -> Result<()> {
        self.available.then_some(()).ok_or(Error::Unavailable)
    }
    fn load(&self, _: &str) -> Result<zeroize::Zeroizing<Vec<u8>>> {
        Err(Error::Unavailable)
    }
    fn replace(&self, _: &str, _: &[u8]) -> Result<()> {
        Err(Error::Unavailable)
    }
    fn delete(&self, _: &str) -> Result<()> {
        Err(Error::Unavailable)
    }
}

#[test]
fn only_a_fresh_installation_can_choose_file_storage() {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let marker = root.path().join("secret-store");
    let files = root.path().join("credentials");
    let locked = |_: &str| Some(Arc::new(KeychainProbe { available: false }) as Arc<dyn Keychain>);
    fs::write(&marker, br#"{"backend":"keychain","namespace":"fixed"}"#).unwrap();
    fs::set_permissions(&marker, fs::Permissions::from_mode(0o600)).unwrap();
    let resolve = |name: &str| {
        assert_eq!(name, "fixed");
        locked(name)
    };
    assert!(matches!(
        select_storage(&marker, &files, None, "new", resolve),
        Err(Error::Unavailable)
    ));
    assert!(!files.exists());
    fs::remove_file(&marker).unwrap();
    let (selected, credentials) = select_storage(&marker, &files, None, "new", |_| {
        Some(Arc::new(KeychainProbe { available: false }))
    })
    .unwrap();
    assert_eq!(selected.backend, StorageMode::File);
    credentials.replace("host", b"secret").unwrap();
    assert_eq!(&**credentials.load("host").unwrap(), b"secret");
}

#[test]
fn fresh_required_keychain_persists_only_a_working_backend() {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let marker = root.path().join("secret-store");
    let files = root.path().join("credentials");

    let select = |available| {
        select_storage(
            &marker,
            &files,
            Some(StorageMode::Keychain),
            "fresh",
            |namespace| {
                assert_eq!(namespace, "fresh");
                Some(Arc::new(KeychainProbe { available }))
            },
        )
    };
    assert!(matches!(select(false), Err(Error::Unavailable)));
    assert!(!marker.exists());
    assert!(!files.exists());

    let (selection, _) = select(true).unwrap();
    assert_eq!(selection.backend, StorageMode::Keychain);
    assert_eq!(selection.namespace, "fresh");
    let persisted: serde_json::Value = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
    assert_eq!(
        persisted,
        serde_json::json!({"backend":"keychain","namespace":"fresh"})
    );
    assert!(!files.exists());
}

#[test]
fn file_identity_survives_restart_without_rekeying() {
    use vmm_signing::{FileIdentityStore, IdentitySigner};

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("trust").join("device-root");
    let first = FileIdentityStore::new(&path)
        .ensure(1)
        .unwrap()
        .public_key()
        .unwrap();
    let reopened = FileIdentityStore::new(&path);
    assert_eq!(reopened.load(1).unwrap().public_key().unwrap(), first);
    assert_eq!(reopened.ensure(1).unwrap().public_key().unwrap(), first);
    assert_ne!(reopened.ensure(2).unwrap().public_key().unwrap(), first);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::write(&path, b"invalid DER").unwrap();
    assert!(matches!(reopened.ensure(1), Err(Error::Key)));
}
