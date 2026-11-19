use crate::SigningKeys;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;
use zeroize::Zeroizing;

pub struct FileSigningKeys {
    directory: PathBuf,
    path: PathBuf,
}

impl FileSigningKeys {
    /// Use only for a file-backed installation. The legacy MCP key uses this same path.
    pub fn open(state_root: &Path) -> io::Result<Self> {
        let directory = state_root.join("appliance-secrets");
        match DirBuilder::new().mode(0o700).create(&directory) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            result => result?,
        }
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() || !private_owner(&metadata, 0o700) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid appliance secret directory",
            ));
        }
        let digest = Sha256::digest(b"agent-vmm-mcp");
        let name = format!("{}.mcp-token-key", hex::encode(&digest[..16]));
        let keys = Self {
            path: directory.join(name),
            directory,
        };
        match keys.current() {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut initial = Zeroizing::new([0u8; 32]);
                getrandom::fill(initial.as_mut_slice()).map_err(io::Error::other)?;
                let file = keys.staged(&initial)?;
                match file.persist_noclobber(&keys.path) {
                    Ok(_) => File::open(&keys.directory)?.sync_all()?,
                    Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
                        keys.current()?;
                    }
                    Err(e) => return Err(e.error),
                }
            }
            result => {
                result?;
            }
        }
        Ok(keys)
    }

    fn staged(&self, key: &[u8; 32]) -> io::Result<NamedTempFile> {
        let mut temp = NamedTempFile::new_in(&self.directory)?;
        temp.write_all(key)?;
        temp.as_file().sync_all()?;
        Ok(temp)
    }
}

fn private_owner(metadata: &fs::Metadata, mode: u32) -> bool {
    metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o7777 == mode
}

impl SigningKeys for FileSigningKeys {
    fn current(&self) -> io::Result<Zeroizing<[u8; 32]>> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || !private_owner(&metadata, 0o600)
            || metadata.nlink() != 1
            || metadata.len() != 32
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid MCP signing key file",
            ));
        }
        let mut key = Zeroizing::new([0u8; 32]);
        file.read_exact(key.as_mut_slice())?;
        Ok(key)
    }

    fn replace(&self, key: &[u8; 32]) -> io::Result<()> {
        self.staged(key)?.persist(&self.path)?;
        File::open(&self.directory)?.sync_all()
    }
}
