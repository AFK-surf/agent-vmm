#![forbid(unsafe_code)]
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io,
    path::{Component, Path, PathBuf},
};

#[derive(Deserialize)]
struct Manifest {
    schema_version: u32,
    architecture: String,
    artifacts: BTreeMap<String, Artifact>,
}

#[derive(Deserialize)]
struct Artifact {
    path: PathBuf,
    size: u64,
    sha256: String,
}

#[derive(Debug, Clone)]
pub struct Bundle {
    pub kernel: PathBuf,
    pub initramfs: Option<PathBuf>,
    pub rootfs: Option<PathBuf>,
}

impl Bundle {
    pub fn verify(directory: &Path) -> io::Result<Self> {
        let directory = directory.canonicalize()?;
        let bytes = std::fs::read(directory.join("manifest.json"))?;
        if let Some(expected) =
            option_env!("AGENT_VMM_GUEST_MANIFEST_DIGEST").filter(|s| !s.is_empty())
            && !hex::encode(Sha256::digest(&bytes)).eq_ignore_ascii_case(expected)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Guest manifest differs from embedded digest",
            ));
        }
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        if manifest.schema_version != 1 || manifest.architecture != "arm64" {
            return Err(io::Error::other("unsupported carrier Guest manifest"));
        }
        let verify = |name: &str| -> io::Result<PathBuf> {
            let entry = manifest
                .artifacts
                .get(name)
                .ok_or_else(|| io::Error::other(format!("missing {name}")))?;
            if entry
                .path
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err(io::Error::other("artifact path outside Guest directory"));
            }
            let path = directory.join(&entry.path).canonicalize()?;
            if !path.starts_with(&directory) {
                return Err(io::Error::other("artifact outside Guest directory"));
            }
            let mut f = File::open(&path)?;
            if !f.metadata()?.is_file() {
                return Err(io::Error::other("invalid artifact type"));
            }
            let mut hash = Sha256::new();
            if io::copy(&mut f, &mut hash)? != entry.size
                || hex::encode(hash.finalize()) != entry.sha256
            {
                return Err(io::Error::other(format!(
                    "{name} content differs from manifest"
                )));
            }
            Ok(path)
        };
        Ok(Self {
            kernel: verify("kernel")?,
            initramfs: manifest
                .artifacts
                .contains_key("initramfs")
                .then(|| verify("initramfs"))
                .transpose()?,
            rootfs: manifest
                .artifacts
                .contains_key("rootfs")
                .then(|| verify("rootfs"))
                .transpose()?,
        })
    }
}
