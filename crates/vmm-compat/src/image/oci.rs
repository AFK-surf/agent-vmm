use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Seek},
    path::Path,
};
use tonic::Status;

pub(super) const META_LIMIT: u64 = 16 << 20;
pub(super) const MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const CONFIG: &str = "application/vnd.oci.image.config.v1+json";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Descriptor {
    pub(super) media_type: String,
    pub(super) digest: String,
    size: u64,
    pub(super) platform: Option<Platform>,
}
#[derive(Deserialize)]
pub(super) struct Platform {
    os: String,
    architecture: String,
    #[serde(default)]
    variant: String,
}
impl Platform {
    pub(super) fn supported(&self) -> bool {
        self.os == "linux" && self.architecture == "arm64" && self.variant.is_empty()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Index {
    schema_version: u32,
    pub(super) manifests: Vec<Descriptor>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Manifest {
    schema_version: u32,
    pub(super) media_type: String,
    pub(super) config: Descriptor,
    pub(super) layers: Vec<Descriptor>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Layout {
    image_layout_version: String,
}

fn invalid() -> Status {
    Status::invalid_argument("invalid OCI archive or content identity")
}

pub(super) fn digest(value: &str) -> Result<[u8; 32], Status> {
    let hex = value.strip_prefix("sha256:").ok_or_else(invalid)?;
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
    {
        return Err(invalid());
    }
    let mut bytes = [0; 32];
    hex::decode_to_slice(hex, &mut bytes).map_err(|_| invalid())?;
    Ok(bytes)
}

pub(super) fn visit(
    file: &mut File,
    mut inspect: impl FnMut(&str, &mut tar::Entry<'_, &mut File>) -> Result<(), Status>,
) -> Result<(), Status> {
    file.rewind().map_err(|_| invalid())?;
    let mut archive = tar::Archive::new(file);
    for member in archive.entries_with_seek().map_err(|_| invalid())? {
        let mut entry = member.map_err(|_| invalid())?;
        let ty = entry.header().entry_type();
        if !ty.is_file() && !ty.is_dir() {
            return Err(invalid());
        }
        let name = String::from_utf8(entry.path_bytes().into_owned()).map_err(|_| invalid())?;
        let path = if ty.is_dir() {
            name.strip_suffix('/').unwrap_or(&name)
        } else {
            &name
        };
        if path
            .split('/')
            .any(|p| p.is_empty() || matches!(p, "." | "..") || p.contains('\0'))
        {
            return Err(invalid());
        }
        if ty.is_file() {
            inspect(&name, &mut entry)?;
        }
    }
    Ok(())
}

pub(super) fn metadata(
    file: &mut File,
    names: &[(&str, u64)],
) -> Result<HashMap<String, Vec<u8>>, Status> {
    let mut found = HashMap::new();
    visit(file, |path, entry| {
        if let Some((_, limit)) = names.iter().find(|(target, _)| *target == path) {
            if found.contains_key(path) || entry.size() > *limit {
                return Err(invalid());
            }
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(|_| invalid())?;
            if bytes.len() as u64 != entry.size() {
                return Err(invalid());
            }
            found.insert(path.to_owned(), bytes);
        }
        Ok(())
    })?;
    if found.len() != names.len() {
        return Err(invalid());
    }
    Ok(found)
}

pub(super) fn validate(path: &Path, identity: [u8; 32]) -> Result<u64, Status> {
    validate_platform(path, identity, false)
}

pub(super) fn validate_built(path: &Path, identity: [u8; 32]) -> Result<u64, Status> {
    validate_platform(path, identity, true)
}

fn validate_platform(path: &Path, identity: [u8; 32], built: bool) -> Result<u64, Status> {
    let mut file = File::open(path).map_err(|_| Status::internal("staged archive unavailable"))?;
    let manifest_path = format!("blobs/sha256/{}", hex::encode(identity));
    let bytes = metadata(
        &mut file,
        &[
            ("oci-layout", 4096),
            ("index.json", META_LIMIT),
            (&manifest_path, META_LIMIT),
        ],
    )?;
    let layout: Layout = serde_json::from_slice(&bytes["oci-layout"]).map_err(|_| invalid())?;
    let index: Index = serde_json::from_slice(&bytes["index.json"]).map_err(|_| invalid())?;
    if layout.image_layout_version != "1.0.0" || index.schema_version != 2 {
        return Err(invalid());
    }
    let manifest_key = format!("sha256:{}", hex::encode(identity));
    let mut choices = index.manifests.iter().filter(|d| d.digest == manifest_key);
    let selected = choices.next().ok_or_else(invalid)?;
    let raw = &bytes[&manifest_path];
    if choices.next().is_some()
        || selected.media_type != MANIFEST
        || !selected
            .platform
            .as_ref()
            .map_or(built, Platform::supported)
        || selected.size != raw.len() as u64
        || <[u8; 32]>::from(Sha256::digest(raw)) != identity
    {
        return Err(invalid());
    }
    let manifest: Manifest = serde_json::from_slice(raw).map_err(|_| invalid())?;
    if manifest.schema_version != 2
        || manifest.media_type != MANIFEST
        || manifest.config.media_type != CONFIG
    {
        return Err(invalid());
    }
    let config_hash = digest(&manifest.config.digest)?;
    let config_path = format!("blobs/sha256/{}", hex::encode(config_hash));
    let config = metadata(&mut file, &[(&config_path, META_LIMIT)])?;
    let raw = &config[&config_path];
    if raw.len() as u64 != manifest.config.size
        || <[u8; 32]>::from(Sha256::digest(raw)) != config_hash
        || !serde_json::from_slice::<Platform>(raw)
            .map_err(|_| invalid())?
            .supported()
    {
        return Err(invalid());
    }

    let mut expected = HashMap::new();
    for layer in manifest.layers {
        if !(layer
            .media_type
            .starts_with("application/vnd.oci.image.layer.v1.tar")
            || layer
                .media_type
                .starts_with("application/vnd.oci.image.layer.nondistributable.v1.tar"))
            || layer.size == 0
        {
            return Err(invalid());
        }
        let hash = digest(&layer.digest)?;
        let name = format!("blobs/sha256/{}", hex::encode(hash));
        if expected
            .insert(name, (hash, layer.size, false))
            .is_some_and(|(_, old, _)| old != layer.size)
        {
            return Err(invalid());
        }
    }
    visit(&mut file, |name, entry| {
        if let Some((hash, size, seen)) = expected.get_mut(name) {
            if *seen || entry.size() != *size {
                return Err(invalid());
            }
            let mut sha = Sha256::new();
            if io::copy(entry, &mut sha).map_err(|_| invalid())? != *size
                || <[u8; 32]>::from(sha.finalize()) != *hash
            {
                return Err(invalid());
            }
            *seen = true;
        }
        Ok(())
    })?;
    if expected.values().any(|(_, _, seen)| !seen) {
        return Err(invalid());
    }
    let metadata_bytes = selected
        .size
        .checked_add(manifest.config.size)
        .ok_or_else(invalid)?;
    expected
        .values()
        .try_fold(metadata_bytes, |sum, (_, size, _)| {
            sum.checked_add(*size).ok_or_else(invalid)
        })
}
