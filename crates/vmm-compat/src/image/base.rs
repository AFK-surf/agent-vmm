//! Retain the exact OCI blobs needed by offline builds. Other index entries and archive
//! members belong to the uploading scope and must not follow a digest into another scope.
use super::oci::{self, Descriptor, Index, Manifest};
use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;
use tonic::Status;
use vmm_core::{Digest, Store};

fn io_error(_: std::io::Error) -> Status {
    Status::unavailable("build base archive storage unavailable")
}
fn append(
    tar: &mut tar::Builder<File>,
    name: &str,
    size: u64,
    data: impl Read,
) -> Result<(), Status> {
    let mut header = tar::Header::new_ustar();
    header.set_size(size);
    header.set_mode(0o644);
    tar.append_data(&mut header, name, data).map_err(io_error)
}
fn path(dir: &Path, digest: Digest) -> PathBuf {
    dir.join("oci").join(hex::encode(digest))
}
pub(super) fn retain(dir: &Path, archive: &Path, digest: Digest) -> Result<(), Status> {
    let dest = path(dir, digest);
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir.join("oci"))
        .map_err(io_error)?;
    if dest.try_exists().map_err(io_error)? {
        return Ok(());
    }
    let mut source = File::open(archive).map_err(io_error)?;
    let name = format!("blobs/sha256/{}", hex::encode(digest));
    let bytes = oci::metadata(&mut source, &[(&name, oci::META_LIMIT)])?;
    let manifest: Manifest = serde_json::from_slice(&bytes[&name])
        .map_err(|_| Status::data_loss("invalid verified manifest"))?;
    let mut required: BTreeSet<_> = std::iter::once(&manifest.config)
        .chain(&manifest.layers)
        .map(|d: &Descriptor| {
            oci::digest(&d.digest).map(|hash| format!("blobs/sha256/{}", hex::encode(hash)))
        })
        .collect::<Result<_, _>>()?;
    required.insert(name.clone());
    let tmp = NamedTempFile::new_in(dir).map_err(io_error)?;
    let mut tar = tar::Builder::new(tmp.reopen().map_err(io_error)?);
    let layout = br#"{"imageLayoutVersion":"1.0.0"}"#;
    append(&mut tar, "oci-layout", layout.len() as u64, &layout[..])?;
    let index = serde_json::to_vec(&serde_json::json!({"schemaVersion":2,"manifests":[{
        "mediaType":"application/vnd.oci.image.manifest.v1+json","digest":format!("sha256:{}",hex::encode(digest)),
        "size":bytes[&name].len(),"platform":{"os":"linux","architecture":"arm64"}
    }]})).map_err(|_|Status::internal("encode build base index"))?;
    append(&mut tar, "index.json", index.len() as u64, &index[..])?;
    oci::visit(&mut source, |name, entry| {
        if required.remove(name) {
            append(&mut tar, name, entry.size(), entry)?;
        }
        Ok(())
    })?;
    if !required.is_empty() {
        return Err(Status::data_loss("missing verified base blob"));
    }
    tar.into_inner()
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)?;
    match tmp.persist_noclobber(&dest) {
        Ok(_) => File::open(dest.parent().unwrap())
            .and_then(|f| f.sync_all())
            .map_err(io_error),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(io_error(e.error)),
    }
}
pub(super) fn open(dir: &Path, digest: Digest) -> Result<File, Status> {
    File::open(path(dir, digest))
        .map_err(|_| Status::failed_precondition("the admitted image has no retained OCI layout"))
}
pub(crate) fn collect(dir: &Path, store: &Store) -> Result<(), Status> {
    let entries = match dir.join("oci").read_dir() {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_error(e)),
    };
    for entry in entries {
        let entry = entry.map_err(io_error)?;
        let Some(digest) = entry
            .file_name()
            .to_str()
            .and_then(|name| oci::digest(&format!("sha256:{name}")).ok())
        else {
            continue;
        };
        match store.image_artifact(digest) {
            Ok(_) => {}
            Err(vmm_core::Error::Missing(_)) => {
                std::fs::remove_file(entry.path()).map_err(io_error)?
            }
            Err(e) => return Err(crate::error::core_status(e)),
        }
    }
    Ok(())
}

pub(super) fn output_digest(path: &Path) -> Result<Digest, Status> {
    let mut source = File::open(path).map_err(io_error)?;
    let bytes = oci::metadata(&mut source, &[("index.json", oci::META_LIMIT)])?;
    let index: Index = serde_json::from_slice(&bytes["index.json"])
        .map_err(|_| Status::data_loss("invalid build output index"))?;
    let mut manifests = index.manifests.iter().filter(|d| {
        d.media_type == oci::MANIFEST && d.platform.as_ref().is_none_or(|p| p.supported())
    });
    let selected = manifests
        .next()
        .ok_or_else(|| Status::data_loss("missing build output manifest"))?;
    if manifests.next().is_some() {
        return Err(Status::data_loss("ambiguous build output manifest"));
    }
    oci::digest(&selected.digest)
}
