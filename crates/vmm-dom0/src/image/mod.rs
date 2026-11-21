mod layers;
mod plain;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tempfile::{NamedTempFile, TempDir};
use vmm_core::{Digest, Error, Result};

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Defaults {
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

#[derive(Deserialize)]
struct Descriptor {
    digest: String,
    size: u64,
}
#[derive(Deserialize)]
struct Manifest {
    config: Descriptor,
    layers: Vec<Descriptor>,
}
#[derive(Deserialize)]
struct Index {
    manifests: Vec<Descriptor>,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Docker {
    config: PathBuf,
    layers: Vec<PathBuf>,
}
#[derive(Deserialize)]
struct Config {
    config: Option<Defaults>,
    rootfs: RootFs,
}
#[derive(Deserialize)]
struct RootFs {
    diff_ids: Vec<String>,
}

enum BlobReader {
    File(File),
    Slice {
        file: File,
        offset: u64,
        len: u64,
        remaining: u64,
    },
}

impl BlobReader {
    fn len(&self) -> std::io::Result<u64> {
        match self {
            Self::File(file) => Ok(file.metadata()?.len()),
            Self::Slice { len, .. } => Ok(*len),
        }
    }
    fn rewind(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(file) => file.rewind(),
            Self::Slice {
                file,
                offset,
                len,
                remaining,
            } => {
                file.seek(SeekFrom::Start(*offset))?;
                *remaining = *len;
                Ok(())
            }
        }
    }
}

impl Read for BlobReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buf),
            Self::Slice {
                file, remaining, ..
            } => {
                if *remaining == 0 {
                    return Ok(0);
                }
                let max = (*remaining).min(buf.len() as u64) as usize;
                let n = file.read(&mut buf[..max])?;
                *remaining -= n as u64;
                Ok(n)
            }
        }
    }
}

enum Content<'a> {
    Layout(&'a Path),
    Tar(BTreeMap<PathBuf, NamedTempFile>),
    IndexedTar(&'a Path, BTreeMap<PathBuf, (u64, u64)>),
}
impl Content<'_> {
    fn open(&self, path: &Path) -> Result<BlobReader> {
        let key = layers::relative(path)?;
        match self {
            Self::Layout(root) => {
                // OCI layout readers never follow archive-controlled symlinks.
                use rustix::fs::{Mode, OFlags, openat};
                let mut dir = File::open(root)?;
                for name in key.iter() {
                    dir = openat(
                        &dir,
                        name,
                        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )
                    .map_err(std::io::Error::from)?
                    .into();
                }
                if !dir.metadata()?.is_file() {
                    return Err(Error::Invalid("OCI blob file"));
                }
                Ok(BlobReader::File(dir))
            }
            Self::Tar(files) => Ok(BlobReader::File(
                files
                    .get(&key)
                    .ok_or(Error::Missing("archive member"))?
                    .reopen()?,
            )),
            Self::IndexedTar(archive, entries) => {
                let &(offset, len) = entries
                    .get(&key)
                    .ok_or(Error::Missing("archive member"))?;
                let mut file = File::open(archive)?;
                file.seek(SeekFrom::Start(offset))?;
                Ok(BlobReader::Slice {
                    file,
                    offset,
                    len,
                    remaining: len,
                })
            }
        }
    }
    fn blob(&self, d: &Descriptor) -> Result<BlobReader> {
        let hash = d
            .digest
            .strip_prefix("sha256:")
            .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or(Error::Invalid("OCI digest"))?;
        let mut f = self.open(&Path::new("blobs/sha256").join(hash))?;
        let mut sha = Sha256::new();
        if std::io::copy(&mut f, &mut sha)? != d.size || hex::encode(sha.finalize()) != hash {
            return Err(Error::Invalid("OCI blob identity"));
        }
        f.rewind()?;
        Ok(f)
    }
}

pub struct BuiltImage {
    pub file: NamedTempFile,
    pub defaults: Defaults,
}

const MAX_METADATA_BYTES: u64 = 16 << 20;

fn json<T: serde::de::DeserializeOwned>(file: BlobReader) -> Result<T> {
    if file.len()? > MAX_METADATA_BYTES {
        return Err(Error::Invalid("image metadata size"));
    }
    Ok(serde_json::from_reader(file)?)
}

fn open_outer<'a>(source: &'a Path, tmp: &Path) -> Result<Content<'a>> {
    let mut file = File::open(source)?;
    let mut sig = [0u8; 4];
    let n = file.read(&mut sig)?;
    file.rewind()?;
    if (n >= 2 && sig[..2] == [0x1f, 0x8b]) || (n >= 4 && sig == [0x28, 0xb5, 0x2f, 0xfd]) {
        return Ok(Content::Tar(unpack_outer(file, tmp)?));
    }
    let mut entries = BTreeMap::new();
    for member in tar::Archive::new(file).entries_with_seek()? {
        let member = member?;
        if member.header().entry_type().is_dir() {
            continue;
        }
        if !member.header().entry_type().is_file() {
            return Err(Error::Invalid("outer archive member"));
        }
        let key = layers::relative(&member.path()?)?;
        let offset = member.raw_file_position();
        let size = member.size();
        if entries.insert(key, (offset, size)).is_some() {
            return Err(Error::Invalid("duplicate archive member"));
        }
    }
    Ok(Content::IndexedTar(source, entries))
}

fn unpack_outer(file: File, tmp: &Path) -> Result<BTreeMap<PathBuf, NamedTempFile>> {
    let mut files = BTreeMap::new();
    for member in tar::Archive::new(decompressed(file)?).entries()? {
        let mut member = member?;
        if member.header().entry_type().is_dir() {
            continue;
        }
        if !member.header().entry_type().is_file() {
            return Err(Error::Invalid("outer archive member"));
        }
        let key = layers::relative(&member.path()?)?;
        let mut output = NamedTempFile::new_in(tmp)?;
        std::io::copy(&mut member, &mut output)?;
        if files.insert(key, output).is_some() {
            return Err(Error::Invalid("duplicate archive member"));
        }
    }
    Ok(files)
}

fn decompressed<R: Read + 'static>(file: R) -> Result<Box<dyn Read>> {
    let mut reader = BufReader::new(file);
    let sig = reader.fill_buf()?;
    if sig.starts_with(&[0x1f, 0x8b]) {
        Ok(Box::new(flate2::read::MultiGzDecoder::new(reader)))
    } else if sig.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        Ok(Box::new(zstd::stream::read::Decoder::new(reader)?))
    } else {
        Ok(Box::new(reader))
    }
}

fn layers_and_config(
    src: &Content<'_>,
    expected: Digest,
    trusted_builder: bool,
) -> Result<(Config, Vec<BlobReader>)> {
    if trusted_builder {
        let layout: serde_json::Value = json(src.open(Path::new("oci-layout"))?)?;
        if layout["imageLayoutVersion"] != "1.0.0" {
            return Err(Error::Invalid("builder OCI layout"));
        }
    }
    let index = src.open(Path::new("index.json"));
    if let Ok(index) = index {
        let index: Index = json(index)?;
        let digest = format!("sha256:{}", hex::encode(expected));
        if trusted_builder && index.manifests.first().is_none_or(|d| d.digest != digest) {
            return Err(Error::Invalid("builder manifest digest"));
        }
        let desc = index
            .manifests
            .iter()
            .find(|d| d.digest == digest)
            .ok_or(Error::Missing("manifest digest in OCI index"))?;
        let manifest: Manifest = json(src.blob(desc)?)?;
        return Ok((
            json(src.blob(&manifest.config)?)?,
            manifest
                .layers
                .iter()
                .map(|d| src.blob(d))
                .collect::<Result<_>>()?,
        ));
    }
    if trusted_builder || matches!(src, Content::Layout(_)) {
        return Err(Error::Invalid("OCI index"));
    }
    let list: Vec<Docker> = json(src.open(Path::new("manifest.json"))?)?;
    let [image] = list.as_slice() else {
        return Err(Error::Invalid("ambiguous Docker archive"));
    };
    let mut bytes = Vec::new();
    src.open(&image.config)?
        .take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        return Err(Error::Invalid("image metadata size"));
    }
    if Digest::from(Sha256::digest(&bytes)) != expected {
        return Err(Error::Invalid("Docker config digest"));
    }
    Ok((
        serde_json::from_slice(&bytes)?,
        image
            .layers
            .iter()
            .map(|p| src.open(p))
            .collect::<Result<_>>()?,
    ))
}

struct Hashed<R> {
    inner: R,
    hash: Sha256,
}
impl<R: Read> Read for Hashed<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
}

/// Staging is on the image-build filesystem. No layer file or symlink is
/// extracted into dom0's filesystem namespace.
pub fn build(source: &Path, expected: Digest, staging: &Path) -> Result<BuiltImage> {
    build_with(
        source,
        Some(expected),
        staging,
        Some(Path::new("mkfs.erofs")),
        false,
    )
}

/// Build an uncompressed EROFS image directly with the built-in writer (no `mkfs.erofs`).
pub fn build_plain(source: &Path, expected: Digest, staging: &Path) -> Result<BuiltImage> {
    build_with(source, Some(expected), staging, None, false)
}

/// Compute the SHA-256 digest of a seekable file from byte 0.
pub fn file_sha256(file: &mut File) -> Result<Digest> {
    file.rewind()?;
    let mut sha = Sha256::new();
    std::io::copy(file, &mut sha)?;
    Ok(sha.finalize().into())
}

pub(crate) fn build_trusted_builder(
    source: &Path,
    expected: Digest,
    staging: &Path,
) -> Result<BuiltImage> {
    build_with(
        source,
        Some(expected),
        staging,
        Some(Path::new("mkfs.erofs")),
        true,
    )
}

pub(crate) fn build_directory(dir: &Path, staging: &Path) -> Result<BuiltImage> {
    if !dir.is_dir() {
        return Err(Error::Invalid("trusted image directory"));
    }
    build_with(dir, None, staging, Some(Path::new("mkfs.erofs")), false)
}

fn build_with(
    source: &Path,
    expected: Option<Digest>,
    staging: &Path,
    formatter: Option<&Path>,
    trusted_builder: bool,
) -> Result<BuiltImage> {
    let temp = TempDir::new_in(staging)?;
    let mut output = NamedTempFile::new_in(staging)?;
    let mut defaults = Defaults::default();
    let mut layered_tree = None;
    if let Some(expected) = expected {
        let src = if source.is_dir() {
            if !source.join("oci-layout").is_file() {
                return Err(Error::Invalid("OCI layout directory"));
            }
            Content::Layout(source)
        } else {
            open_outer(source, temp.path())?
        };
        let (config, layers) = layers_and_config(&src, expected, trusted_builder)?;
        if layers.len() != config.rootfs.diff_ids.len() {
            return Err(Error::Invalid("layer count"));
        }
        let mut tree = layers::Tree::new(tempfile::tempfile_in(temp.path())?);
        for (layer, diff) in layers.into_iter().zip(config.rootfs.diff_ids) {
            let mut reader = Hashed {
                inner: decompressed(layer)?,
                hash: Sha256::new(),
            };
            tree.overlay(&mut reader)?;
            std::io::copy(&mut reader, &mut std::io::sink())?;
            if format!("sha256:{}", hex::encode(reader.hash.finalize())) != diff {
                return Err(Error::Invalid("layer diff ID"));
            }
        }
        defaults = config.config.unwrap_or_default();
        layered_tree = Some(tree);
    }
    let formatted = if let Some(formatter) = formatter {
        let mut cmd = Command::new(formatter);
        cmd.arg("-T0");
        if let Some(tree) = layered_tree.as_mut() {
            cmd.arg("--tar=f")
                .arg(output.path())
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped());
            match cmd.spawn() {
                Ok(mut child) => {
                    let write_res = {
                        let mut stdin = child
                            .stdin
                            .take()
                            .ok_or_else(|| std::io::Error::other("mkfs.erofs stdin missing"))?;
                        tree.write_tar(&mut stdin)
                    };
                    let result = child.wait_with_output()?;
                    if !result.status.success() {
                        return Err(std::io::Error::other(
                            String::from_utf8_lossy(&result.stderr).into_owned(),
                        )
                        .into());
                    }
                    write_res?;
                    true
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => return Err(e.into()),
            }
        } else {
            cmd.arg(output.path()).arg(source);
            match cmd.output() {
                Ok(result) if !result.status.success() => {
                    return Err(std::io::Error::other(
                        String::from_utf8_lossy(&result.stderr).into_owned(),
                    )
                    .into());
                }
                Ok(_) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => return Err(e.into()),
            }
        }
    } else {
        false
    };
    if !formatted {
        let mut tree = match layered_tree {
            Some(tree) => tree,
            None => layers::Tree::from_directory(source, temp.path())?,
        };
        plain::write(&mut tree, output.as_file_mut())?;
    }
    let len = output.as_file().metadata()?.len();
    if len == 0 {
        return Err(Error::Invalid("empty EROFS image"));
    }
    output.as_file().set_len(
        len.checked_next_multiple_of(crate::extent::BLOCK_BYTES)
            .ok_or(Error::Invalid("image length"))?,
    )?;
    Ok(BuiltImage {
        file: output,
        defaults,
    })
}

#[cfg(test)]
pub(crate) mod tests;
