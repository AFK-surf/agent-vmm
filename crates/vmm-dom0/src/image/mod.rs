mod layers;
mod plain;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read, Seek},
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

enum Content<'a> {
    Layout(&'a Path),
    Tar(BTreeMap<PathBuf, NamedTempFile>),
}
impl Content<'_> {
    fn open(&self, path: &Path) -> Result<File> {
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
                Ok(dir)
            }
            Self::Tar(files) => Ok(files
                .get(&key)
                .ok_or(Error::Missing("archive member"))?
                .reopen()?),
        }
    }
    fn blob(&self, d: &Descriptor) -> Result<File> {
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

fn json<T: serde::de::DeserializeOwned>(file: File) -> Result<T> {
    if file.metadata()?.len() > MAX_METADATA_BYTES {
        return Err(Error::Invalid("image metadata size"));
    }
    Ok(serde_json::from_reader(file)?)
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

fn decompressed(file: File) -> Result<Box<dyn Read>> {
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
) -> Result<(Config, Vec<File>)> {
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
        Path::new("mkfs.erofs"),
        false,
    )
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
        Path::new("mkfs.erofs"),
        true,
    )
}

pub(crate) fn build_directory(dir: &Path, staging: &Path) -> Result<BuiltImage> {
    if !dir.is_dir() {
        return Err(Error::Invalid("trusted image directory"));
    }
    build_with(dir, None, staging, Path::new("mkfs.erofs"), false)
}

fn build_with(
    source: &Path,
    expected: Option<Digest>,
    staging: &Path,
    formatter: &Path,
    trusted_builder: bool,
) -> Result<BuiltImage> {
    let temp = TempDir::new_in(staging)?;
    let mut output = NamedTempFile::new_in(staging)?;
    let mut defaults = Defaults::default();
    let mut cmd = Command::new(formatter);
    cmd.arg("-T0");
    let mut layered_tree = None;
    if let Some(expected) = expected {
        let src = if source.is_dir() {
            if !source.join("oci-layout").is_file() {
                return Err(Error::Invalid("OCI layout directory"));
            }
            Content::Layout(source)
        } else {
            Content::Tar(unpack_outer(File::open(source)?, temp.path())?)
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
        let mut tar = tempfile::tempfile_in(temp.path())?;
        tree.write_tar(&mut tar)?;
        tar.rewind()?;
        layered_tree = Some(tree);
        cmd.arg("--tar=f")
            .arg(output.path())
            .stdin(Stdio::from(tar));
    } else {
        cmd.arg(output.path()).arg(source);
    }
    match cmd.output() {
        Ok(result) if !result.status.success() => {
            return Err(std::io::Error::other(
                String::from_utf8_lossy(&result.stderr).into_owned(),
            )
            .into());
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut tree = match layered_tree {
                Some(tree) => tree,
                None => layers::Tree::from_directory(source, temp.path())?,
            };
            plain::write(&mut tree, output.as_file_mut())?;
        }
        Err(e) => return Err(e.into()),
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
