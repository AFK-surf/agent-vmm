//! Linux releases. A release directory carries `release.json`, the inventory
//! of its files. The SHA-256 of that file is the content identity and the name
//! of the installed directory; `current` links to the active one.

use crate::error::{Result, err};
use crate::statefs::{hex, random_hex};
use crate::{Paths, valid_id};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::os::unix::fs::{OpenOptionsExt, symlink};
use std::path::{Component, Path, PathBuf};

pub const MANIFEST: &str = "release.json";
const REQUIRED: [&str; 1] = ["agent-vmm"];

#[derive(Clone, Debug, Deserialize)]
struct ReleaseFile {
    path: String,
    size: i64,
    sha256: String,
    #[serde(default)]
    executable: bool,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    version: u32,
    release_id: String,
    files: Vec<ReleaseFile>,
}

fn content_id(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn read_manifest(directory: &Path) -> Result<(Manifest, String)> {
    let data = fs::read(directory.join(MANIFEST)).map_err(|error| err(format!("read release inventory: {error}")))?;
    let manifest: Manifest = serde_json::from_slice(&data).ok().filter(|_| data.len() <= 1 << 20).ok_or_else(|| err("release inventory is invalid"))?;
    if manifest.version != 1 || !valid_id(&manifest.release_id) || manifest.files.is_empty() {
        return Err(err("release inventory is invalid"));
    }
    let mut seen = BTreeMap::new();
    for file in &manifest.files {
        let relative = Path::new(&file.path);
        let plain = relative.components().all(|component| matches!(component, Component::Normal(_))) && relative.components().collect::<PathBuf>().as_os_str() == file.path.as_str();
        if !plain || file.path == MANIFEST || file.size < 0 || file.sha256.len() != 64 || seen.insert(file.path.as_str(), ()).is_some() {
            return Err(err("release inventory entry is invalid"));
        }
    }
    if let Some(missing) = REQUIRED.iter().find(|required| !seen.contains_key(**required)) {
        return Err(err(format!("release inventory does not list {missing}")));
    }
    Ok((manifest, hex(&Sha256::digest(&data))))
}

/// Returns the release identity of a release directory. It does not hash the release files.
pub fn release_id(directory: &Path) -> Result<String> {
    Ok(read_manifest(directory)?.0.release_id)
}

fn sync_directory(path: &Path) -> Result<()> {
    Ok(File::open(path)?.sync_all()?)
}

fn copy_file(source: &Path, target: &Path, executable: bool) -> Result<()> {
    let mut input = File::open(source)?;
    let mut output = fs::OpenOptions::new().write(true).create_new(true).mode(if executable { 0o755 } else { 0o644 }).open(target)?;
    std::io::copy(&mut input, &mut output)?;
    Ok(output.sync_all()?)
}

/// Copies every listed file and refuses a source tree with another file set.
/// Each copied directory is synced, so a published release survives a power loss.
fn copy_tree(source: &Path, target: &Path, relative: &Path, files: &mut BTreeMap<String, ReleaseFile>) -> Result<()> {
    let directory = target.join(relative);
    fs::DirBuilder::new().recursive(relative.as_os_str().is_empty()).create(&directory)?;
    for entry in fs::read_dir(source.join(relative))? {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let name = path.display().to_string();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_tree(source, target, &path, files)?;
        } else if !kind.is_file() {
            return Err(err(format!("release entry {name} is not a regular file")));
        } else if name == MANIFEST {
            copy_file(&entry.path(), &target.join(&path), false)?;
        } else {
            let file = files.remove(&name).ok_or_else(|| err(format!("release entry {name} is not listed in the release inventory")))?;
            copy_file(&entry.path(), &target.join(&path), file.executable)?;
        }
    }
    sync_directory(&directory)
}

/// Publishes `source` as `<install root>/<content id>`. A directory with that
/// name is complete, because only a rename publishes it.
pub fn install(paths: &Paths, source: &Path) -> Result<String> {
    let (manifest, id) = read_manifest(source)?;
    let target = paths.install_root.join(&id);
    if target.is_dir() {
        return Ok(id);
    }
    fs::create_dir_all(&paths.install_root)?;
    let staging = paths.install_root.join(format!(".install-{id}-{}", random_hex(8)));
    let mut files: BTreeMap<String, ReleaseFile> = manifest.files.iter().map(|file| (file.path.clone(), file.clone())).collect();
    let copied = copy_tree(source, &staging, Path::new(""), &mut files).and_then(|()| match files.keys().next() {
        Some(missing) => Err(err(format!("release file {missing} is missing"))),
        None => Ok(()),
    });
    let published = copied.map_err(|error| error.context(format!("install release {}", manifest.release_id))).and_then(|()| {
        fs::rename(&staging, &target).map_err(|error| err(format!("publish release {}: {error}", manifest.release_id)))
    });
    if published.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    published?;
    sync_directory(&paths.install_root)?;
    Ok(id)
}

/// The content identity that `current` links to.
pub fn current(paths: &Paths) -> Option<String> {
    let target = fs::read_link(&paths.app).ok()?;
    target.to_str().filter(|name| content_id(name)).map(str::to_string)
}

/// Replaces the `current` link in one rename.
pub fn switch(paths: &Paths, id: &str) -> Result<()> {
    if current(paths).as_deref() == Some(id) {
        return Ok(());
    }
    let link = paths.install_root.join(format!(".current-{}", random_hex(8)));
    symlink(id, &link)?;
    if let Err(error) = fs::rename(&link, &paths.app) {
        let _ = fs::remove_file(&link);
        return Err(err(format!("switch current release: {error}")));
    }
    sync_directory(&paths.install_root)
}

/// Removes the installed releases other than the current one.
pub fn prune(paths: &Paths) -> Result<()> {
    let current = current(paths);
    for entry in fs::read_dir(&paths.install_root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_dir() && content_id(&name) && current.as_deref() != Some(&name) {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(root: &Path, name: &str, id: &str, extra: Option<&str>) -> PathBuf {
        let directory = root.join(name);
        fs::create_dir_all(directory.join("guest")).unwrap();
        fs::write(directory.join("agent-vmm"), b"binary").unwrap();
        fs::write(directory.join("guest/manifest.json"), b"{}").unwrap();
        if let Some(extra) = extra {
            fs::write(directory.join(extra), b"x").unwrap();
        }
        let digest = "0".repeat(64);
        let manifest = serde_json::json!({"version": 1, "release_id": id, "files": [
            {"path": "agent-vmm", "size": 6, "sha256": digest, "executable": true},
            {"path": "guest/manifest.json", "size": 2, "sha256": digest},
        ]});
        fs::write(directory.join(MANIFEST), manifest.to_string()).unwrap();
        directory
    }

    #[test]
    fn a_release_is_installed_by_content_and_switched_by_one_link() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let paths = Paths { install_root: root.path().join("opt"), app: root.path().join("opt/current"), ..Paths::default() };
        let first = release(root.path(), "first", "agent-vmm-1", None);
        let id = install(&paths, &first).unwrap();
        assert_eq!(install(&paths, &first).unwrap(), id, "an installed release is not copied again");
        assert_eq!(release_id(&paths.install_root.join(&id)).unwrap(), "agent-vmm-1");
        assert_eq!(fs::metadata(paths.install_root.join(&id).join("agent-vmm")).unwrap().permissions().mode() & 0o777, 0o755);
        switch(&paths, &id).unwrap();
        assert_eq!(current(&paths).as_deref(), Some(id.as_str()));

        let second = release(root.path(), "second", "agent-vmm-2", None);
        let next = install(&paths, &second).unwrap();
        switch(&paths, &next).unwrap();
        prune(&paths).unwrap();
        assert!(!paths.install_root.join(&id).exists() && paths.app.join("agent-vmm").is_file());
    }

    #[test]
    fn a_tree_that_differs_from_its_inventory_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let paths = Paths { install_root: root.path().join("opt"), app: root.path().join("opt/current"), ..Paths::default() };
        let extra = release(root.path(), "extra", "agent-vmm-1", Some("unlisted"));
        assert!(install(&paths, &extra).unwrap_err().message.contains("is not listed"));
        let missing = release(root.path(), "missing", "agent-vmm-1", None);
        fs::remove_file(missing.join("guest/manifest.json")).unwrap();
        assert!(install(&paths, &missing).unwrap_err().message.contains("is missing"));
        assert_eq!(fs::read_dir(&paths.install_root).unwrap().count(), 0, "a failed installation leaves no directory");
    }
}
