//! Resolve tar entries in a virtual tree. Archive names never become host paths.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tar::{Archive, Builder, EntryType, Header};
use vmm_core::{Error, Result};

pub(super) fn relative(path: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::Normal(s) => out.push(s),
            Component::CurDir => {}
            _ => return Err(Error::Invalid("archive path")),
        }
    }
    Ok(out)
}

pub(super) struct Inode {
    pub(super) header: Header,
    pub(super) data: u64,
    pub(super) link: Option<PathBuf>,
}

pub(super) struct Tree {
    pub(super) nodes: BTreeMap<PathBuf, Arc<Inode>>,
    pub(super) file: File,
}

impl Tree {
    pub fn new(file: File) -> Self {
        Self {
            nodes: BTreeMap::new(),
            file,
        }
    }

    pub fn overlay(&mut self, layer: impl Read) -> Result<()> {
        let mut additions = Vec::new();
        let mut removed = BTreeSet::new();
        let mut opaque = BTreeSet::new();
        for entry in Archive::new(layer).entries()? {
            let mut entry = entry?;
            let path = relative(&entry.path()?)?;
            if path.as_os_str().is_empty() {
                continue;
            }
            let name = path
                .file_name()
                .ok_or(Error::Invalid("layer path"))?
                .as_encoded_bytes();
            if name == b".wh..wh..opq" {
                opaque.insert(path.parent().unwrap().to_owned());
                continue;
            }
            if let Some(victim) = name.strip_prefix(b".wh.") {
                use std::os::unix::ffi::OsStrExt;
                if victim.is_empty() || victim == b"." || victim == b".." {
                    return Err(Error::Invalid("whiteout"));
                }
                removed.insert(path.with_file_name(std::ffi::OsStr::from_bytes(victim)));
                continue;
            }
            let mut header = entry.header().clone();
            let kind = header.entry_type();
            if !(kind.is_dir() || kind.is_file() || kind.is_symlink() || kind.is_hard_link()) {
                return Err(Error::Invalid("unsupported image inode"));
            }
            let link = entry.link_name()?.map(|p| p.into_owned());
            let data = self.file.stream_position()?;
            if kind.is_file() {
                let size = std::io::copy(&mut entry, &mut self.file)?;
                header.set_size(size);
            } else {
                header.set_size(0);
            }
            additions.push((path, Arc::new(Inode { header, data, link })));
        }
        self.nodes.retain(|p, _| {
            !p.ancestors().any(|a| removed.contains(a))
                && !p.ancestors().skip(1).any(|a| opaque.contains(a))
        });
        for (path, mut inode) in additions {
            let kind = inode.header.entry_type();
            if kind.is_hard_link() {
                let target = relative(
                    inode
                        .link
                        .as_deref()
                        .ok_or(Error::Invalid("hardlink target"))?,
                )?;
                inode = self
                    .nodes
                    .get(&target)
                    .filter(|n| n.header.entry_type().is_file())
                    .cloned()
                    .ok_or(Error::Invalid("missing hardlink target"))?;
            }
            if path.ancestors().skip(1).any(|p| {
                self.nodes
                    .get(p)
                    .is_some_and(|n| !n.header.entry_type().is_dir())
            }) {
                return Err(Error::Invalid("image entry under non-directory"));
            }
            if !kind.is_dir() {
                self.nodes.retain(|p, _| !p.starts_with(&path));
            }
            self.nodes.insert(path, inode);
        }
        Ok(())
    }

    pub fn write_tar(&mut self, dest: impl Write) -> Result<()> {
        let mut archive = Builder::new(dest);
        let mut links = BTreeMap::new();
        for (path, node) in &self.nodes {
            let mut header = node.header.clone();
            if header.entry_type().is_file() {
                if let Some(target) = links.insert(Arc::as_ptr(node), path) {
                    header.set_entry_type(EntryType::Link);
                    header.set_size(0);
                    archive.append_link(&mut header, path, target)?;
                } else {
                    self.file.seek(SeekFrom::Start(node.data))?;
                    let len = header.size()?;
                    archive.append_data(&mut header, path, (&mut self.file).take(len))?;
                }
            } else if header.entry_type().is_symlink() {
                archive.append_link(
                    &mut header,
                    path,
                    node.link
                        .as_deref()
                        .ok_or(Error::Invalid("symlink target"))?,
                )?;
            } else {
                archive.append_data(&mut header, path, std::io::empty())?;
            }
        }
        archive.finish()?;
        Ok(())
    }
}

impl Tree {
    pub fn from_directory(root: &Path, staging: &Path) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let mut tree = Self::new(tempfile::tempfile_in(staging)?);
        let mut pending = vec![PathBuf::new()];
        let mut links = BTreeMap::new();
        while let Some(path) = pending.pop() {
            let src = root.join(&path);
            let meta = std::fs::symlink_metadata(&src)?;
            let mut header = Header::new_gnu();
            header.set_metadata(&meta);
            let data = tree.file.stream_position()?;
            let mut link = None;
            if meta.is_dir() {
                for child in std::fs::read_dir(&src)? {
                    pending.push(path.join(child?.file_name()));
                }
            } else if meta.is_symlink() {
                link = Some(std::fs::read_link(&src)?);
            } else if meta.is_file() {
                let key = (meta.dev(), meta.ino());
                if let Some(existing) = links.get(&key) {
                    tree.nodes.insert(path, Arc::clone(existing));
                    continue;
                }
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
                    .open(src)?;
                if std::io::copy(&mut file, &mut tree.file)? != meta.len() {
                    return Err(Error::Conflict("image source changed"));
                }
                let node = Arc::new(Inode { header, data, link });
                links.insert(key, node.clone());
                tree.nodes.insert(path, node);
                continue;
            } else {
                return Err(Error::Invalid("unsupported image inode"));
            }
            tree.nodes
                .insert(path, Arc::new(Inode { header, data, link }));
        }
        Ok(tree)
    }
}
