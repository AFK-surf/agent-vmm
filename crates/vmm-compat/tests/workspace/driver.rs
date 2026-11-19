use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tonic::Status;
use vmm_compat::workspace::{WorkspaceDriver, WorkspaceLease};
use vmm_core::AccessMode;
use vmm_guest_proto::rpc as g;

// Paths come exclusively from the test fixture. No host directory is exported.
type Roots = HashMap<(String, Option<String>), Arc<TempDir>>;

#[derive(Default)]
pub struct TestDriver {
    roots: Mutex<Roots>,
}
struct Directory {
    root: Arc<TempDir>,
    volume: Option<String>,
    access: AccessMode,
}

#[async_trait]
impl WorkspaceDriver for TestDriver {
    async fn resolve_workspace(
        &self,
        scope: &str,
        volume: Option<&str>,
        access: AccessMode,
    ) -> Result<Arc<dyn WorkspaceLease>, Status> {
        let volume = volume.map(String::from);
        let mut roots = self.roots.lock().unwrap();
        let root = roots
            .entry((scope.into(), volume.clone()))
            .or_insert_with(|| Arc::new(tempfile::tempdir().unwrap()))
            .clone();
        Ok(Arc::new(Directory {
            root,
            volume,
            access,
        }))
    }
}

fn io(error: std::io::Error) -> Status {
    if error.kind() == std::io::ErrorKind::NotFound {
        Status::not_found("workspace entry")
    } else {
        Status::internal(error.to_string())
    }
}
impl Directory {
    fn path(&self, target: &g::WorkspaceTarget) -> Result<PathBuf, Status> {
        if self.volume != target.volume {
            return Err(Status::permission_denied("incorrect workspace"));
        }
        let mut full = self.root.path().to_owned();
        for part in Path::new(&target.path).components() {
            match part {
                Component::CurDir => {}
                Component::Normal(name) => full.push(name),
                _ => return Err(Status::permission_denied("relative path required")),
            }
            if full.is_symlink() {
                return Err(Status::permission_denied("symbolic link"));
            }
        }
        Ok(full)
    }
    fn writable(&self) -> Result<(), Status> {
        if self.access == AccessMode::ReadOnly {
            return Err(Status::permission_denied("read-only workspace"));
        }
        Ok(())
    }
}
impl WorkspaceLease for Directory {
    fn stat(&self, t: g::WorkspaceTarget) -> Result<g::WorkspaceEntry, Status> {
        let path = self.path(&t)?;
        let md = fs::metadata(&path).map_err(io)?;
        let kind = if md.is_dir() {
            g::EntryKind::Directory
        } else {
            g::EntryKind::File
        };
        let sha256 = if md.is_file() {
            Some(Sha256::digest(fs::read(path).map_err(io)?).into())
        } else {
            None
        };
        Ok(g::WorkspaceEntry {
            path: t.path,
            kind,
            size: md.len(),
            mode: 0o600,
            modified_unix_nanos: 0,
            sha256,
        })
    }
    fn list(&self, r: g::WorkspaceListRequest) -> Result<g::WorkspaceListReply, Status> {
        let mut entries = vec![];
        for item in fs::read_dir(self.path(&r.target)?).map_err(io)? {
            let item = item.map_err(io)?;
            let path = item
                .path()
                .strip_prefix(self.root.path())
                .unwrap()
                .to_str()
                .unwrap()
                .into();
            entries.push(self.stat(g::WorkspaceTarget {
                path,
                volume: self.volume.clone(),
            })?);
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        let truncated = entries.len() > r.limit as usize;
        entries.truncate(r.limit as usize);
        Ok(g::WorkspaceListReply { entries, truncated })
    }
    fn read(
        &self,
        r: g::WorkspaceReadRequest,
        out: &mut dyn Write,
    ) -> Result<g::WorkspaceReadReply, Status> {
        let bytes = fs::read(self.path(&r.target)?).map_err(io)?;
        let slice = bytes
            .get(r.offset as usize..)
            .ok_or_else(|| Status::out_of_range("offset"))?;
        let bytes_read = &slice[..slice.len().min(r.max_bytes as usize)];
        out.write_all(bytes_read).map_err(io)?;
        Ok(g::WorkspaceReadReply {
            end_offset: r.offset + bytes_read.len() as u64,
            file_size: bytes.len() as u64,
            sha256: Sha256::digest(bytes_read).into(),
        })
    }
    fn write(
        &self,
        r: g::WorkspaceWriteRequest,
        input: &mut dyn Read,
    ) -> Result<g::WorkspaceWriteReply, Status> {
        self.writable()?;
        let path = self.path(&r.target)?;
        fs::create_dir_all(path.parent().unwrap()).map_err(io)?;
        let tag = Sha256::digest(r.request_id.as_bytes());
        let stage = path.with_file_name(format!(
            ".{:x}.upload-{:x}",
            Sha256::digest(r.target.path.as_bytes()),
            tag
        ));
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(r.offset == 0)
            .open(&stage)
            .map_err(io)?;
        let mut prefix = Vec::new();
        file.read_to_end(&mut prefix).map_err(io)?;
        if prefix.len() as u64 != r.offset
            || (r.offset != 0 && r.expected_prefix_sha256 != Some(Sha256::digest(prefix).into()))
        {
            return Err(Status::failed_precondition("prefix mismatch"));
        }
        std::io::copy(input, &mut file).map_err(io)?;
        file.flush().map_err(io)?;
        drop(file);
        fs::rename(stage, path).map_err(io)?;
        let entry = self.stat(r.target)?;
        Ok(g::WorkspaceWriteReply {
            sha256: entry.sha256.unwrap(),
            entry,
            root_used_bytes: Some(self.used_bytes()?),
        })
    }
    fn import_archive(
        &self,
        r: g::WorkspaceImportRequest,
        src: &mut dyn Read,
    ) -> Result<g::WorkspaceImportReply, Status> {
        self.writable()?;
        let path = self.path(&r.target)?;
        if path.exists() {
            return Err(Status::already_exists("archive destination"));
        }
        let dir = tempfile::tempdir_in(self.root.path()).map_err(io)?;
        let mut bytes = vec![];
        src.read_to_end(&mut bytes).map_err(io)?;
        let mut count = 0;
        for entry in tar::Archive::new(bytes.as_slice()).entries().map_err(io)? {
            let mut entry = entry.map_err(io)?;
            if !(entry.header().entry_type().is_file() || entry.header().entry_type().is_dir())
                || !entry.unpack_in(dir.path()).map_err(io)?
            {
                return Err(Status::invalid_argument("unsafe archive entry"));
            }
            count += 1;
        }
        fs::rename(dir.path(), path).map_err(io)?;
        Ok(g::WorkspaceImportReply {
            entry: self.stat(r.target)?,
            sha256: Sha256::digest(&bytes).into(),
            entries: count,
            bytes: bytes.len() as u64,
            root_used_bytes: Some(self.used_bytes()?),
        })
    }
    fn export_archive(
        &self,
        t: g::WorkspaceTarget,
        out: &mut dyn Write,
    ) -> Result<g::WorkspaceExportReply, Status> {
        let path = self.path(&t)?;
        let mut tar = tar::Builder::new(Vec::new());
        tar.append_dir_all("tree", path).map_err(io)?;
        let bytes = tar.into_inner().map_err(io)?;
        out.write_all(&bytes).map_err(io)?;
        Ok(g::WorkspaceExportReply {
            sha256: Sha256::digest(&bytes).into(),
            entries: 0,
            bytes: bytes.len() as u64,
        })
    }
    fn used_bytes(&self) -> Result<u64, Status> {
        fn size(p: &Path) -> std::io::Result<u64> {
            if p.is_file() {
                return Ok(fs::metadata(p)?.len());
            }
            fs::read_dir(p)?.try_fold(0, |n, e| Ok(n + size(&e?.path())?))
        }
        size(self.root.path()).map_err(io)
    }
}
