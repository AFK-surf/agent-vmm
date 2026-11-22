//! File operations beneath a workspace root.
//!
//! Each path is resolved by `openat2` with `RESOLVE_BENEATH` and
//! `RESOLVE_NO_SYMLINKS`, so no request can leave its root: a `..` that climbs
//! above the root and any symbolic link in the path fail.

mod archive;
mod write;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsStr};
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;

use sha2::{Digest, Sha256};
use vmm_guest_proto::frame::ServerFrame;
use vmm_guest_proto::rpc::{
    EntryKind, ErrorCode, MAX_CHUNK_BYTES, MAX_LIST_ENTRIES, MAX_WORKSPACE_PATH_BYTES, RpcError,
    Sha256Digest, WorkspaceEntry, WorkspaceListReply, WorkspaceListRequest, WorkspaceReadReply,
    WorkspaceReadRequest, WorkspaceTarget,
};

use crate::rpc_server::Outbound;
use crate::{io_error, sys};

/// Largest file that `WorkspaceStat` digests.
const MAX_DIGEST_BYTES: u64 = 256 << 20;
/// Keeps a `WorkspaceList` reply inside one frame.
const MAX_LIST_REPLY_BYTES: usize = 768 * 1024;
/// Deepest directory tree that an archive operation walks.
const MAX_TREE_DEPTH: usize = 256;

pub struct WorkspaceServer {
    default_root: Option<PathBuf>,
    volumes: BTreeMap<String, PathBuf>,
}

/// An open workspace root and a normalized path beneath it. `.` names the root.
struct Located {
    root: File,
    path: String,
}

fn invalid(message: impl Into<String>) -> RpcError {
    RpcError::new(ErrorCode::InvalidArgument, message)
}

/// Normalizes a relative path. A path that climbs above its root fails.
fn clean_relative(path: &str) -> Result<String, RpcError> {
    if path.len() > MAX_WORKSPACE_PATH_BYTES || path.contains('\0') {
        return Err(invalid("the path is too long or holds a NUL byte"));
    }
    if path.starts_with('/') {
        return Err(RpcError::new(
            ErrorCode::PathEscape,
            "the path must be relative",
        ));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(RpcError::new(
                        ErrorCode::PathEscape,
                        "the path leaves its root",
                    ));
                }
            }
            part => parts.push(part),
        }
    }
    Ok(if parts.is_empty() {
        ".".into()
    } else {
        parts.join("/")
    })
}

fn c_string(name: &str) -> Result<CString, RpcError> {
    CString::new(name).map_err(|_| invalid("the path holds a NUL byte"))
}

/// Splits a normalized path into its parent and its last name. The root has neither.
fn split_parent(path: &str) -> Option<(&str, &str)> {
    match path.rsplit_once('/') {
        _ if path == "." => None,
        Some(split) => Some(split),
        None => Some((".", path)),
    }
}

fn join(parent: &str, name: &str) -> String {
    if parent == "." {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

/// Opens `path` beneath `dir` without following any symbolic link.
fn open_beneath(
    dir: &File,
    path: impl AsRef<OsStr>,
    mut flags: libc::c_int,
    mode: u32,
) -> io::Result<File> {
    // `openat2` rejects `O_PATH | O_NONBLOCK`. Other opens must not block on a FIFO.
    if flags & libc::O_PATH == 0 {
        flags |= libc::O_NONBLOCK;
    }
    let path = CString::new(path.as_ref().as_bytes())?;
    let resolve = sys::RESOLVE_BENEATH | sys::RESOLVE_NO_SYMLINKS | sys::RESOLVE_NO_MAGICLINKS;
    sys::openat2(dir.as_fd(), &path, flags, mode, resolve).map(File::from)
}

/// Lists a directory through its descriptor, so the listing is of the opened directory.
fn read_dir(dir: &File) -> io::Result<fs::ReadDir> {
    fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))
}

fn sha256_of(file: &File, hasher: &mut Sha256) -> io::Result<u64> {
    let mut buffer = vec![0u8; MAX_CHUNK_BYTES];
    let mut offset = 0;
    loop {
        match file.read_at(&mut buffer, offset)? {
            0 => return Ok(offset),
            count => {
                hasher.update(&buffer[..count]);
                offset += count as u64;
            }
        }
    }
}

fn entry_of(path: &str, file: &File, digest: bool) -> Result<WorkspaceEntry, RpcError> {
    let metadata = file.metadata().map_err(io_error)?;
    let kind = if metadata.is_file() {
        EntryKind::File
    } else if metadata.is_dir() {
        EntryKind::Directory
    } else {
        return Err(invalid("the entry is neither a file nor a directory"));
    };
    let mut sha256 = None;
    if digest && kind == EntryKind::File && metadata.len() <= MAX_DIGEST_BYTES {
        let mut hasher = Sha256::new();
        sha256_of(file, &mut hasher).map_err(io_error)?;
        sha256 = Some(Sha256Digest::from(hasher.finalize()));
    }
    Ok(WorkspaceEntry {
        path: path.to_owned(),
        kind,
        size: metadata.len(),
        mode: metadata.mode() & 0o7777,
        modified_unix_nanos: metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec(),
        sha256,
    })
}

impl WorkspaceServer {
    /// `default_root` serves targets without a volume. `volumes` maps each
    /// volume name to its mount path.
    pub fn new(default_root: Option<PathBuf>, volumes: BTreeMap<String, PathBuf>) -> Self {
        Self {
            default_root,
            volumes,
        }
    }

    fn locate(&self, target: &WorkspaceTarget) -> Result<Located, RpcError> {
        let root = match &target.volume {
            Some(name) => self
                .volumes
                .get(name)
                .ok_or_else(|| RpcError::new(ErrorCode::NotFound, "no such volume"))?,
            None => self.default_root.as_ref().ok_or_else(|| {
                RpcError::new(
                    ErrorCode::FailedPrecondition,
                    "the guest has no default workspace",
                )
            })?,
        };
        let path = clean_relative(&target.path)?;
        let root = File::options()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(root)
            .map_err(io_error)?;
        Ok(Located { root, path })
    }

    /// Describes a file or directory. A file of at most 256 MiB gets its digest.
    pub fn stat(&self, target: &WorkspaceTarget) -> Result<WorkspaceEntry, RpcError> {
        let located = self.locate(target)?;
        let file = open_beneath(&located.root, &located.path, libc::O_RDONLY, 0);
        entry_of(&located.path, &file.map_err(io_error)?, true)
    }

    pub fn list(&self, request: &WorkspaceListRequest) -> Result<WorkspaceListReply, RpcError> {
        if let Some(after) = &request.after
            && !after.is_empty()
            && (after.len() > MAX_WORKSPACE_PATH_BYTES
                || after.contains('/')
                || after.contains('\0'))
        {
            return Err(invalid("invalid list cursor"));
        }
        let after = request.after.as_deref().filter(|cursor| !cursor.is_empty());
        let located = self.locate(&request.target)?;
        let flags = libc::O_RDONLY | libc::O_DIRECTORY;
        let dir = open_beneath(&located.root, &located.path, flags, 0).map_err(io_error)?;
        let limit = match request.limit {
            0 => MAX_LIST_ENTRIES,
            limit => limit.min(MAX_LIST_ENTRIES),
        } as usize;
        // Keeps the first `limit` names after `after` in order without holding a large directory.
        let mut names = BTreeSet::new();
        let mut truncated = false;
        for child in read_dir(&dir).map_err(io_error)? {
            let child = child.map_err(io_error)?;
            let kind = child.file_type().map_err(io_error)?;
            let Ok(name) = child.file_name().into_string() else {
                continue;
            };
            if (kind.is_file() || kind.is_dir())
                && after.is_none_or(|cursor| name.as_str() > cursor)
            {
                names.insert(name);
                if names.len() > limit {
                    names.pop_last();
                    truncated = true;
                }
            }
        }
        let mut entries = Vec::with_capacity(names.len());
        let mut last_name = None;
        let mut room = MAX_LIST_REPLY_BYTES;
        for name in names {
            let path = join(&located.path, &name);
            let Some(left) = room.checked_sub(path.len() + 64) else {
                truncated = true;
                break;
            };
            room = left;
            match open_beneath(&dir, &name, libc::O_PATH, 0) {
                Ok(file) => {
                    entries.push(entry_of(&path, &file, false)?);
                    last_name = Some(name);
                }
                // The entry was removed or replaced after the listing.
                Err(error) if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ELOOP)) => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        let next_cursor = if truncated { last_name } else { None };
        Ok(WorkspaceListReply {
            entries,
            truncated,
            next_cursor,
        })
    }

    /// Sends a range of a file in `Data` frames.
    pub fn read(
        &self,
        request: &WorkspaceReadRequest,
        outbound: &Outbound,
    ) -> Result<WorkspaceReadReply, RpcError> {
        let located = self.locate(&request.target)?;
        let file =
            open_beneath(&located.root, &located.path, libc::O_RDONLY, 0).map_err(io_error)?;
        let metadata = file.metadata().map_err(io_error)?;
        if !metadata.is_file() {
            return Err(invalid("a read requires a regular file"));
        }
        if request.offset > metadata.len() {
            return Err(invalid("the offset is after the end of the file"));
        }
        let mut remaining = match request.max_bytes {
            0 => u64::MAX,
            max_bytes => max_bytes,
        };
        let mut offset = request.offset;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; MAX_CHUNK_BYTES];
        while remaining > 0 {
            let want = remaining.min(buffer.len() as u64) as usize;
            let count = file
                .read_at(&mut buffer[..want], offset)
                .map_err(io_error)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            outbound
                .send(&ServerFrame::Data(buffer[..count].to_vec()))
                .map_err(io_error)?;
            offset += count as u64;
            remaining -= count as u64;
        }
        Ok(WorkspaceReadReply {
            end_offset: offset,
            file_size: metadata.len(),
            sha256: hasher.finalize().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_normalize_and_cannot_climb_above_the_root() {
        assert_eq!(clean_relative("").unwrap(), ".");
        assert_eq!(clean_relative("./a//b/").unwrap(), "a/b");
        assert_eq!(clean_relative("a/../b").unwrap(), "b");
        for escape in ["..", "../x", "a/../../x", "/etc/passwd"] {
            let error = clean_relative(escape).unwrap_err();
            assert_eq!(error.code, ErrorCode::PathEscape, "{escape}");
        }
        assert_eq!(
            clean_relative("a\0b").unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(split_parent("."), None);
        assert_eq!(split_parent("a"), Some((".", "a")));
        assert_eq!(split_parent("a/b/c"), Some(("a/b", "c")));
    }
}
