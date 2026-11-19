//! Resumable file upload that replaces its target in one rename.

use std::ffi::CString;
use std::fs::{File, Permissions};
use std::io::{self, Read};
use std::os::fd::AsFd;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt, fchown};

use sha2::{Digest, Sha256};
use vmm_guest_proto::rpc::{
    ErrorCode, MAX_CHUNK_BYTES, RpcError, Sha256Digest, WorkspaceWriteReply, WorkspaceWriteRequest,
};

use super::{WorkspaceServer, c_string, entry_of, invalid, open_beneath, sha256_of, split_parent};
use crate::{check_request_id, io_error, sys};

/// Owner of a workspace root. Entries that PID 1 creates get this owner, so
/// the workload user can use them.
pub(super) struct Owner {
    uid: u32,
    gid: u32,
}

impl Owner {
    pub(super) fn of(root: &File) -> io::Result<Self> {
        let metadata = root.metadata()?;
        Ok(Self {
            uid: metadata.uid(),
            gid: metadata.gid(),
        })
    }

    pub(super) fn apply(&self, file: &File) -> io::Result<()> {
        let metadata = file.metadata()?;
        if (metadata.uid(), metadata.gid()) != (self.uid, self.gid) {
            fchown(file, Some(self.uid), Some(self.gid))?;
        }
        Ok(())
    }
}

/// Opens the directory `path` beneath `root` and creates its missing parts.
pub(super) fn create_dirs(root: &File, path: &str, owner: &Owner) -> io::Result<File> {
    let flags = libc::O_RDONLY | libc::O_DIRECTORY;
    let mut current = root.try_clone()?;
    for part in path.split('/').filter(|part| *part != ".") {
        current = match open_beneath(&current, part, flags, 0) {
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                let created = match sys::mkdirat(current.as_fd(), &CString::new(part)?, 0o700) {
                    Ok(()) => true,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => false,
                    Err(error) => return Err(error),
                };
                let dir = open_beneath(&current, part, flags, 0)?;
                if created {
                    owner.apply(&dir)?;
                }
                dir
            }
            opened => opened?,
        };
    }
    Ok(current)
}

/// Name of the hidden staging entry of one request beside its target.
pub(super) fn staging_name(name: &str, kind: &str, request_id: &str) -> Result<String, RpcError> {
    let digest = Sha256::digest(request_id.as_bytes());
    let tag: String = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let staging = format!(".{name}.{kind}-{tag}");
    if staging.len() > 255 {
        return Err(invalid("the file name is too long"));
    }
    Ok(staging)
}

fn precondition(message: impl Into<String>) -> RpcError {
    RpcError::new(ErrorCode::FailedPrecondition, message)
}

impl WorkspaceServer {
    /// Writes the upload to a staging file, then renames it over the target.
    ///
    /// The target changes only at the end, in one step. An interrupted upload
    /// keeps its staging file: a retry with the same `request_id` continues at
    /// `offset` when `expected_prefix_sha256` matches the staged bytes.
    pub fn write(
        &self,
        request: &WorkspaceWriteRequest,
        upload: &mut dyn Read,
    ) -> Result<WorkspaceWriteReply, RpcError> {
        check_request_id(&request.request_id)?;
        if request.mode & !0o777 != 0 {
            return Err(invalid("mode"));
        }
        let located = self.locate(&request.target)?;
        let Some((parent_path, name)) = split_parent(&located.path) else {
            return Err(invalid("a write requires a file path"));
        };
        let owner = Owner::of(&located.root).map_err(io_error)?;
        let parent = create_dirs(&located.root, parent_path, &owner).map_err(io_error)?;
        let staging_name = staging_name(name, "upload", &request.request_id)?;
        let flags = libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW;
        let staging = open_beneath(&parent, &staging_name, flags, 0o600).map_err(io_error)?;
        if !staging.metadata().map_err(io_error)?.is_file() {
            return Err(invalid("the staging entry is not a regular file"));
        }
        sys::try_lock_exclusive(&staging)
            .map_err(|_| precondition("another upload of this request is in progress"))?;

        let mut hasher = Sha256::new();
        if request.offset == 0 {
            staging.set_len(0).map_err(io_error)?;
        } else {
            let staged = sha256_of(&staging, &mut hasher).map_err(io_error)?;
            if staged != request.offset {
                return Err(precondition(format!(
                    "offset {} does not match the {staged} staged bytes",
                    request.offset
                )));
            }
            let prefix = Sha256Digest::from(hasher.clone().finalize());
            if request.expected_prefix_sha256 != Some(prefix) {
                return Err(precondition(
                    "expected_prefix_sha256 does not match the staged bytes",
                ));
            }
        }

        let mut offset = request.offset;
        let mut buffer = vec![0u8; MAX_CHUNK_BYTES];
        loop {
            let count = upload.read(&mut buffer).map_err(io_error)?;
            if count == 0 {
                break;
            }
            staging
                .write_all_at(&buffer[..count], offset)
                .map_err(io_error)?;
            hasher.update(&buffer[..count]);
            offset += count as u64;
        }

        let mode = if request.mode == 0 {
            0o600
        } else {
            request.mode
        };
        staging
            .set_permissions(Permissions::from_mode(mode))
            .map_err(io_error)?;
        owner.apply(&staging).map_err(io_error)?;
        staging.sync_all().map_err(io_error)?;
        sys::rename_in(
            parent.as_fd(),
            &c_string(&staging_name)?,
            &c_string(name)?,
            true,
        )
        .map_err(io_error)?;
        parent.sync_all().map_err(io_error)?;

        let sha256 = Sha256Digest::from(hasher.finalize());
        let mut entry = entry_of(&located.path, &staging, false)?;
        entry.sha256 = Some(sha256);
        Ok(WorkspaceWriteReply {
            entry,
            sha256,
            root_used_bytes: sys::used_bytes(&located.root).ok(),
        })
    }
}
