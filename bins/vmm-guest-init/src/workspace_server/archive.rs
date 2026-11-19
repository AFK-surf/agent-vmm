//! Tar import into a new directory and tar export of a tree.
//!
//! Archives hold regular files and directories only. Both directions resolve
//! each name beneath the workspace root without following a symbolic link.

use std::ffi::{CString, OsStr};
use std::fs::{File, Permissions};
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};

use sha2::{Digest, Sha256};
use tar::EntryType;
use vmm_guest_proto::frame::ServerFrame;
use vmm_guest_proto::rpc::{
    ErrorCode, MAX_ARCHIVE_BYTES, MAX_ARCHIVE_ENTRIES, MAX_CHUNK_BYTES, RpcError, Sha256Digest,
    WorkspaceExportReply, WorkspaceImportReply, WorkspaceImportRequest, WorkspaceTarget,
};

use super::write::{Owner, create_dirs, staging_name};
use super::{
    MAX_TREE_DEPTH, WorkspaceServer, c_string, clean_relative, entry_of, invalid, join,
    open_beneath, read_dir, split_parent,
};
use crate::rpc_server::Outbound;
use crate::{check_request_id, io_error, sys};

fn too_large() -> io::Error {
    io::Error::other("the archive exceeds the size limit")
}

fn archive_error(error: io::Error) -> RpcError {
    invalid(format!("archive: {error}"))
}

/// Counts and digests the bytes of an archive stream.
struct Metered<T> {
    inner: T,
    hasher: Sha256,
    bytes: u64,
}

impl<T> Metered<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn account(&mut self, data: &[u8]) -> io::Result<()> {
        self.bytes += data.len() as u64;
        self.hasher.update(data);
        if self.bytes > MAX_ARCHIVE_BYTES {
            Err(too_large())
        } else {
            Ok(())
        }
    }
}

impl<T: Read> Read for Metered<T> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.account(&buffer[..count])?;
        Ok(count)
    }
}

/// Sends the archive in `Data` frames of the chunk size.
struct FrameSink<'a> {
    outbound: &'a Outbound,
    chunk: Vec<u8>,
}

impl Write for FrameSink<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let count = data.len().min(MAX_CHUNK_BYTES - self.chunk.len());
        self.chunk.extend_from_slice(&data[..count]);
        if self.chunk.len() == MAX_CHUNK_BYTES {
            self.flush()?;
        }
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.chunk, Vec::with_capacity(MAX_CHUNK_BYTES));
        self.outbound.send(&ServerFrame::Data(chunk))
    }
}

impl<T: Write> Write for Metered<T> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(data)?;
        self.account(&data[..count])?;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Reads exactly the size that the tar header announced.
struct Exact<'a> {
    file: &'a File,
    offset: u64,
    remaining: u64,
}

impl Read for Exact<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let want = buffer
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        if want == 0 {
            return Ok(0);
        }
        let count = self.file.read_at(&mut buffer[..want], self.offset)?;
        if count == 0 {
            return Err(io::Error::other("a file shrank during the export"));
        }
        self.offset += count as u64;
        self.remaining -= count as u64;
        Ok(count)
    }
}

/// Removes a file or a directory tree. A missing entry is not an error.
fn remove_tree(parent: &File, name: &OsStr, depth: usize) -> io::Result<()> {
    let c_name = CString::new(name.as_bytes())?;
    match sys::unlinkat(parent.as_fd(), &c_name, false) {
        Err(error) if error.raw_os_error() == Some(libc::EISDIR) => {}
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
        result => return result,
    }
    if depth > MAX_TREE_DEPTH {
        return Err(io::Error::other("the directory tree is too deep"));
    }
    let dir = open_beneath(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    for child in read_dir(&dir)? {
        remove_tree(&dir, &child?.file_name(), depth + 1)?;
    }
    sys::unlinkat(parent.as_fd(), &c_name, true)
}

/// Extracts the tar stream into `staging`. Returns the entry count.
fn extract(staging: &File, owner: &Owner, stream: &mut dyn Read) -> Result<u64, RpcError> {
    let mut archive = tar::Archive::new(stream);
    let mut entries = 0;
    for entry in archive.entries().map_err(archive_error)? {
        let mut entry = entry.map_err(archive_error)?;
        let kind = entry.header().entry_type();
        if kind == EntryType::XGlobalHeader {
            continue;
        }
        entries += 1;
        if entries > MAX_ARCHIVE_ENTRIES {
            return Err(RpcError::new(
                ErrorCode::ResourceExhausted,
                "too many archive entries",
            ));
        }
        let path = {
            let raw = entry.path().map_err(archive_error)?;
            clean_relative(raw.to_str().ok_or_else(|| invalid("archive entry name"))?)?
        };
        let mode = Permissions::from_mode(entry.header().mode().map_err(archive_error)? & 0o777);
        if kind == EntryType::Directory {
            let dir = create_dirs(staging, &path, owner).map_err(io_error)?;
            if path != "." {
                dir.set_permissions(mode).map_err(io_error)?;
            }
        } else if kind.is_file() {
            let Some((parent, name)) = split_parent(&path) else {
                return Err(invalid("archive entry name"));
            };
            let parent = create_dirs(staging, parent, owner).map_err(io_error)?;
            let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW;
            let mut file = open_beneath(&parent, name, flags, 0o600).map_err(io_error)?;
            io::copy(&mut entry, &mut file).map_err(archive_error)?;
            file.set_permissions(mode).map_err(io_error)?;
            owner.apply(&file).map_err(io_error)?;
        } else {
            return Err(invalid(
                "archive entries must be regular files or directories",
            ));
        }
    }
    Ok(entries)
}

fn append_tree(
    builder: &mut tar::Builder<&mut dyn Write>,
    file: &File,
    path: &str,
    entries: &mut u64,
    depth: usize,
) -> Result<(), RpcError> {
    let metadata = file.metadata().map_err(io_error)?;
    let mut header = tar::Header::new_gnu();
    header.set_mode(metadata.mode() & 0o7777);
    header.set_uid(metadata.uid().into());
    header.set_gid(metadata.gid().into());
    header.set_mtime(metadata.mtime().max(0) as u64);
    header.set_size(0);
    if *entries >= MAX_ARCHIVE_ENTRIES {
        return Err(RpcError::new(
            ErrorCode::ResourceExhausted,
            "too many archive entries",
        ));
    }
    if metadata.is_file() {
        *entries += 1;
        header.set_entry_type(EntryType::Regular);
        header.set_size(metadata.len());
        let content = Exact {
            file,
            offset: 0,
            remaining: metadata.len(),
        };
        builder
            .append_data(&mut header, path, content)
            .map_err(io_error)?;
    } else if metadata.is_dir() {
        if path != "." {
            *entries += 1;
            header.set_entry_type(EntryType::Directory);
            builder
                .append_data(&mut header, format!("{path}/"), io::empty())
                .map_err(io_error)?;
        }
        if depth >= MAX_TREE_DEPTH {
            return Err(invalid("the directory tree is too deep"));
        }
        let names: io::Result<Vec<_>> = read_dir(file)
            .map_err(io_error)?
            .map(|child| Ok(child?.file_name()))
            .collect();
        let mut names = names.map_err(io_error)?;
        names.sort();
        for name in names {
            let Some(name) = name.to_str() else {
                return Err(invalid("a file name is not UTF-8"));
            };
            let child = open_beneath(file, name, libc::O_RDONLY, 0).map_err(|error| match error
                .raw_os_error()
            {
                Some(libc::ELOOP) => invalid("an archive cannot hold a symbolic link"),
                _ => io_error(error),
            })?;
            append_tree(builder, &child, &join(path, name), entries, depth + 1)?;
        }
    } else {
        return Err(invalid(
            "an archive holds only regular files and directories",
        ));
    }
    Ok(())
}

/// Removes the staging directory of an import that did not complete.
struct Staging<'a> {
    parent: &'a File,
    name: &'a str,
    committed: bool,
}

impl Drop for Staging<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = remove_tree(self.parent, self.name.as_ref(), 0);
        }
    }
}

impl WorkspaceServer {
    /// Extracts a tar stream into a directory that does not exist yet.
    ///
    /// The tree is built in a hidden staging directory and renamed to the
    /// target at the end, so a failed import leaves nothing behind.
    pub fn import_archive(
        &self,
        request: &WorkspaceImportRequest,
        upload: &mut dyn Read,
    ) -> Result<WorkspaceImportReply, RpcError> {
        check_request_id(&request.request_id)?;
        let located = self.locate(&request.target)?;
        let Some((parent_path, name)) = split_parent(&located.path) else {
            return Err(invalid("an import requires a new directory path"));
        };
        let owner = Owner::of(&located.root).map_err(io_error)?;
        let parent = create_dirs(&located.root, parent_path, &owner).map_err(io_error)?;
        match open_beneath(&parent, name, libc::O_PATH, 0) {
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {}
            _ => {
                return Err(RpcError::new(
                    ErrorCode::AlreadyExists,
                    "the import target exists",
                ));
            }
        }
        let staging_name = staging_name(name, "extract", &request.request_id)?;
        let (from, to) = (c_string(&staging_name)?, c_string(name)?);
        // Removes what an interrupted import of this request left.
        remove_tree(&parent, staging_name.as_ref(), 0).map_err(io_error)?;
        sys::mkdirat(parent.as_fd(), &from, 0o700).map_err(io_error)?;
        let mut cleanup = Staging {
            parent: &parent,
            name: &staging_name,
            committed: false,
        };

        let flags = libc::O_RDONLY | libc::O_DIRECTORY;
        let staging = open_beneath(&parent, &staging_name, flags, 0).map_err(io_error)?;
        owner.apply(&staging).map_err(io_error)?;
        let mut stream = Metered::new(upload);
        let entries = extract(&staging, &owner, &mut stream)?;
        // Reads the padding after the archive, up to the client's `End`.
        io::copy(&mut stream, &mut io::sink()).map_err(archive_error)?;
        sys::rename_in(parent.as_fd(), &from, &to, false).map_err(io_error)?;
        cleanup.committed = true;
        parent.sync_all().map_err(io_error)?;
        Ok(WorkspaceImportReply {
            entry: entry_of(&located.path, &staging, false)?,
            sha256: Sha256Digest::from(stream.hasher.finalize()),
            entries,
            bytes: stream.bytes,
            root_used_bytes: sys::used_bytes(&located.root).ok(),
        })
    }

    /// Sends a tar stream of a file or of a directory tree in `Data` frames.
    /// Entry names are relative to the workspace root.
    pub fn export_archive(
        &self,
        target: &WorkspaceTarget,
        outbound: &Outbound,
    ) -> Result<WorkspaceExportReply, RpcError> {
        let located = self.locate(target)?;
        let file =
            open_beneath(&located.root, &located.path, libc::O_RDONLY, 0).map_err(io_error)?;
        let mut sink = Metered::new(FrameSink {
            outbound,
            chunk: Vec::new(),
        });
        let mut entries = 0;
        let mut builder = tar::Builder::new(&mut sink as &mut dyn Write);
        append_tree(&mut builder, &file, &located.path, &mut entries, 0)?;
        builder
            .into_inner()
            .map_err(io_error)?
            .flush()
            .map_err(io_error)?;
        Ok(WorkspaceExportReply {
            sha256: Sha256Digest::from(sink.hasher.finalize()),
            entries,
            bytes: sink.bytes,
        })
    }
}
