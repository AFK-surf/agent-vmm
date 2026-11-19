//! Lifecycle state lives in a directory that a non-root service user owns and
//! that a root installer can also write. Every access resolves one name below
//! an open directory and never follows a symbolic link.

use crate::error::{Error, Kind, Result, err};
use rustix::fs::{AtFlags, Dir, FileType, Gid, Mode, OFlags, Uid};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::path::Path;

/// The service identity that a root installer gives the files it writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileOwner {
    pub uid: u32,
    pub gid: u32,
}

pub struct StateDir(OwnedFd);

const DIRECTORY: OFlags = OFlags::RDONLY.union(OFlags::DIRECTORY).union(OFlags::NOFOLLOW).union(OFlags::CLOEXEC);

fn single_name(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(err("state entry name is invalid"));
    }
    Ok(())
}

fn chown(fd: BorrowedFd<'_>, owner: Option<FileOwner>) -> Result<()> {
    if let Some(owner) = owner {
        rustix::fs::fchown(fd, Some(Uid::from_raw(owner.uid)), Some(Gid::from_raw(owner.gid)))?;
    }
    Ok(())
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn random_hex(bytes: usize) -> String {
    let mut value = vec![0u8; bytes];
    // The identifier only has to be unique; the clock replaces a failed generator.
    if getrandom::fill(&mut value).is_err() {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        return format!("{nanos:x}");
    }
    hex(&value)
}

impl StateDir {
    pub fn open(path: &Path, owner: Option<FileOwner>) -> Result<Self> {
        let fd = rustix::fs::open(path, DIRECTORY, Mode::empty())?;
        chown(fd.as_fd(), owner)?;
        Ok(Self(fd))
    }

    pub fn child(&self, name: &str) -> Result<Self> {
        single_name(name)?;
        Ok(Self(rustix::fs::openat(&self.0, name, DIRECTORY, Mode::empty())?))
    }

    /// Opens `name`, creating it with mode 0700 when it is missing.
    pub fn child_or_create(&self, name: &str, owner: Option<FileOwner>) -> Result<Self> {
        single_name(name)?;
        let created = match rustix::fs::mkdirat(&self.0, name, Mode::RWXU) {
            Ok(()) => true,
            Err(rustix::io::Errno::EXIST) => false,
            Err(error) => return Err(error.into()),
        };
        let directory = self.child(name)?;
        if created {
            rustix::fs::fchmod(&directory.0, Mode::RWXU)?;
        }
        chown(directory.0.as_fd(), owner)?;
        if created {
            self.sync()?;
        }
        Ok(directory)
    }

    pub fn sync(&self) -> Result<()> {
        Ok(rustix::fs::fsync(&self.0)?)
    }

    pub fn read(&self, name: &str, limit: u64) -> Result<Vec<u8>> {
        single_name(name)?;
        let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let fd = rustix::fs::openat(&self.0, name, flags, Mode::empty())?;
        if FileType::from_raw_mode(rustix::fs::fstat(&fd)?.st_mode) != FileType::RegularFile {
            return Err(err("lifecycle state is not a regular file"));
        }
        let mut data = Vec::new();
        File::from(fd).take(limit + 1).read_to_end(&mut data)?;
        if data.len() as u64 > limit {
            return Err(err(format!("lifecycle state exceeds {limit} bytes")));
        }
        Ok(data)
    }

    /// Replaces `name` with complete contents and syncs the directory.
    pub fn write(&self, name: &str, data: &[u8], mode: u32, owner: Option<FileOwner>) -> Result<()> {
        single_name(name)?;
        let temporary = format!(".{name}.{}.tmp", random_hex(8));
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let fd = rustix::fs::openat(&self.0, &temporary, flags, Mode::from_raw_mode(mode as _))?;
        let published = (|| -> Result<()> {
            rustix::fs::fchmod(&fd, Mode::from_raw_mode(mode as _))?;
            chown(fd.as_fd(), owner)?;
            let mut file = File::from(fd);
            file.write_all(data)?;
            file.sync_all()?;
            rustix::fs::renameat(&self.0, &temporary, &self.0, name)?;
            Ok(())
        })();
        if published.is_err() {
            let _ = rustix::fs::unlinkat(&self.0, &temporary, AtFlags::empty());
        }
        published?;
        self.sync()
    }

    /// Opens the lock file, creating it when missing. The caller locks the returned file.
    pub fn lock_file(&self, name: &str, owner: Option<FileOwner>) -> Result<File> {
        single_name(name)?;
        let flags = OFlags::RDWR | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mode = Mode::RUSR | Mode::WUSR;
        let fd = match rustix::fs::openat(&self.0, name, flags | OFlags::CREATE | OFlags::EXCL, mode) {
            Err(rustix::io::Errno::EXIST) => rustix::fs::openat(&self.0, name, flags, mode),
            other => other,
        }
        .map_err(|error| Error::from(error).context(format!("openat {name}")))?;
        if FileType::from_raw_mode(rustix::fs::fstat(&fd)?.st_mode) != FileType::RegularFile {
            return Err(err("lifecycle state is not a regular file"));
        }
        rustix::fs::fchmod(&fd, mode)?;
        chown(fd.as_fd(), owner)?;
        Ok(File::from(fd))
    }

    pub fn names(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in Dir::read_from(&self.0)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name != "." && name != ".." {
                names.push(name);
            }
        }
        Ok(names)
    }

    pub fn remove_file(&self, name: &str) -> Result<()> {
        single_name(name)?;
        Ok(rustix::fs::unlinkat(&self.0, name, AtFlags::empty())?)
    }

    pub fn remove_dir(&self, name: &str) -> Result<()> {
        single_name(name)?;
        Ok(rustix::fs::unlinkat(&self.0, name, AtFlags::REMOVEDIR)?)
    }

    /// Removes `name` and everything below it without following links.
    pub fn remove_tree(&self, name: &str) -> Result<()> {
        single_name(name)?;
        match self.child(name) {
            Ok(directory) => {
                for entry in directory.names()? {
                    directory.remove_tree(&entry)?;
                }
                self.remove_dir(name)
            }
            Err(error) if error.kind == Kind::NotFound => Ok(()),
            // Not a directory, or a link: unlink the entry itself.
            Err(_) => self.remove_file(name),
        }
    }
}

/// Reads at most `limit` bytes from the end of a regular file, with its modification time.
pub fn read_tail(path: &Path, limit: u64) -> Result<(Vec<u8>, std::time::SystemTime)> {
    use std::io::{Seek, SeekFrom};
    let fd = rustix::fs::open(path, OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?;
    let mut file = File::from(fd);
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(err("log is not a regular file"));
    }
    file.seek(SeekFrom::Start(metadata.len().saturating_sub(limit)))?;
    let mut data = Vec::new();
    file.take(limit).read_to_end(&mut data)?;
    Ok((data, metadata.modified()?))
}

/// Replaces a file below a caller-trusted directory, as the service definition writer needs.
pub fn replace_file(path: &Path, data: &[u8], mode: u32) -> Result<()> {
    let parent = path.parent().ok_or_else(|| err("file has no parent directory"))?;
    let name = path.file_name().and_then(|name| name.to_str()).ok_or_else(|| err("file name is invalid"))?;
    StateDir::open(parent, None)?.write(name, data, mode, None)
}
