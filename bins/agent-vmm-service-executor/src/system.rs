//! The operating system calls of the executor. A test replaces them all.

use crate::config::Owner;
use std::ffi::{CStr, CString};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct Stat {
    pub uid: u32,
    pub mode: u32,
    pub symlink: bool,
    pub regular: bool,
}

pub enum Launch {
    /// The standard output of a successful call.
    Done(String),
    /// The job or its domain is not loaded.
    NotFound,
    Failed(String),
}

pub trait System {
    fn euid(&self) -> u32;
    fn executable(&self) -> io::Result<PathBuf>;
    fn lstat(&self, path: &Path) -> io::Result<Stat>;
    fn read(&self, path: &Path) -> io::Result<String>;
    /// Replaces the file with complete contents.
    fn replace_file(&self, path: &Path, data: &[u8], mode: u32) -> io::Result<()>;
    /// Creates the directory when it is missing.
    fn create_dir(&self, path: &Path, mode: u32) -> io::Result<()>;
    /// Removes the file. A missing file is not an error.
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn account(&self, name: &str) -> Option<Owner>;
    fn launchctl(&self, arguments: &[&str]) -> Launch;
    fn process_running(&self, pid: i32) -> bool;
    /// The processes that hold `path` open.
    fn open_owners(&self, path: &Path) -> io::Result<Vec<i32>>;
    /// Creates the root-owned log directory that the service group can read.
    fn prepare_logs(&self, gid: u32) -> io::Result<()>;
    fn pause(&self);
}

pub struct Production;

const LOGS: &str = "/Library/Logs/AgentVMM";

impl System for Production {
    fn euid(&self) -> u32 {
        // SAFETY: geteuid has no arguments and cannot fail.
        unsafe { libc::geteuid() }
    }

    fn executable(&self) -> io::Result<PathBuf> {
        std::env::current_exe()
    }

    fn lstat(&self, path: &Path) -> io::Result<Stat> {
        let metadata = std::fs::symlink_metadata(path)?;
        Ok(Stat { uid: metadata.uid(), mode: metadata.mode() & 0o7777, symlink: metadata.file_type().is_symlink(), regular: metadata.is_file() })
    }

    fn read(&self, path: &Path) -> io::Result<String> {
        std::fs::read_to_string(path)
    }

    fn replace_file(&self, path: &Path, data: &[u8], mode: u32) -> io::Result<()> {
        use std::io::Write;
        let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
        let written = (|| {
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(mode).open(&temporary)?;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
            file.write_all(data)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)
        })();
        if written.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        written
    }

    fn create_dir(&self, path: &Path, mode: u32) -> io::Result<()> {
        match std::fs::create_dir(path) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => Err(error),
            Err(_) => Ok(()),
            Ok(()) => std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)),
        }
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    fn account(&self, name: &str) -> Option<Owner> {
        let name = CString::new(name).ok()?;
        // SAFETY: getpwnam and getgrgid return a pointer to static storage or null.
        // This program has one thread, and each field is copied before the next call.
        unsafe {
            let user = libc::getpwnam(name.as_ptr());
            if user.is_null() {
                return None;
            }
            let (uid, gid) = ((*user).pw_uid, (*user).pw_gid);
            let user_name = CStr::from_ptr((*user).pw_name).to_str().ok()?.to_string();
            let home = PathBuf::from(CStr::from_ptr((*user).pw_dir).to_str().ok()?);
            let group = libc::getgrgid(gid);
            let group = if group.is_null() { String::new() } else { CStr::from_ptr((*group).gr_name).to_str().ok()?.to_string() };
            Some(Owner { user: user_name, group, uid, gid, home })
        }
    }

    fn launchctl(&self, arguments: &[&str]) -> Launch {
        let output = match Command::new("/bin/launchctl").args(arguments).output() {
            Ok(output) => output,
            Err(error) => return Launch::Failed(error.to_string()),
        };
        if output.status.success() {
            return Launch::Done(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        let message = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        let absent = ["Could not find service", "Could not find domain", "Domain does not support specified action"].iter().any(|text| message.contains(text));
        if absent || arguments[0] == "bootout" && message.contains("No such process") { Launch::NotFound } else { Launch::Failed(message.trim().to_string()) }
    }

    fn process_running(&self, pid: i32) -> bool {
        // SAFETY: signal 0 only checks that the process exists.
        unsafe { libc::kill(pid, 0) == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM) }
    }

    fn open_owners(&self, path: &Path) -> io::Result<Vec<i32>> {
        if std::fs::symlink_metadata(path).is_err() {
            return Ok(Vec::new());
        }
        let output = Command::new("/usr/sbin/lsof").arg("-Fp").arg("--").arg(path).output()?;
        // lsof exits with 1 and prints nothing when no process holds the file.
        if !output.stderr.is_empty() || !matches!(output.status.code(), Some(0 | 1)) {
            return Err(io::Error::other("open-file inspection failed"));
        }
        Ok(String::from_utf8_lossy(&output.stdout).lines().filter_map(|line| line.strip_prefix('p')?.parse().ok()).collect())
    }

    fn prepare_logs(&self, gid: u32) -> io::Result<()> {
        self.create_dir(Path::new(LOGS), 0o750)?;
        let metadata = std::fs::symlink_metadata(LOGS)?;
        if metadata.uid() != 0 || !metadata.is_dir() {
            return Err(io::Error::other("daemon log directory must be a directory owned by root"));
        }
        std::os::unix::fs::chown(LOGS, Some(0), Some(gid))?;
        std::fs::set_permissions(LOGS, std::fs::Permissions::from_mode(0o750))
    }

    fn pause(&self) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
