//! Machine side of `PowerOff`, and the stop after a failed boot.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use vmm_guest_proto::rpc::{PowerOffReply, TrimReport};

use super::dm::remove;
use super::mounts::{Assembled, mount, unmount};
use crate::lock;
use crate::runtime::System;
use crate::sys::cvt;

/// `_IOWR('X', 121, struct fstrim_range)`.
const FITRIM: u32 = 0xc018_5879;

#[repr(C)]
struct TrimRange {
    start: u64,
    length: u64,
    minimum_length: u64,
}

/// Discards the free blocks of a mounted filesystem. Returns the discarded bytes.
pub(super) fn trim(filesystem: &File) -> std::io::Result<u64> {
    let mut range = TrimRange {
        start: 0,
        length: u64::MAX,
        minimum_length: 0,
    };
    // SAFETY: `FITRIM` reads and writes one `struct fstrim_range`.
    cvt(unsafe {
        libc::ioctl(
            filesystem.as_raw_fd(),
            FITRIM as _,
            &mut range as *mut TrimRange,
        )
    })?;
    Ok(range.length)
}

pub struct Machine {
    trimmable: Mutex<Vec<(String, File)>>,
    mounted: Vec<PathBuf>,
    mappings: Vec<String>,
}

impl Machine {
    pub fn new(assembled: Assembled) -> Self {
        Self {
            trimmable: Mutex::new(assembled.trimmable),
            mounted: assembled.mounted,
            mappings: assembled.mappings,
        }
    }
}

impl System for Machine {
    /// Returns the free blocks of each writable disk to the host. Then unmounts
    /// the volumes in reverse order, closes their mappings, and makes the root read-only.
    fn prepare_power_off(&self) -> PowerOffReply {
        // The descriptors close here, or the volumes stay busy.
        let trimmable = std::mem::take(&mut *lock(&self.trimmable));
        let trims = trimmable
            .into_iter()
            .map(|(mount_path, filesystem)| {
                let result = trim(&filesystem);
                TrimReport {
                    mount_path,
                    trimmed_bytes: result.as_ref().ok().copied(),
                    error: result.err().map(|error| error.to_string()),
                }
            })
            .collect();
        // SAFETY: `sync` takes no arguments.
        unsafe { libc::sync() };
        let mut clean_unmount = true;
        for path in self.mounted.iter().rev() {
            if unmount(path, 0).is_err() {
                clean_unmount = false;
                let _ = unmount(path, libc::MNT_DETACH);
            }
        }
        // A mapping whose volume is still busy stays. The power off ends it.
        for name in self.mappings.iter().rev() {
            clean_unmount &= remove(name).is_ok();
        }
        let flags = libc::MS_REMOUNT | libc::MS_RDONLY;
        clean_unmount &= mount("none", Path::new("/"), None, flags, None).is_ok();
        PowerOffReply {
            trims,
            clean_unmount,
        }
    }

    fn power_off(&self) {
        power_off()
    }
}

pub fn power_off() -> ! {
    // SAFETY: neither call takes a pointer.
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_POWER_OFF);
    }
    // PID 1 must not return. The kernel stops the machine before this matters.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Reports a failed boot as the last console line, where dom0 reads it, and stops the machine.
pub fn fail(message: &str) -> ! {
    eprintln!("vmm-guest-init: FAILED: {}", message.replace('\n', " "));
    // Lets the console drain before the power goes.
    std::thread::sleep(Duration::from_secs(2));
    power_off()
}
