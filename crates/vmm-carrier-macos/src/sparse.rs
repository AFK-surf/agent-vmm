use rustix::fs::{OFlags, SeekFrom, seek, statvfs};
use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

pub const EMERGENCY_BYTES: u64 = 10 << 30;

pub fn open_pool(path: &Path, size: u64) -> io::Result<File> {
    if size == 0 || !size.is_multiple_of(4 << 20) || size > i64::MAX as u64 {
        return Err(io::Error::other(
            "pool size must be a positive multiple of 4 MiB",
        ));
    }
    ensure_emergency_headroom(
        path.parent()
            .ok_or_else(|| io::Error::other("pool directory"))?,
        0,
        EMERGENCY_BYTES,
    )?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(OFlags::NOFOLLOW.bits() as i32)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(io::Error::other("pool must be a regular file"));
    }
    if meta.len() < size {
        file.set_len(size)?;
        file.sync_all()?;
    }
    Ok(file)
}

pub fn allocated_bytes(file: &File) -> io::Result<u64> {
    Ok(file.metadata()?.blocks() * 512)
}

pub fn hole_bytes(file: &File, logical_size: u64) -> io::Result<u64> {
    let mut cursor = 0;
    let mut total = 0;
    while cursor < logical_size {
        let hole = seek(file, SeekFrom::Hole(cursor))?;
        if hole >= logical_size {
            break;
        }
        let data = match seek(file, SeekFrom::Data(hole)) {
            Ok(n) => n.min(logical_size),
            Err(rustix::io::Errno::NXIO) => logical_size,
            Err(e) => return Err(e.into()),
        };
        if hole < cursor || data <= hole {
            return Err(io::Error::other("invalid pool extent map"));
        }
        total += data - hole;
        cursor = data;
    }
    Ok(total)
}

pub fn host_free_bytes(path: &Path) -> io::Result<u64> {
    let stat = statvfs(path)?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

pub fn ensure_emergency_headroom(
    path: &Path,
    needed_bytes: u64,
    emergency_bytes: u64,
) -> io::Result<()> {
    if host_free_bytes(path)?
        .checked_sub(needed_bytes)
        .is_none_or(|left| left < emergency_bytes)
    {
        return Err(io::Error::new(
            io::ErrorKind::StorageFull,
            "insufficient host emergency space",
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn punch_hole(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let size = file.metadata()?.len();
    if len == 0
        || !offset.is_multiple_of(4096)
        || !len.is_multiple_of(4096)
        || offset.checked_add(len).is_none_or(|end| end > size)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid pool punch range",
        ));
    }
    let range = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset as i64,
        fp_length: len as i64,
    };
    // SAFETY: F_PUNCHHOLE reads the initialized fpunchhole_t during this call, with file open.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &range) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
