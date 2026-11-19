//! Thin wrappers over the Linux system calls that `std` does not expose.

use std::ffi::CStr;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::time::Instant;

use vmm_guest_proto::rpc::TtySize;

pub const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
pub const RESOLVE_NO_SYMLINKS: u64 = 0x04;
pub const RESOLVE_BENEATH: u64 = 0x08;
pub const RESOLVE_IN_ROOT: u64 = 0x10;

const TIOCSCTTY: u32 = 0x540e;
const TIOCSWINSZ: u32 = 0x5414;
const TIOCGPTPEER: u32 = 0x5441;
const TIOCSPTLCK: u32 = 0x4004_5431;
const RENAME_NOREPLACE: libc::c_uint = 1;

pub fn cvt(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

pub fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    // SAFETY: `fds` is a valid array of two descriptors for the kernel to fill.
    cvt(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) })?;
    // SAFETY: `pipe2` returned two new descriptors that nothing else owns.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

pub fn set_nonblocking(fd: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: `fcntl` with these commands reads and writes only descriptor flags.
    let flags = cvt(unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) })?;
    // SAFETY: as above.
    cvt(unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
    Ok(())
}

/// Signals a process, or its process group when `group` is set.
pub fn kill(pid: u32, signal: libc::c_int, group: bool) -> io::Result<()> {
    let target = if group {
        -(pid as libc::pid_t)
    } else {
        pid as libc::pid_t
    };
    // SAFETY: `kill` takes no pointers.
    cvt(unsafe { libc::kill(target, signal) }).map(drop)
}

fn poll(fds: &mut [libc::pollfd], deadline: Option<Instant>) -> io::Result<usize> {
    loop {
        let timeout = match deadline {
            None => -1,
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(i32::MAX as u128) as libc::c_int,
        };
        // SAFETY: `fds` is a valid slice of `pollfd` for the duration of the call.
        match cvt(unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) }) {
            Ok(ready) => return Ok(ready as usize),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn pollfd(fd: BorrowedFd<'_>, events: libc::c_short) -> libc::pollfd {
    libc::pollfd {
        fd: fd.as_raw_fd(),
        events,
        revents: 0,
    }
}

/// Copies `source` to `sink` until its end. When `stop` closes, copies what is
/// already buffered and returns, so a descendant that keeps the source open
/// cannot hold the caller.
pub fn pump(mut source: File, stop: BorrowedFd<'_>, mut sink: impl FnMut(&[u8])) {
    if set_nonblocking(source.as_fd()).is_err() {
        return;
    }
    let mut buffer = vec![0u8; 64 * 1024];
    let mut stopping = false;
    loop {
        match source.read(&mut buffer) {
            Ok(0) => return,
            Ok(count) => {
                sink(&buffer[..count]);
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            // A terminal reports EIO after its last writer closes.
            Err(_) => return,
        }
        if stopping {
            return;
        }
        let mut fds = [
            pollfd(source.as_fd(), libc::POLLIN),
            pollfd(stop, libc::POLLIN),
        ];
        if poll(&mut fds, None).is_err() {
            return;
        }
        stopping = fds[1].revents != 0;
    }
}

/// Writes all of `data` to a non-blocking descriptor. Stops with `TimedOut` at
/// `deadline`, or with `ConnectionAborted` when the peer of `watch` hangs up.
pub fn write_all_watched(
    mut sink: &File,
    data: &[u8],
    deadline: Option<Instant>,
    watch: Option<BorrowedFd<'_>>,
) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        match sink.write(&data[written..]) {
            Ok(count) => written += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let mut fds = [
                    pollfd(sink.as_fd(), libc::POLLOUT),
                    libc::pollfd {
                        fd: -1,
                        events: 0,
                        revents: 0,
                    },
                ];
                if let Some(watch) = watch {
                    fds[1] = pollfd(watch, libc::POLLRDHUP);
                }
                if poll(&mut fds, deadline)? == 0 {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                if fds[1].revents != 0 {
                    return Err(io::ErrorKind::ConnectionAborted.into());
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

/// `openat2(2)` with `O_CLOEXEC`. `mode` must be zero unless `flags` creates a file.
pub fn openat2(
    dir: BorrowedFd<'_>,
    path: &CStr,
    flags: libc::c_int,
    mode: u32,
    resolve: u64,
) -> io::Result<OwnedFd> {
    let how = OpenHow {
        flags: (flags | libc::O_CLOEXEC) as u64,
        mode: mode.into(),
        resolve,
    };
    // The kernel reports EAGAIN when a concurrent rename makes the resolution unsafe.
    for _ in 0..16 {
        // SAFETY: `path` is NUL-terminated and `how` is a valid `open_how` of the given size.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                dir.as_raw_fd(),
                path.as_ptr(),
                &how as *const OpenHow,
                size_of::<OpenHow>(),
            )
        };
        if fd >= 0 {
            // SAFETY: the kernel returned a new descriptor that nothing else owns.
            return Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) });
        }
        let error = io::Error::last_os_error();
        if !matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
            return Err(error);
        }
    }
    Err(io::Error::from_raw_os_error(libc::EAGAIN))
}

pub fn mkdirat(dir: BorrowedFd<'_>, name: &CStr, mode: u32) -> io::Result<()> {
    // SAFETY: `name` is NUL-terminated.
    cvt(unsafe { libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), mode) }).map(drop)
}

pub fn unlinkat(dir: BorrowedFd<'_>, name: &CStr, directory: bool) -> io::Result<()> {
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    // SAFETY: `name` is NUL-terminated.
    cvt(unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), flags) }).map(drop)
}

/// Renames within one directory. With `replace` unset an existing target fails with `EEXIST`.
pub fn rename_in(dir: BorrowedFd<'_>, from: &CStr, to: &CStr, replace: bool) -> io::Result<()> {
    let flags = if replace { 0 } else { RENAME_NOREPLACE };
    let fd = dir.as_raw_fd();
    // SAFETY: both names are NUL-terminated.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            fd,
            from.as_ptr(),
            fd,
            to.as_ptr(),
            flags,
        )
    };
    cvt(result as libc::c_int).map(drop)
}

/// Takes an exclusive advisory lock. Fails with `WouldBlock` when another descriptor holds it.
pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
    // SAFETY: `flock` takes no pointers.
    cvt(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }).map(drop)
}

/// Bytes in use on the filesystem of `file`.
// The widths of the `statvfs` fields differ between targets.
#[allow(clippy::unnecessary_cast)]
pub fn used_bytes(file: &File) -> io::Result<u64> {
    // SAFETY: `stat` is a valid buffer for the kernel to fill.
    let stat = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        cvt(libc::fstatvfs(file.as_raw_fd(), &mut stat))?;
        stat
    };
    Ok((stat.f_blocks as u64).saturating_sub(stat.f_bfree as u64) * stat.f_frsize as u64)
}

pub struct Pty {
    pub master: File,
    pub slave: OwnedFd,
}

pub fn open_pty(size: TtySize) -> io::Result<Pty> {
    let master = File::options().read(true).write(true).open("/dev/ptmx")?;
    let unlock: libc::c_int = 0;
    // SAFETY: `TIOCSPTLCK` reads one `int` through the pointer.
    cvt(unsafe {
        libc::ioctl(
            master.as_raw_fd(),
            TIOCSPTLCK as _,
            &unlock as *const libc::c_int,
        )
    })?;
    let flags = libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC;
    // SAFETY: `TIOCGPTPEER` takes open flags by value and returns a new descriptor.
    let slave = cvt(unsafe { libc::ioctl(master.as_raw_fd(), TIOCGPTPEER as _, flags) })?;
    // SAFETY: the kernel returned a new descriptor that nothing else owns.
    let slave = unsafe { OwnedFd::from_raw_fd(slave) };
    set_window_size(master.as_fd(), size)?;
    Ok(Pty { master, slave })
}

pub fn set_window_size(terminal: BorrowedFd<'_>, size: TtySize) -> io::Result<()> {
    let size = libc::winsize {
        ws_row: size.rows,
        ws_col: size.columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: `TIOCSWINSZ` reads one `winsize` through the pointer.
    cvt(unsafe {
        libc::ioctl(
            terminal.as_raw_fd(),
            TIOCSWINSZ as _,
            &size as *const libc::winsize,
        )
    })
    .map(drop)
}

/// Makes descriptor 0 the controlling terminal of the calling session leader.
/// Async-signal-safe: a child can call it between `fork` and `exec`.
pub fn set_controlling_terminal() -> io::Result<()> {
    // SAFETY: `TIOCSCTTY` takes an integer argument.
    cvt(unsafe { libc::ioctl(0, TIOCSCTTY as _, 0) }).map(drop)
}
