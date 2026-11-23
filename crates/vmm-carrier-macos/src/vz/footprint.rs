//! Physical memory that macOS charges for a process.
//!
//! Virtualization.framework runs the VM in an XPC service. `launchd` starts the
//! service, so the service is not a child of the Carrier. The service holds the
//! disk image of the VM open, and that identifies it.
use std::{
    io,
    mem::{MaybeUninit, size_of},
    os::unix::fs::MetadataExt,
    path::Path,
    ptr,
};

/// The executable of the XPC service of Virtualization.framework.
pub(super) const VM_SERVICE: &str = "/com.apple.Virtualization.VirtualMachine";

// These definitions of `sys/proc_info.h` are not in the `libc` crate.
const PROC_PIDFDVNODEPATHINFO: libc::c_int = 2;
#[repr(C)]
struct ProcFileInfo {
    fi_openflags: u32,
    fi_status: u32,
    fi_offset: libc::off_t,
    fi_type: i32,
    fi_guardflags: u32,
}
#[repr(C)]
struct VnodeFdInfoWithPath {
    pfi: ProcFileInfo,
    pvip: libc::vnode_info_path,
}

/// `phys_footprint` of one process. `None` when the process is gone or belongs to another user.
pub(super) fn of(pid: libc::pid_t) -> Option<u64> {
    let mut usage = MaybeUninit::<libc::rusage_info_v4>::zeroed();
    // SAFETY: the buffer has the size of the requested flavor.
    let failed =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V4, usage.as_mut_ptr().cast()) };
    // SAFETY: the buffer is zero-initialized, and each field is an integer.
    (failed == 0).then(|| unsafe { usage.assume_init() }.ri_phys_footprint)
}

/// The sum for `pids`. `None` when one of them does not answer.
pub(super) fn of_all(pids: &[libc::pid_t]) -> Option<u64> {
    pids.iter().map(|&pid| of(pid)).sum()
}

/// The other processes that hold `file` open and whose executable path ends with `executable`.
pub(super) fn holders(file: &Path, executable: &str) -> io::Result<Vec<libc::pid_t>> {
    let file = std::fs::metadata(file)?;
    // SAFETY: a null buffer asks for the number of processes.
    let count = unsafe { libc::proc_listallpids(ptr::null_mut(), 0) };
    // Processes can start between the two calls.
    let mut pids = vec![0 as libc::pid_t; usize::try_from(count).unwrap_or(0) + 64];
    // SAFETY: the size is the size of the buffer in bytes.
    let count = unsafe {
        libc::proc_listallpids(
            pids.as_mut_ptr().cast(),
            (pids.len() * size_of::<libc::pid_t>()) as libc::c_int,
        )
    };
    pids.truncate(usize::try_from(count).map_err(|_| io::Error::last_os_error())?);
    let own = std::process::id() as libc::pid_t;
    pids.retain(|&pid| pid != own && runs(pid, executable) && holds(pid, &file));
    Ok(pids)
}

fn runs(pid: libc::pid_t, executable: &str) -> bool {
    let mut path = [0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: the size is the size of the buffer.
    let len = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
    usize::try_from(len).is_ok_and(|len| path[..len].ends_with(executable.as_bytes()))
}

fn holds(pid: libc::pid_t, file: &std::fs::Metadata) -> bool {
    let entry = size_of::<libc::proc_fdinfo>();
    // SAFETY: a null buffer asks for the size of the descriptor list.
    let size = unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, ptr::null_mut(), 0) };
    let Ok(size) = usize::try_from(size) else {
        return false;
    };
    // SAFETY: `proc_fdinfo` contains only integers.
    let mut fds = vec![unsafe { std::mem::zeroed::<libc::proc_fdinfo>() }; size / entry + 16];
    // SAFETY: the size is the size of the buffer in bytes.
    let size = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDLISTFDS,
            0,
            fds.as_mut_ptr().cast(),
            (fds.len() * entry) as libc::c_int,
        )
    };
    fds.truncate(usize::try_from(size).unwrap_or(0) / entry);
    fds.iter()
        .filter(|fd| fd.proc_fdtype == libc::PROX_FDTYPE_VNODE as u32)
        .any(|fd| {
            let mut info = MaybeUninit::<VnodeFdInfoWithPath>::zeroed();
            let size = size_of::<VnodeFdInfoWithPath>() as libc::c_int;
            // SAFETY: the buffer has the size of the requested flavor.
            let read = unsafe {
                libc::proc_pidfdinfo(
                    pid,
                    fd.proc_fd,
                    PROC_PIDFDVNODEPATHINFO,
                    info.as_mut_ptr().cast(),
                    size,
                )
            };
            // SAFETY: the buffer is zero-initialized, and each field is an integer or a byte array.
            let stat = unsafe { info.assume_init() }.pvip.vip_vi.vi_stat;
            read == size && u64::from(stat.vst_dev) == file.dev() && stat.vst_ino == file.ino()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn finds_the_named_process_that_holds_a_file_and_reads_its_footprint() {
        let dir = tempfile::tempdir().unwrap();
        let held = dir.path().join("held");
        let other = dir.path().join("other");
        std::fs::write(&held, []).unwrap();
        std::fs::write(&other, []).unwrap();
        let mut child = Command::new("/bin/sleep")
            .arg("60")
            .stdin(std::fs::File::open(&held).unwrap())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id() as libc::pid_t;
        let found = holders(&held, "/sleep").unwrap();
        let wrong_name = holders(&held, "/not-sleep").unwrap();
        let wrong_file = holders(&other, "/sleep").unwrap();
        let footprint = of(pid);
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(found, [pid]);
        assert!(wrong_name.is_empty() && wrong_file.is_empty());
        assert!(footprint.is_some_and(|bytes| bytes > 0));
        assert!(of(std::process::id() as libc::pid_t).is_some_and(|bytes| bytes > 0));
        assert_eq!(of(pid), None);
    }
}
