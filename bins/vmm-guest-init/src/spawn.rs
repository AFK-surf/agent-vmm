//! Builds the command of a workload process: environment, user, session, and cgroup.

use std::ffi::CString;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use vmm_guest_proto::boot::valid_env_entry;
use vmm_guest_proto::rpc::{CommandSpec, ErrorCode, RpcError};

use crate::{sys, users};

const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Defaults that every workload process starts from.
#[derive(Debug, Clone)]
pub struct SpawnContext {
    /// Directory of the `passwd` and `group` files.
    pub etc_dir: PathBuf,
    /// `cgroup.procs` of the workload cgroup. Each process joins it before `exec`.
    pub cgroup_procs: Option<CString>,
    pub base_env: Vec<String>,
    pub default_workdir: String,
    pub default_user: Option<String>,
    /// Retains `CAP_SETGID`, `CAP_SETUID`, and `CAP_SETPCAP` as ambient
    /// capabilities across non-root UID drops (build helper VMs only).
    pub grant_subid_caps: bool,
}

fn invalid(message: impl Into<String>) -> RpcError {
    RpcError::new(ErrorCode::InvalidArgument, message)
}

/// Returns a command whose child starts a new session, so a signal to its
/// process group reaches all of its descendants that stay in the group.
pub fn command(
    context: &SpawnContext,
    spec: &CommandSpec,
    controlling_terminal: bool,
) -> Result<Command, RpcError> {
    let Some((program, arguments)) = spec.argv.split_first() else {
        return Err(invalid("argv is empty"));
    };
    if program.is_empty() || spec.argv.iter().any(|argument| argument.contains('\0')) {
        return Err(invalid("argv"));
    }
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .env("PATH", DEFAULT_PATH);
    let mut home_set = false;
    for entry in context.base_env.iter().chain(&spec.env) {
        let Some((key, value)) = entry.split_once('=').filter(|_| valid_env_entry(entry)) else {
            return Err(invalid("env entries must be KEY=VALUE"));
        };
        home_set |= key == "HOME";
        command.env(key, value);
    }

    let workdir = spec.workdir.as_deref().unwrap_or(&context.default_workdir);
    if !workdir.starts_with('/') || workdir.contains('\0') || !Path::new(workdir).is_dir() {
        return Err(invalid(format!(
            "workdir {workdir:?} is not an existing absolute directory"
        )));
    }
    command.current_dir(workdir);

    let user = spec.user.as_deref().or(context.default_user.as_deref());
    let credentials = user
        .map(|user| users::resolve(&context.etc_dir, user))
        .transpose();
    let credentials = credentials.map_err(invalid)?;
    if let Some(home) = credentials
        .as_ref()
        .and_then(|credentials| credentials.home.as_ref())
        && !home_set
    {
        command.env("HOME", home);
    }
    // SAFETY: `geteuid` and `getegid` cannot fail and take no arguments.
    let current = unsafe { (libc::geteuid(), libc::getegid()) };
    let switch = credentials.filter(|credentials| (credentials.uid, credentials.gid) != current);
    let cgroup_procs = context.cgroup_procs.clone();
    #[cfg(target_os = "linux")]
    let grant_subid_caps = context.grant_subid_caps;

    // SAFETY: the closure runs between `fork` and `exec` and makes only
    // async-signal-safe calls on values that were allocated before the fork.
    unsafe {
        command.pre_exec(move || {
            sys::cvt(libc::setsid())?;
            if controlling_terminal {
                sys::set_controlling_terminal()?;
            }
            if let Some(path) = &cgroup_procs {
                let fd = sys::cvt(libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC))?;
                let written = libc::write(fd, b"0".as_ptr().cast(), 1);
                let error = std::io::Error::last_os_error();
                libc::close(fd);
                if written != 1 {
                    return Err(error);
                }
            }
            if let Some(credentials) = &switch {
                #[cfg(target_os = "linux")]
                if grant_subid_caps {
                    sys::cvt(libc::prctl(libc::PR_SET_KEEPCAPS, 1, 0, 0, 0))?;
                }
                sys::cvt(libc::setgroups(
                    credentials.groups.len(),
                    credentials.groups.as_ptr(),
                ))?;
                sys::cvt(libc::setgid(credentials.gid))?;
                sys::cvt(libc::setuid(credentials.uid))?;
                #[cfg(target_os = "linux")]
                if grant_subid_caps {
                    retain_subid_caps()?;
                }
            }
            Ok(())
        });
    }
    Ok(command)
}

#[cfg(target_os = "linux")]
unsafe fn retain_subid_caps() -> std::io::Result<()> {
    #[repr(C)]
    struct CapUserHeader {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    struct CapUserData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
    const SUBID_CAPS: [u32; 3] = [6, 7, 8]; // CAP_SETGID, CAP_SETUID, CAP_SETPCAP
    let mask = (1u32 << 6) | (1u32 << 7) | (1u32 << 8);
    let hdr = CapUserHeader {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let data = [
        CapUserData {
            effective: mask,
            permitted: mask,
            inheritable: mask,
        },
        CapUserData {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        },
    ];
    unsafe {
        sys::cvt(libc::syscall(libc::SYS_capset, &hdr, data.as_ptr()) as libc::c_int)?;
        for cap in SUBID_CAPS {
            sys::cvt(libc::prctl(
                libc::PR_CAP_AMBIENT,
                libc::PR_CAP_AMBIENT_RAISE,
                cap as libc::c_ulong,
                0,
                0,
            ))?;
        }
        sys::cvt(libc::prctl(libc::PR_SET_KEEPCAPS, 0, 0, 0, 0))?;
    }
    Ok(())
}
