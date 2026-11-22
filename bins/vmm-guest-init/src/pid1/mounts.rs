//! Assembles the workload root from the disks of the boot configuration.

use std::ffi::CString;
use std::fs::{self, File, Permissions};
use std::io::{self, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::{PermissionsExt, chroot, fchown};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use vmm_guest_proto::boot::{BootConfig, EncryptedDisk};

use super::dm::{CryptDisk, open_crypt};
use crate::sys::{self, cvt};

const STAGE: &str = "/run/vmm";
const MKFS: &str = "/sbin/mkfs.ext4";
/// Options that let a writable layer outlive a replaced image.
const OVERLAY_OPTIONS: &str =
    "index=off,metacopy=off,xino=off,redirect_dir=nofollow,nfs_export=off";

/// What `PowerOff` needs after the root changed.
pub struct Assembled {
    /// Mount paths in mount order, as the workload sees them.
    pub mounted: Vec<PathBuf>,
    /// Device-mapper names of the volumes, free again after the unmount.
    pub mappings: Vec<String>,
    /// Writable ext4 filesystems on devices that accept discards.
    pub trimmable: Vec<(String, File)>,
}

fn c_path(path: &Path) -> io::Result<CString> {
    Ok(CString::new(path.as_os_str().as_encoded_bytes())?)
}

pub fn mount(
    source: &str,
    target: &Path,
    kind: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> io::Result<()> {
    let source = CString::new(source)?;
    let target = c_path(target)?;
    let kind = kind.map(CString::new).transpose()?;
    let data = data.map(CString::new).transpose()?;
    // SAFETY: each pointer is null or a NUL-terminated string that outlives the call.
    cvt(unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            kind.as_ref().map_or(std::ptr::null(), |kind| kind.as_ptr()),
            flags,
            data.as_ref()
                .map_or(std::ptr::null(), |data| data.as_ptr().cast()),
        )
    })
    .map(drop)
}

pub fn unmount(target: &Path, flags: libc::c_int) -> io::Result<()> {
    let target = c_path(target)?;
    // SAFETY: `target` is a NUL-terminated string.
    cvt(unsafe { libc::umount2(target.as_ptr(), flags) }).map(drop)
}

fn step<T>(what: impl std::fmt::Display, result: io::Result<T>) -> Result<T, String> {
    result.map_err(|error| format!("{what}: {error}"))
}

/// Mounts the kernel filesystems in the initramfs. They move to the workload root later.
pub fn kernel_filesystems() -> Result<(), String> {
    let nosuid = libc::MS_NOSUID | libc::MS_NOEXEC;
    let mounts = [
        ("/proc", "proc", nosuid | libc::MS_NODEV, None),
        ("/sys", "sysfs", nosuid | libc::MS_NODEV, None),
        ("/dev", "devtmpfs", libc::MS_NOSUID, Some("mode=0755")),
        (
            "/dev/pts",
            "devpts",
            nosuid,
            Some("gid=5,mode=620,ptmxmode=666"),
        ),
        (
            "/dev/shm",
            "tmpfs",
            libc::MS_NOSUID | libc::MS_NODEV,
            Some("mode=1777,size=512m"),
        ),
        ("/sys/fs/cgroup", "cgroup2", nosuid | libc::MS_NODEV, None),
    ];
    for (target, kind, flags, data) in mounts {
        step(format_args!("create {target}"), fs::create_dir_all(target))?;
        step(
            format_args!("mount {target}"),
            mount(kind, Path::new(target), Some(kind), flags, data),
        )?;
    }
    Ok(())
}

/// Opens the directory `path` inside `root` and creates its missing parts.
/// Symbolic links of the image resolve inside `root`.
pub(super) fn directory_in(root: &File, path: &str) -> io::Result<File> {
    let flags = libc::O_PATH | libc::O_DIRECTORY;
    let mut current = root.try_clone()?;
    let mut prefix = String::new();
    for part in path.split('/').filter(|part| !part.is_empty()) {
        prefix = format!("{prefix}/{part}");
        let relative = CString::new(&prefix[1..])?;
        let open = || sys::openat2(root.as_fd(), &relative, flags, 0, sys::RESOLVE_IN_ROOT);
        current = match open() {
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                sys::mkdirat(current.as_fd(), &CString::new(part)?, 0o755)?;
                open()?
            }
            opened => opened?,
        }
        .into();
    }
    Ok(current)
}

pub(super) fn descriptor_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

pub(super) fn format_ext4(device: &Path, journal: bool) -> Result<(), String> {
    let mut command = Command::new(MKFS);
    command.args(["-q", "-F"]);
    if journal {
        command.args(["-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1"]);
    } else {
        command.args(["-O", "^has_journal", "-E", "nodiscard,lazy_itable_init=1"]);
    }
    match step(format_args!("run {MKFS}"), command.arg(device).status())? {
        status if status.success() => Ok(()),
        status => Err(format!("{MKFS} {}: {status}", device.display())),
    }
}

/// Opens an encrypted disk, formats it when dom0 marked it empty, and mounts it.
///
/// The host attaches a read-only disk as a read-only block device, so nothing
/// can replay its journal here. dom0 unmounts the filesystem cleanly before.
pub(super) fn mount_encrypted(
    name: &str,
    disk: &EncryptedDisk,
    target: &Path,
    read_only: bool,
) -> Result<(), String> {
    let crypt = CryptDisk {
        name,
        device: &disk.device,
        key: &disk.key,
        allow_discards: disk.allow_discards && !read_only,
        read_only,
    };
    let device = step(
        format_args!("open the encrypted disk {}", disk.device),
        open_crypt(&crypt),
    )?;
    if disk.format {
        format_ext4(&device, true)?;
    }
    let flags = libc::MS_NOATIME | libc::MS_NODEV;
    let (flags, options) = if read_only {
        (flags | libc::MS_RDONLY, Some("noload"))
    } else {
        (flags, None)
    };
    let source = device.to_string_lossy();
    step(
        format_args!("mount the disk {}", disk.device),
        mount(&source, target, Some("ext4"), flags, options),
    )
}

fn create_stage(name: &str) -> Result<PathBuf, String> {
    let path = Path::new(STAGE).join(name);
    step(
        format_args!("create {}", path.display()),
        fs::create_dir_all(&path),
    )?;
    Ok(path)
}

/// Builds the workload root at the returned path and mounts everything below it.
pub fn assemble(config: &BootConfig) -> Result<(PathBuf, Assembled), String> {
    let mut assembled = Assembled {
        mounted: Vec::new(),
        mappings: Vec::new(),
        trimmable: Vec::new(),
    };
    let (lower, layer, root) = (
        create_stage("lower")?,
        create_stage("layer")?,
        create_stage("root")?,
    );

    // The device can appear shortly after the kernel starts PID 1.
    let image = format!("/dev/{}", config.root.device);
    let mut attempts = 0;
    while let Err(error) = mount(
        &image,
        &lower,
        Some("erofs"),
        libc::MS_RDONLY | libc::MS_NODEV,
        None,
    ) {
        attempts += 1;
        if attempts == 50 {
            return Err(format!("mount the image {image}: {error}"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let scratch = match &config.scratch {
        None => None,
        Some(scratch) => {
            let path = create_stage("scratch")?;
            let mut device = PathBuf::from(format!("/dev/{}", scratch.device));
            if let Some(key) = &scratch.key {
                let crypt = CryptDisk {
                    name: "vmm-scratch",
                    device: &scratch.device,
                    key,
                    allow_discards: false,
                    read_only: false,
                };
                device = step("open the scratch disk", open_crypt(&crypt))?;
            }
            // Nothing on this disk outlives the VM, so it needs no journal.
            format_ext4(&device, false)?;
            let flags = libc::MS_NOATIME | libc::MS_NODEV;
            step(
                "mount the scratch disk",
                mount(&device.to_string_lossy(), &path, Some("ext4"), flags, None),
            )?;
            Some(path)
        }
    };

    // The root is always an overlay, so PID 1 can create the mount points that
    // the image lacks. A read-only root keeps that layer on volatile storage.
    match (&config.root.overlay, &scratch) {
        (Some(disk), _) => {
            mount_encrypted("vmm-root", disk, &layer, false)?;
            if disk.allow_discards {
                let handle = step("open the root layer", File::open(&layer))?;
                assembled.trimmable.push(("/".into(), handle));
            }
        }
        (None, Some(scratch)) => {
            step(
                "bind the root layer",
                mount(
                    &scratch.to_string_lossy(),
                    &layer,
                    None,
                    libc::MS_BIND,
                    None,
                ),
            )?;
        }
        (None, None) => step(
            "mount the root layer",
            mount(
                "tmpfs",
                &layer,
                Some("tmpfs"),
                0,
                Some("mode=0755,size=2048m"),
            ),
        )?,
    }
    for name in ["upper", "work"] {
        step(
            "create the root layer",
            fs::create_dir_all(layer.join(name)),
        )?;
    }
    let options = format!(
        "lowerdir={},upperdir={},workdir={},{OVERLAY_OPTIONS}",
        lower.display(),
        layer.join("upper").display(),
        layer.join("work").display(),
    );
    step(
        "mount the root overlay",
        mount("overlay", &root, Some("overlay"), 0, Some(&options)),
    )?;
    let root_dir = step("open the root", File::open(&root))?;
    volatile_directories(&root_dir, scratch.as_deref())?;

    for (index, volume) in config.volumes.iter().enumerate() {
        let what = format!("volume {:?}", volume.name);
        let target = step(&what, directory_in(&root_dir, &volume.mount_path))?;
        let name = format!("vmm-volume-{index}");
        mount_encrypted(
            &name,
            &volume.disk,
            &descriptor_path(&target),
            volume.read_only,
        )?;
        // The earlier descriptor names the directory under the new mount.
        let mounted = step(&what, directory_in(&root_dir, &volume.mount_path))?;
        let mounted = step(&what, File::open(descriptor_path(&mounted)))?;
        if volume.disk.format {
            // PID 1 is root here, so an empty volume gets any numeric owner.
            step(
                &what,
                fchown(&mounted, Some(volume.owner_uid), Some(volume.owner_gid)),
            )?;
            step(
                &what,
                mounted.set_permissions(Permissions::from_mode(volume.mode)),
            )?;
        }
        assembled.mounted.push(volume.mount_path.clone().into());
        assembled.mappings.push(name);
        if !volume.read_only && volume.disk.allow_discards {
            assembled
                .trimmable
                .push((volume.mount_path.clone(), mounted));
        }
    }

    for share in &config.host_shares {
        let what = format!("host share {:?}", share.name);
        let target = step(&what, directory_in(&root_dir, &share.mount_path))?;
        let options = format!(
            "trans=tcp,port={},version=9p2000.L,aname={},msize=1048576,cache=none",
            share.port, share.name
        );
        let mut flags = libc::MS_NODEV | libc::MS_NOSUID;
        if share.read_only {
            flags |= libc::MS_RDONLY;
        }
        let source = config.network.gateway.to_string();
        step(
            &what,
            mount(
                &source,
                &descriptor_path(&target),
                Some("9p"),
                flags,
                Some(&options),
            ),
        )?;
        assembled.mounted.push(share.mount_path.clone().into());
    }

    finish_root(config, &root_dir)?;
    if config.root.read_only_root {
        let flags = libc::MS_REMOUNT | libc::MS_RDONLY;
        step(
            "make the root read-only",
            mount("overlay", &root, None, flags, None),
        )?;
    }
    Ok((root, assembled))
}

/// Mounts `/run` and `/tmp`. They come before the volumes, so a volume can
/// mount below them.
fn volatile_directories(root: &File, scratch: Option<&Path>) -> Result<(), String> {
    let flags = libc::MS_NOSUID | libc::MS_NODEV;
    let run = step("create /run", directory_in(root, "/run"))?;
    let run = descriptor_path(&run);
    step(
        "mount /run",
        mount(
            "tmpfs",
            &run,
            Some("tmpfs"),
            flags,
            Some("mode=0755,size=256m"),
        ),
    )?;

    let tmp = step("create /tmp", directory_in(root, "/tmp"))?;
    let tmp = descriptor_path(&tmp);
    match scratch {
        Some(scratch) => {
            let backing = scratch.join("tmp");
            step("create the scratch /tmp", fs::create_dir_all(&backing))?;
            let mode = Permissions::from_mode(0o1777);
            step("set the mode of /tmp", fs::set_permissions(&backing, mode))?;
            let source = backing.to_string_lossy();
            step(
                "mount /tmp",
                mount(&source, &tmp, None, libc::MS_BIND, None),
            )
        }
        None => step(
            "mount /tmp",
            mount(
                "tmpfs",
                &tmp,
                Some("tmpfs"),
                flags,
                Some("mode=1777,size=2048m"),
            ),
        ),
    }
}

/// Adds the directories of the workload, the resolver file, and the kernel filesystems.
fn finish_root(config: &BootConfig, root: &File) -> Result<(), String> {
    // A read-only root cannot get these directories after this point.
    for path in [&config.workload.workdir, &config.workspace_path]
        .into_iter()
        .flatten()
    {
        step(format_args!("create {path}"), directory_in(root, path))?;
    }

    if !config.network.dns.is_empty() {
        let etc = step("create /etc", directory_in(root, "/etc"))?;
        let name = c"resolv.conf";
        // An image can ship this name as a link to a resolver that is absent here.
        let _ = sys::unlinkat(etc.as_fd(), name, false);
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW;
        let file = sys::openat2(etc.as_fd(), name, flags, 0o644, sys::RESOLVE_IN_ROOT);
        let servers: String = config
            .network
            .dns
            .iter()
            .map(|server| format!("nameserver {server}\n"))
            .collect();
        let written = file.and_then(|file| File::from(file).write_all(servers.as_bytes()));
        step("write /etc/resolv.conf", written)?;
    }

    // `/proc` moves last: the descriptor paths above resolve through it.
    for path in ["/dev", "/sys", "/proc"] {
        let target = step(format_args!("create {path}"), directory_in(root, path))?;
        let moved = mount(path, &descriptor_path(&target), None, libc::MS_MOVE, None);
        step(format_args!("move {path}"), moved)?;
    }
    Ok(())
}

/// Makes `root` the root filesystem of this process.
pub fn enter_root(root: &Path) -> Result<(), String> {
    step("enter the root", std::env::set_current_dir(root))?;
    // SAFETY: both arguments are NUL-terminated strings.
    let pivoted = unsafe { libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr()) };
    match cvt(pivoted as libc::c_int) {
        // The old root is stacked on the new one. Detaching it leaves the new root.
        Ok(_) => step(
            "detach the initramfs",
            unmount(Path::new("."), libc::MNT_DETACH),
        )?,
        // The initramfs root cannot be pivoted. Move the new root over it instead.
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
            step(
                "move the root",
                mount(".", Path::new("/"), None, libc::MS_MOVE, None),
            )?;
        }
        Err(error) => return Err(format!("pivot the root: {error}")),
    }
    step("change the root", chroot("."))?;
    step("enter the root", std::env::set_current_dir("/"))
}
