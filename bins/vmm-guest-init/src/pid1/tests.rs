//! Tests of the boot steps that need a real kernel object.

use std::fs::{self, File};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::Command;

use vmm_guest_proto::Secret;
use vmm_guest_proto::boot::EncryptedDisk;

use super::dm::{self, CryptDisk};
use super::{format_swap_header, mounts, shutdown};

#[test]
fn swap_header_encodes_v1_layout_and_signature() {
    let header = format_swap_header(512 << 20);
    assert_eq!(header.len(), 4096);
    assert_eq!(
        u32::from_le_bytes(header[1024..1028].try_into().unwrap()),
        1
    );
    assert_eq!(
        u32::from_le_bytes(header[1028..1032].try_into().unwrap()),
        (512u32 << 8) - 1
    );
    assert_eq!(&header[4086..4096], b"SWAPSPACE2");
}

#[test]
fn directories_resolve_inside_the_root_also_through_absolute_links() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(root.join("data")).unwrap();
    symlink("/data", root.join("absolute")).unwrap();
    let root_dir = File::open(&root).unwrap();

    let made = mounts::directory_in(&root_dir, "/absolute/volume/deep").unwrap();
    let expected = fs::canonicalize(root.join("data/volume/deep")).unwrap();
    assert_eq!(
        fs::canonicalize(mounts::descriptor_path(&made)).unwrap(),
        expected
    );

    mounts::directory_in(&root_dir, "/../../outside").unwrap();
    assert!(root.join("outside").is_dir());
    assert!(!dir.path().join("outside").exists());
}

fn disk<'a>(name: &'a str, device: &'a str, key: &'a Secret) -> CryptDisk<'a> {
    CryptDisk {
        name,
        device,
        key,
        allow_discards: true,
        read_only: false,
    }
}

fn attach(image: &Path, read_only: bool) -> String {
    let mut command = Command::new("losetup");
    command.args(["--find", "--show"]);
    if read_only {
        command.arg("--read-only");
    }
    let attached = command.arg(image).output().unwrap();
    assert!(
        attached.status.success(),
        "{}",
        String::from_utf8_lossy(&attached.stderr)
    );
    String::from_utf8(attached.stdout)
        .unwrap()
        .trim()
        .to_owned()
}

fn detach(loop_device: &str) {
    let detached = Command::new("losetup").arg("-d").arg(loop_device).status();
    assert!(detached.unwrap().success());
}

struct Cleanup {
    target: PathBuf,
    mapping: String,
    loop_device: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = mounts::unmount(&self.target, libc::MNT_DETACH);
        let _ = dm::remove(&self.mapping);
        let _ = Command::new("losetup")
            .arg("-d")
            .arg(&self.loop_device)
            .status();
    }
}

/// Run as root in a private mount namespace:
/// `sudo unshare -m <lib test binary> --ignored`.
#[test]
#[ignore = "needs root, a loop device, and /sbin/mkfs.ext4"]
fn an_encrypted_disk_formats_mounts_trims_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let image = dir.path().join("disk.img");
    File::create(&image).unwrap().set_len(64 << 20).unwrap();
    let loop_device = attach(&image, false);
    let target = dir.path().join("mount");
    fs::create_dir(&target).unwrap();
    let mapping = format!("vmm-test-{}", std::process::id());
    let mut cleanup = Cleanup {
        target: target.clone(),
        mapping: mapping.clone(),
        loop_device: loop_device.clone(),
    };

    let key = Secret::new("3c".repeat(64));
    let device_name = loop_device.strip_prefix("/dev/").unwrap();
    let mount = |device: &PathBuf| {
        mounts::mount(
            &device.to_string_lossy(),
            &target,
            Some("ext4"),
            libc::MS_NOATIME,
            None,
        )
    };

    let device = dm::open_crypt(&disk(&mapping, device_name, &key)).unwrap();
    mounts::format_ext4(&device, true).unwrap();
    mount(&device).unwrap();
    fs::write(target.join("large.bin"), vec![7u8; 16 << 20]).unwrap();
    fs::remove_file(target.join("large.bin")).unwrap();
    fs::write(target.join("kept.txt"), "kept").unwrap();
    // The discard reaches the image file through dm-crypt and the loop device.
    let trimmed = shutdown::trim(&File::open(&target).unwrap()).unwrap();
    assert!(trimmed >= 16 << 20, "{trimmed}");
    mounts::unmount(&target, 0).unwrap();
    dm::remove(&mapping).unwrap();

    // The image holds ciphertext: its superblock location has no ext4 magic.
    let raw = fs::read(&image).unwrap();
    assert_ne!(raw[1080..1082], [0x53, 0xef]);

    let device = dm::open_crypt(&disk(&mapping, device_name, &key)).unwrap();
    mount(&device).unwrap();
    assert_eq!(fs::read(target.join("kept.txt")).unwrap(), b"kept");
    mounts::unmount(&target, 0).unwrap();
    dm::remove(&mapping).unwrap();

    // Another key shows no filesystem.
    let other = Secret::new("d2".repeat(64));
    let device = dm::open_crypt(&disk(&mapping, device_name, &other)).unwrap();
    assert!(mount(&device).is_err());
    dm::remove(&mapping).unwrap();

    // A read-only volume on a read-only block device: the mapping is
    // read-only too, and the filesystem mounts.
    detach(&loop_device);
    cleanup.loop_device = attach(&image, true);
    let device_name = cleanup.loop_device.strip_prefix("/dev/").unwrap();
    let volume = EncryptedDisk {
        device: device_name.to_owned(),
        key: key.clone(),
        allow_discards: true,
        format: false,
    };
    mounts::mount_encrypted(&mapping, &volume, &target, true).unwrap();
    let holders = format!("/sys/class/block/{device_name}/holders");
    let holder = fs::read_dir(holders).unwrap().next().unwrap().unwrap();
    assert_eq!(fs::read_to_string(holder.path().join("ro")).unwrap(), "1\n");
    assert_eq!(fs::read(target.join("kept.txt")).unwrap(), b"kept");
    assert!(fs::write(target.join("new.txt"), "new").is_err());
    mounts::unmount(&target, 0).unwrap();
    dm::remove(&mapping).unwrap();
}
