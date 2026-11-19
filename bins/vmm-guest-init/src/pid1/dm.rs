//! Opens plain dm-crypt devices through the device-mapper control `ioctl`.
//!
//! The initramfs holds no `dmsetup`. The request buffers follow
//! `include/uapi/linux/dm-ioctl.h`.

use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;

use vmm_guest_proto::Secret;
use zeroize::Zeroizing;

use crate::sys::cvt;

const HEADER_BYTES: usize = 312;
const TARGET_SPEC_BYTES: usize = 40;
const NAME_OFFSET: usize = 48;
const NAME_BYTES: usize = 128;
const DM_DEV_CREATE: u32 = 3;
const DM_DEV_REMOVE: u32 = 4;
const DM_DEV_SUSPEND: u32 = 6;
const DM_TABLE_LOAD: u32 = 9;
/// Makes the kernel open the backing device of a table without write access.
const DM_READONLY_FLAG: u32 = 1 << 0;
/// Makes the kernel wipe its copy of the request, which holds the key.
const DM_SECURE_DATA_FLAG: u32 = 1 << 15;

/// `_IOWR(0xfd, command, struct dm_ioctl)`.
const fn request(command: u32) -> u32 {
    (3 << 30) | ((HEADER_BYTES as u32) << 16) | (0xfd << 8) | command
}

/// A `struct dm_ioctl` for the device `name`, with room for `payload` bytes.
fn header(name: &str, flags: u32, target_count: u32, payload: usize) -> Zeroizing<Vec<u8>> {
    assert!(
        name.len() < NAME_BYTES,
        "device-mapper names are short constants"
    );
    let mut buffer = Zeroizing::new(vec![0u8; HEADER_BYTES + payload]);
    let mut put = |offset: usize, value: u32| {
        buffer[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
    };
    put(0, 4); // version 4.0.0
    put(12, (HEADER_BYTES + payload) as u32); // data_size
    put(16, HEADER_BYTES as u32); // data_start
    put(20, target_count);
    put(28, flags);
    buffer[NAME_OFFSET..NAME_OFFSET + name.len()].copy_from_slice(name.as_bytes());
    buffer
}

/// A table-load request with one target that covers the whole device.
fn table_load(
    name: &str,
    flags: u32,
    sectors: u64,
    kind: &str,
    params: &str,
) -> Zeroizing<Vec<u8>> {
    // The parameter string is NUL-terminated and padded to eight bytes.
    let spec_bytes = (TARGET_SPEC_BYTES + params.len() + 1).next_multiple_of(8);
    let mut buffer = header(name, flags, 1, spec_bytes);
    let spec = &mut buffer[HEADER_BYTES..];
    spec[8..16].copy_from_slice(&sectors.to_ne_bytes()); // length; sector_start stays 0
    spec[20..24].copy_from_slice(&(spec_bytes as u32).to_ne_bytes()); // next
    spec[24..24 + kind.len()].copy_from_slice(kind.as_bytes()); // target_type
    spec[TARGET_SPEC_BYTES..TARGET_SPEC_BYTES + params.len()].copy_from_slice(params.as_bytes());
    buffer
}

fn crypt_params(key: &Secret, device: &str, allow_discards: bool) -> Zeroizing<String> {
    let options = if allow_discards {
        "2 allow_discards no_read_workqueue"
    } else {
        "1 no_read_workqueue"
    };
    let tail = format!(" 0 /dev/{device} 0 {options}");
    // The capacity is exact, so the key is never copied by a reallocation.
    let cipher = "aes-xts-plain64 ";
    let mut params = Zeroizing::new(String::with_capacity(
        cipher.len() + key.expose().len() + tail.len(),
    ));
    params.push_str(cipher);
    params.push_str(key.expose());
    params.push_str(&tail);
    params
}

/// Minor number of the device in the `dev` field of a reply.
fn minor(reply: &[u8]) -> u64 {
    let dev = u64::from_ne_bytes(
        reply[40..48]
            .try_into()
            .expect("the dev field is eight bytes"),
    );
    (dev & 0xff) | ((dev >> 12) & 0xfff00)
}

fn control(control: &File, command: u32, buffer: &mut [u8]) -> io::Result<()> {
    // SAFETY: `buffer` is a `struct dm_ioctl` whose `data_size` is its length.
    cvt(unsafe {
        libc::ioctl(
            control.as_raw_fd(),
            request(command) as _,
            buffer.as_mut_ptr(),
        )
    })
    .map(drop)
}

pub struct CryptDisk<'a> {
    /// Device-mapper name of the new device.
    pub name: &'a str,
    /// Backing device name, such as `vdb`.
    pub device: &'a str,
    pub key: &'a Secret,
    pub allow_discards: bool,
    /// Set for a backing device that is read-only.
    pub read_only: bool,
}

/// Creates the mapping and returns its device node.
pub fn open_crypt(disk: &CryptDisk<'_>) -> io::Result<PathBuf> {
    let sectors = fs::read_to_string(format!("/sys/class/block/{}/size", disk.device))?;
    let sectors: u64 = sectors.trim().parse().map_err(io::Error::other)?;
    let control_device = File::options()
        .read(true)
        .write(true)
        .open("/dev/mapper/control")?;

    let mut create = header(disk.name, 0, 0, 0);
    control(&control_device, DM_DEV_CREATE, &mut create)?;
    let params = crypt_params(disk.key, disk.device, disk.allow_discards);
    let mode = if disk.read_only { DM_READONLY_FLAG } else { 0 };
    let flags = DM_SECURE_DATA_FLAG | mode;
    let mut load = table_load(disk.name, flags, sectors, "crypt", &params);
    // A resume is a suspend request without the suspend flag.
    let activated = control(&control_device, DM_TABLE_LOAD, &mut load).and_then(|()| {
        control(
            &control_device,
            DM_DEV_SUSPEND,
            &mut header(disk.name, 0, 0, 0),
        )
    });
    if let Err(error) = activated {
        let _ = control(
            &control_device,
            DM_DEV_REMOVE,
            &mut header(disk.name, 0, 0, 0),
        );
        return Err(error);
    }
    Ok(PathBuf::from(format!("/dev/dm-{}", minor(&create))))
}

/// Removes an unused mapping. The kernel then drops its copy of the key.
pub fn remove(name: &str) -> io::Result<()> {
    let control_device = File::options()
        .read(true)
        .write(true)
        .open("/dev/mapper/control")?;
    control(&control_device, DM_DEV_REMOVE, &mut header(name, 0, 0, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_follow_the_dm_ioctl_layout() {
        assert_eq!(request(DM_DEV_CREATE), 0xc138_fd03);
        assert_eq!(request(DM_TABLE_LOAD), 0xc138_fd09);

        let key = Secret::new("0f".repeat(64));
        let params = crypt_params(&key, "vdd", true);
        assert_eq!(
            *params,
            format!(
                "aes-xts-plain64 {} 0 /dev/vdd 0 2 allow_discards no_read_workqueue",
                "0f".repeat(64)
            )
        );
        let buffer = table_load("vmm-volume-0", DM_SECURE_DATA_FLAG, 2048, "crypt", &params);
        let word =
            |offset: usize| u32::from_ne_bytes(buffer[offset..offset + 4].try_into().unwrap());
        assert_eq!((word(0), word(4), word(8)), (4, 0, 0));
        assert_eq!(word(12) as usize, buffer.len());
        assert_eq!(
            (word(16), word(20), word(28)),
            (312, 1, DM_SECURE_DATA_FLAG)
        );
        assert_eq!(&buffer[48..60], b"vmm-volume-0");
        assert_eq!(buffer[60], 0);

        let spec = &buffer[312..];
        assert_eq!(spec.len() % 8, 0);
        assert_eq!(u64::from_ne_bytes(spec[0..8].try_into().unwrap()), 0);
        assert_eq!(u64::from_ne_bytes(spec[8..16].try_into().unwrap()), 2048);
        assert_eq!(
            u32::from_ne_bytes(spec[20..24].try_into().unwrap()) as usize,
            spec.len()
        );
        assert_eq!(&spec[24..30], b"crypt\0");
        assert_eq!(&spec[40..40 + params.len()], params.as_bytes());
        assert!(spec[40 + params.len()..].iter().all(|byte| *byte == 0));
        assert!(spec.len() > 40 + params.len());
    }

    #[test]
    fn decodes_the_minor_of_a_reply() {
        let mut reply = vec![0u8; HEADER_BYTES];
        // new_encode_dev(major 253, minor 5), then a minor above 255.
        reply[40..48].copy_from_slice(&((253u64 << 8) | 5).to_ne_bytes());
        assert_eq!(minor(&reply), 5);
        reply[40..48].copy_from_slice(&((253u64 << 8) | 0x2c | (0x1 << 20)).to_ne_bytes());
        assert_eq!(minor(&reply), 0x12c);
    }
}
