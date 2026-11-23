use super::{Settings, netlink};
use crate::protocol::Shares;
use objc2::{AnyThread, rc::Retained};
use objc2_foundation::{NSArray, NSError, NSFileHandle, NSString, NSURL};
use objc2_virtualization::*;
use std::{
    io,
    os::fd::{IntoRawFd, OwnedFd},
    path::Path,
};

pub(super) fn error(e: Retained<NSError>) -> io::Error {
    io::Error::other(e.to_string())
}
fn url(path: &Path) -> io::Result<Retained<NSURL>> {
    NSURL::from_file_path(path).ok_or_else(|| io::Error::other("invalid file URL"))
}

pub(super) fn validate_shares(paths: &Shares) -> io::Result<()> {
    for (name, path) in paths {
        if name.is_empty() || !path.is_absolute() || !path.is_dir() {
            return Err(io::Error::other("invalid share directory"));
        }
    }
    Ok(())
}

fn disk(path: &Path, ro: bool, id: &str) -> io::Result<Retained<VZStorageDeviceConfiguration>> {
    let url = url(path)?;
    // SAFETY: the URL is local and the two fixed ASCII identifiers meet VZ's length and character restrictions.
    unsafe {
        let attachment = VZDiskImageStorageDeviceAttachment::initWithURL_readOnly_error(
            VZDiskImageStorageDeviceAttachment::alloc(),
            &url,
            ro,
        )
        .map_err(error)?;
        let device = VZVirtioBlockDeviceConfiguration::initWithAttachment(
            VZVirtioBlockDeviceConfiguration::alloc(),
            &attachment,
        );
        device.setBlockDeviceIdentifier(&NSString::from_str(id));
        Ok(device.into_super())
    }
}

pub(super) fn configuration(
    settings: &Settings,
    guest_link_fd: Option<OwnedFd>,
) -> io::Result<Retained<VZVirtualMachineConfiguration>> {
    let kernel = url(&settings.bundle.kernel)?;
    let log = url(&settings.serial_log)?;
    // Disk 0 = pool.raw (l4h dom0_disk0, RW); Disk 1 = rootfs.squashfs (RO).
    let mut disks = Vec::new();
    disks.push(disk(&settings.pool, false, "AGENT_POOL")?);
    if let Some(root) = &settings.bundle.rootfs {
        disks.push(disk(root, true, "AGENT_ROOT")?);
    }
    // SAFETY: each attachment belongs to the matching device class. Configuration is local to the caller
    // until all device arrays have been copied and validateWithError has succeeded.
    unsafe {
        let cfg = VZVirtualMachineConfiguration::new();
        let boot = VZLinuxBootLoader::initWithKernelURL(VZLinuxBootLoader::alloc(), &kernel);
        boot.setCommandLine(&NSString::from_str("console=hvc0 panic=-1 psi=1"));
        cfg.setBootLoader(Some(&boot));
        cfg.setCPUCount(settings.cpus.clamp(1, 16));
        cfg.setMemorySize(settings.memory);
        let serial = VZVirtioConsoleDeviceSerialPortConfiguration::new();
        let file = VZFileSerialPortAttachment::initWithURL_append_error(
            VZFileSerialPortAttachment::alloc(),
            &log,
            true,
        )
        .map_err(error)?;
        serial.setAttachment(Some(&file));
        cfg.setSerialPorts(&NSArray::from_retained_slice(&[serial.into_super()]));
        cfg.setStorageDevices(&NSArray::from_retained_slice(&disks));
        cfg.setEntropyDevices(&NSArray::from_retained_slice(&[
            VZVirtioEntropyDeviceConfiguration::new().into_super(),
        ]));
        cfg.setMemoryBalloonDevices(&NSArray::from_retained_slice(&[
            VZVirtioTraditionalMemoryBalloonDeviceConfiguration::new().into_super(),
        ]));
        let mut networks: Vec<Retained<VZNetworkDeviceConfiguration>> = Vec::new();
        let nat = VZVirtioNetworkDeviceConfiguration::new();
        if let Some(mac) = VZMACAddress::initWithString(
            VZMACAddress::alloc(),
            &NSString::from_str("02:56:4d:4d:00:10"),
        ) {
            nat.setMACAddress(&mac);
        }
        nat.setAttachment(Some(&VZNATNetworkDeviceAttachment::new()));
        networks.push(nat.into_super());
        if let Some(fd) = guest_link_fd {
            let link = VZVirtioNetworkDeviceConfiguration::new();
            if let Some(mac) = VZMACAddress::initWithString(
                VZMACAddress::alloc(),
                &NSString::from_str(netlink::GUEST_MAC_STR),
            ) {
                link.setMACAddress(&mac);
            }
            let handle = NSFileHandle::initWithFileDescriptor_closeOnDealloc(
                NSFileHandle::alloc(),
                fd.into_raw_fd(),
                true,
            );
            let attach = VZFileHandleNetworkDeviceAttachment::initWithFileHandle(
                VZFileHandleNetworkDeviceAttachment::alloc(),
                &handle,
            );
            link.setAttachment(Some(&attach));
            networks.push(link.into_super());
        }
        cfg.setNetworkDevices(&NSArray::from_retained_slice(&networks));
        cfg.validateWithError().map_err(error)?;
        Ok(cfg)
    }
}
