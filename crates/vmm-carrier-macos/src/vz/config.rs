use super::Settings;
use crate::protocol::Shares;
use objc2::{AnyThread, rc::Retained};
use objc2_foundation::{NSArray, NSDictionary, NSError, NSString, NSURL};
use objc2_virtualization::*;
use std::{io, path::Path};

pub(super) fn error(e: Retained<NSError>) -> io::Error {
    io::Error::other(e.to_string())
}
fn url(path: &Path) -> io::Result<Retained<NSURL>> {
    NSURL::from_file_path(path).ok_or_else(|| io::Error::other("invalid file URL"))
}

pub(super) fn share(paths: &Shares) -> io::Result<Retained<VZMultipleDirectoryShare>> {
    let mut names = Vec::new();
    let mut dirs = Vec::new();
    // SAFETY: keys are validated with the framework and the directory URLs stay alive through initialization.
    unsafe {
        for (name, path) in paths {
            if !path.is_absolute() || !path.is_dir() {
                return Err(io::Error::other("invalid share directory"));
            }
            let name = NSString::from_str(name);
            VZMultipleDirectoryShare::validateName_error(&name).map_err(error)?;
            names.push(name);
            let url = url(path)?;
            dirs.push(VZSharedDirectory::initWithURL_readOnly(
                VZSharedDirectory::alloc(),
                &url,
                false,
            ));
        }
        let keys: Vec<_> = names.iter().map(|n| &**n).collect();
        let map = NSDictionary::from_retained_objects(&keys, &dirs);
        Ok(VZMultipleDirectoryShare::initWithDirectories(
            VZMultipleDirectoryShare::alloc(),
            &map,
        ))
    }
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
    shares: &Shares,
) -> io::Result<Retained<VZVirtualMachineConfiguration>> {
    let kernel = url(&settings.bundle.kernel)?;
    let initrd = url(&settings.bundle.initramfs)?;
    let log = url(&settings.serial_log)?;
    let mut disks = Vec::new();
    if let Some(root) = &settings.bundle.rootfs {
        disks.push(disk(root, true, "AGENT_ROOT")?);
    }
    disks.push(disk(&settings.pool, false, "AGENT_POOL")?);
    let shares = share(shares)?;
    // SAFETY: each attachment belongs to the matching device class. Configuration is local to the caller
    // until all device arrays have been copied and validateWithError has succeeded.
    unsafe {
        let cfg = VZVirtualMachineConfiguration::new();
        let boot = VZLinuxBootLoader::initWithKernelURL(VZLinuxBootLoader::alloc(), &kernel);
        boot.setInitialRamdiskURL(Some(&initrd));
        boot.setCommandLine(&NSString::from_str("console=hvc0 panic=-1 psi=1"));
        cfg.setBootLoader(Some(&boot));
        cfg.setCPUCount(settings.cpus);
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
        cfg.setSocketDevices(&NSArray::from_retained_slice(&[
            VZVirtioSocketDeviceConfiguration::new().into_super(),
        ]));
        let network = VZVirtioNetworkDeviceConfiguration::new();
        network.setAttachment(Some(&VZNATNetworkDeviceAttachment::new()));
        cfg.setNetworkDevices(&NSArray::from_retained_slice(&[network.into_super()]));
        let fs = VZVirtioFileSystemDeviceConfiguration::initWithTag(
            VZVirtioFileSystemDeviceConfiguration::alloc(),
            &NSString::from_str("agent.hostshares"),
        );
        fs.setShare(Some(&shares));
        cfg.setDirectorySharingDevices(&NSArray::from_retained_slice(&[fs.into_super()]));
        cfg.validateWithError().map_err(error)?;
        Ok(cfg)
    }
}
