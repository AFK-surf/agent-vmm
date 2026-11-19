// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::LinuxL4hCtl;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiskExtent {
    /// Offset in 512-byte sectors.
    pub sector: u64,
    /// Length in 512-byte sectors.
    pub sectors: u64,
}

impl DiskExtent {
    fn end(self) -> Result<u64> {
        self.sector
            .checked_add(self.sectors)
            .filter(|_| {
                self.sectors != 0 && self.sector.is_multiple_of(8) && self.sectors.is_multiple_of(8)
            })
            .ok_or(Error::InvalidExtent)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskVolume {
    pub disk: u64,
    pub read_only: bool,
    pub volatile: bool,
    /// Extents in guest sector order.
    pub extents: Vec<DiskExtent>,
}

pub struct VmSpec<'a> {
    pub name: &'a str,
    pub memory_bytes: u64,
    pub vcpus: u32,
    pub initrd: &'a [u8],
    pub cmdline: &'a str,
    pub volumes: &'a [DiskVolume],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VmState {
    Created,
    Running,
    Stopped,
    Unknown(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VmInfo {
    pub id: u64,
    pub name: String,
    pub memory_bytes: u64,
    pub vcpus: u32,
    pub state: VmState,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    InvalidExtent,
    Busy,
    NoSuchVm(u64),
    Control(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidExtent => f.write_str("disk extent must be finite and 4 KiB aligned"),
            Self::Busy => f.write_str("disk extent overlaps an attached volume"),
            Self::NoSuchVm(id) => write!(f, "no such VM: {id}"),
            Self::Control(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub trait HypervisorCtl {
    fn create_vm(&mut self, spec: &VmSpec<'_>) -> Result<u64>;
    fn list_vms(&self) -> Result<Vec<VmInfo>>;
    fn destroy_vm(&mut self, vm: u64) -> Result<()>;
    fn blk_discard(&mut self, disk: u64, extent: DiskExtent) -> Result<()>;
}

#[derive(Default)]
pub struct MockHypervisorCtl {
    next_id: u64,
    vms: BTreeMap<u64, (VmInfo, Vec<DiskVolume>)>,
    discards: Vec<(u64, DiskExtent)>,
}

impl MockHypervisorCtl {
    pub fn discards(&self) -> &[(u64, DiskExtent)] {
        &self.discards
    }
}

impl HypervisorCtl for MockHypervisorCtl {
    fn create_vm(&mut self, spec: &VmSpec<'_>) -> Result<u64> {
        for volume in spec.volumes {
            for extent in &volume.extents {
                extent.end()?;
            }
        }
        // ID zero belongs to dom0.
        self.next_id += 1;
        let id = self.next_id;
        let info = VmInfo {
            id,
            name: spec.name.into(),
            memory_bytes: spec.memory_bytes,
            vcpus: spec.vcpus,
            state: VmState::Running,
        };
        self.vms.insert(id, (info, spec.volumes.to_vec()));
        Ok(id)
    }

    fn list_vms(&self) -> Result<Vec<VmInfo>> {
        Ok(self.vms.values().map(|(info, _)| info.clone()).collect())
    }

    fn destroy_vm(&mut self, vm: u64) -> Result<()> {
        self.vms.remove(&vm).map(|_| ()).ok_or(Error::NoSuchVm(vm))
    }

    fn blk_discard(&mut self, disk: u64, extent: DiskExtent) -> Result<()> {
        let end = extent.end()?;
        let volumes = self.vms.values().flat_map(|(_, volumes)| volumes);
        if volumes.filter(|volume| volume.disk == disk).any(|volume| {
            volume.extents.iter().any(|attached| {
                attached.sector < end && extent.sector < attached.sector + attached.sectors
            })
        }) {
            return Err(Error::Busy);
        }
        self.discards.push((disk, extent));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_tracks_vm_and_blocks_discard_until_destroyed() -> Result<()> {
        let mut mock = MockHypervisorCtl::default();
        let ctl: &mut dyn HypervisorCtl = &mut mock;
        let extent = DiskExtent {
            sector: 16,
            sectors: 8,
        };
        let id = ctl.create_vm(&VmSpec {
            name: "workload",
            memory_bytes: 64 << 20,
            vcpus: 1,
            initrd: &[],
            cmdline: "",
            volumes: &[DiskVolume {
                disk: 2,
                read_only: true,
                volatile: false,
                extents: vec![extent],
            }],
        })?;
        assert_eq!(ctl.list_vms()?[0].state, VmState::Running);
        assert_eq!(ctl.blk_discard(2, extent), Err(Error::Busy));
        ctl.blk_discard(3, extent)?;
        ctl.destroy_vm(id)?;
        assert!(ctl.list_vms()?.is_empty());
        ctl.blk_discard(2, extent)?;
        assert_eq!(mock.discards(), &[(3, extent), (2, extent)]);
        Ok(())
    }
}
