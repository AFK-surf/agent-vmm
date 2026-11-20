// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IsolatedNic {
    pub switch: u64,
    pub address: Ipv4Addr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiskInfo {
    pub id: u64,
    pub sectors: u64,
    pub owned: bool,
    pub failed: bool,
}

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

pub trait HypervisorCtl: Send {
    /// Verify the persisted switch id, or allocate one on first installation.
    fn ensure_switch(&mut self, existing: Option<u64>) -> Result<u64>;
    /// Install the source IP filter before any vCPU can run.
    fn create_networked_vm(&mut self, spec: &VmSpec<'_>, nic: IsolatedNic) -> Result<u64>;
    fn blk_disks(&self) -> Result<Vec<DiskInfo>>;
    fn attached_extents(&self, disk: u64) -> Result<Vec<DiskExtent>>;
    fn blk_write(&mut self, disk: u64, sector: u64, bytes: &[u8]) -> Result<()>;
    fn blk_flush(&mut self, disk: u64) -> Result<()>;
    fn vm_set_mem(&mut self, vm: u64, bytes: u64) -> Result<()>;
    fn console_read(&self, vm: u64, offset: u64) -> Result<(u64, Vec<u8>, u64)>;
    fn create_vm(&mut self, spec: &VmSpec<'_>) -> Result<u64>;
    fn list_vms(&self) -> Result<Vec<VmInfo>>;
    fn destroy_vm(&mut self, vm: u64) -> Result<()>;
    fn blk_discard(&mut self, disk: u64, extent: DiskExtent) -> Result<()>;
}

#[derive(Default)]
pub struct MockHypervisorCtl {
    next_id: u64,
    switches: u64,
    nics: BTreeMap<u64, IsolatedNic>,
    disks: Vec<DiskInfo>,
    sectors: BTreeMap<(u64, u64), Vec<u8>>,

    vms: BTreeMap<u64, (VmInfo, Vec<DiskVolume>)>,
    discards: Vec<(u64, DiskExtent)>,
}

impl MockHypervisorCtl {
    pub fn with_disks(disks: Vec<DiskInfo>) -> Self {
        Self {
            disks,
            ..Self::default()
        }
    }

    fn check_free(&self, disk: u64, extent: DiskExtent) -> Result<()> {
        let end = extent.end()?;
        if self
            .vms
            .values()
            .flat_map(|(_, volumes)| volumes)
            .filter(|v| v.disk == disk)
            .flat_map(|v| &v.extents)
            .any(|e| e.sector < end && extent.sector < e.sector + e.sectors)
        {
            return Err(Error::Busy);
        }
        Ok(())
    }

    pub fn volumes(&self, vm: u64) -> Option<&[DiskVolume]> {
        self.vms.get(&vm).map(|(_, v)| v.as_slice())
    }

    pub fn discards(&self) -> &[(u64, DiskExtent)] {
        &self.discards
    }
}

impl HypervisorCtl for MockHypervisorCtl {
    fn ensure_switch(&mut self, existing: Option<u64>) -> Result<u64> {
        match existing {
            Some(id) if id < self.switches => Ok(id),
            Some(_) => Err(Error::Control("unknown switch".into())),
            None => {
                let id = self.switches;
                self.switches += 1;
                Ok(id)
            }
        }
    }

    fn create_networked_vm(&mut self, spec: &VmSpec<'_>, nic: IsolatedNic) -> Result<u64> {
        self.ensure_switch(Some(nic.switch))?;
        if self.nics.values().any(|n| *n == nic) {
            return Err(Error::Busy);
        }
        let id = self.create_vm(spec)?;
        self.nics.insert(id, nic);
        Ok(id)
    }

    fn attached_extents(&self, disk: u64) -> Result<Vec<DiskExtent>> {
        Ok(self
            .vms
            .values()
            .flat_map(|(_, v)| v)
            .filter(|v| v.disk == disk)
            .flat_map(|v| v.extents.iter().copied())
            .collect())
    }

    fn blk_disks(&self) -> Result<Vec<DiskInfo>> {
        Ok(self.disks.clone())
    }

    fn blk_write(&mut self, disk: u64, sector: u64, bytes: &[u8]) -> Result<()> {
        if !bytes.len().is_multiple_of(4096) {
            return Err(Error::InvalidExtent);
        }
        let extent = DiskExtent {
            sector,
            sectors: bytes.len() as u64 / 512,
        };
        let end = extent.end()?;
        if !self
            .disks
            .iter()
            .any(|d| d.id == disk && !d.failed && !d.owned && end <= d.sectors)
        {
            return Err(Error::InvalidExtent);
        }
        self.check_free(disk, extent)?;
        for (i, block) in bytes.as_chunks::<4096>().0.iter().enumerate() {
            self.sectors
                .insert((disk, sector + i as u64 * 8), block.to_vec());
        }
        Ok(())
    }

    fn blk_flush(&mut self, disk: u64) -> Result<()> {
        self.disks
            .iter()
            .any(|d| d.id == disk)
            .then_some(())
            .ok_or(Error::InvalidExtent)
    }

    fn vm_set_mem(&mut self, vm: u64, bytes: u64) -> Result<()> {
        self.vms
            .get_mut(&vm)
            .ok_or(Error::NoSuchVm(vm))?
            .0
            .memory_bytes = bytes;
        Ok(())
    }

    fn console_read(&self, vm: u64, offset: u64) -> Result<(u64, Vec<u8>, u64)> {
        self.vms.get(&vm).ok_or(Error::NoSuchVm(vm))?;
        Ok((offset, Vec::new(), 0))
    }

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
        self.nics.remove(&vm);
        self.vms.remove(&vm).map(|_| ()).ok_or(Error::NoSuchVm(vm))
    }

    fn blk_discard(&mut self, disk: u64, extent: DiskExtent) -> Result<()> {
        let end = extent.end()?;
        self.check_free(disk, extent)?;
        self.sectors
            .retain(|&(d, s), _| d != disk || s < extent.sector || s >= end);
        self.discards.push((disk, extent));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolated_nic_lives_with_its_vm_and_image_writes_respect_attachments() -> Result<()> {
        let mut ctl = MockHypervisorCtl::with_disks(vec![DiskInfo {
            id: 4,
            sectors: 8192,
            owned: false,
            failed: false,
        }]);
        let switch = ctl.ensure_switch(None)?;
        assert_eq!(ctl.ensure_switch(Some(switch))?, switch);
        let nic = IsolatedNic {
            switch,
            address: Ipv4Addr::new(10, 77, 0, 2),
        };
        let extent = DiskExtent {
            sector: 0,
            sectors: 8,
        };
        let spec = VmSpec {
            name: "guest",
            memory_bytes: 64 << 20,
            vcpus: 1,
            initrd: &[],
            cmdline: "",
            volumes: &[DiskVolume {
                disk: 4,
                read_only: true,
                volatile: false,
                extents: vec![extent],
            }],
        };
        ctl.blk_write(4, 0, &[1; 4096])?;
        ctl.blk_flush(4)?;
        let id = ctl.create_networked_vm(&spec, nic)?;
        assert_eq!(ctl.create_networked_vm(&spec, nic), Err(Error::Busy));
        assert_eq!(ctl.blk_write(4, 0, &[2; 4096]), Err(Error::Busy));
        ctl.vm_set_mem(id, 128 << 20)?;
        assert_eq!(ctl.list_vms()?[0].memory_bytes, 128 << 20);
        assert_eq!(ctl.console_read(id, 5)?, (5, Vec::new(), 0));
        ctl.destroy_vm(id)?;
        ctl.blk_discard(4, extent)?;
        assert!(ctl.sectors.is_empty());
        ctl.create_networked_vm(&spec, nic)?;
        Ok(())
    }

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
