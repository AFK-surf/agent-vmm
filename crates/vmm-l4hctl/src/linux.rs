// SPDX-License-Identifier: AGPL-3.0-or-later

use super::{
    DiskExtent, DiskInfo, Error, HypervisorCtl, IsolatedNic, Result, VmInfo, VmMemInfo, VmSpec,
    VmState,
};

pub struct LinuxL4hCtl(l4hctl::Ctl);

impl LinuxL4hCtl {
    pub fn open() -> Result<Self> {
        Ok(Self(l4hctl::Ctl::open().map_err(control_error)?))
    }
}

fn control_error(error: l4hctl::Error) -> Error {
    Error::Control(error.0)
}

fn native_spec<'a>(spec: &VmSpec<'a>) -> l4hctl::VmSpec<'a> {
    let volumes = spec
        .volumes
        .iter()
        .map(|volume| {
            let mut flags = 0;
            if volume.read_only {
                flags |= l4hctl::BLK_RO;
            }
            if volume.volatile {
                flags |= l4hctl::BLK_VOLATILE;
            }
            let extents = volume
                .extents
                .iter()
                .map(|extent| (extent.sector, extent.sectors))
                .collect();
            (volume.disk, flags, extents)
        })
        .collect();

    l4hctl::VmSpec {
        name: spec.name,
        mem: spec.memory_bytes,
        vcpus: u64::from(spec.vcpus),
        initrd: spec.initrd,
        cmdline: spec.cmdline,
        disks: Vec::new(),
        volumes,
        io_pool: 0,
        nics: Vec::new(),
        gpu: None,
        usbs: Vec::new(),
    }
}

fn vm_info(vm: l4hctl::Vm) -> VmInfo {
    VmInfo {
        id: vm.id,
        name: vm.name,
        memory_bytes: vm.ram,
        vcpus: vm.vcpus,
        state: match vm.state {
            l4hctl::VM_CREATED => VmState::Created,
            l4hctl::VM_RUNNING => VmState::Running,
            l4hctl::VM_STOPPED => VmState::Stopped,
            state => VmState::Unknown(state),
        },
    }
}

fn isolated_nic(nic: IsolatedNic) -> l4hctl::NicSpec {
    l4hctl::NicSpec {
        switch: nic.switch,
        mac: 0,
        flags: l4hctl::NIC_ISOLATED | l4hctl::NIC_IP_FILTER,
        ips: vec![l4hctl::IpPrefix {
            ip: nic.address.into(),
            len: 32,
        }],
    }
}

impl HypervisorCtl for LinuxL4hCtl {
    fn ensure_switch(&mut self, existing: Option<u64>) -> Result<u64> {
        match existing {
            Some(id) => self.0.switch_info(id).map(|_| id),
            None => self.0.switch_create(),
        }
        .map_err(control_error)
    }

    fn create_networked_vm(&mut self, spec: &VmSpec<'_>, nic: IsolatedNic) -> Result<u64> {
        let mut spec = native_spec(spec);
        spec.nics.push(isolated_nic(nic));
        self.0
            .create_vm(&spec)
            .map(|(vm, _)| vm)
            .map_err(control_error)
    }

    fn attached_extents(&self, disk: u64) -> Result<Vec<DiskExtent>> {
        Ok(self
            .0
            .disk_attachments(disk)
            .map_err(control_error)?
            .into_iter()
            .map(|e| DiskExtent {
                sector: e.start,
                sectors: e.sectors,
            })
            .collect())
    }

    fn blk_disks(&self) -> Result<Vec<DiskInfo>> {
        let count = self.0.host_info().map_err(control_error)?.disks;
        (0..count)
            .map(|id| {
                let disk = self.0.disk_info(id).map_err(control_error)?;
                Ok(DiskInfo {
                    id,
                    sectors: disk.sectors,
                    owned: disk.owner.is_some(),
                    failed: disk.failed,
                })
            })
            .collect()
    }

    fn blk_write(&mut self, disk: u64, sector: u64, bytes: &[u8]) -> Result<()> {
        if !bytes.len().is_multiple_of(4096) {
            return Err(Error::InvalidExtent);
        }
        DiskExtent {
            sector,
            sectors: bytes.len() as u64 / 512,
        }
        .end()?;
        let mut pos = sector;
        for chunk in bytes.chunks(l4hctl::CTL_LOAD_MAX) {
            self.0.disk_write(disk, pos, chunk).map_err(control_error)?;
            pos += chunk.len() as u64 / 512;
        }
        Ok(())
    }
    fn blk_flush(&mut self, disk: u64) -> Result<()> {
        self.0.disk_flush(disk).map_err(control_error)
    }
    fn vm_set_mem(&mut self, vm: u64, bytes: u64) -> Result<()> {
        self.0.set_mem(vm, bytes).map_err(control_error)
    }
    fn host_mem_info(&self) -> Result<(u64, u64)> {
        let host = self.0.host_info().map_err(control_error)?;
        Ok((host.free, host.total))
    }
    fn vm_mem_info(&self, vm: u64) -> Result<VmMemInfo> {
        let mem = self.0.mem_info(vm).map_err(control_error)?;
        Ok(VmMemInfo {
            boot_bytes: mem.boot,
            region_bytes: mem.region,
            target_bytes: mem.target,
            plugged_bytes: mem.plugged,
            reported_bytes: mem.reported,
            clean_bytes: mem.clean,
        })
    }
    fn console_read(&self, vm: u64, offset: u64) -> Result<(u64, Vec<u8>, u64)> {
        self.0.console_read(vm, offset).map_err(control_error)
    }

    fn create_vm(&mut self, spec: &VmSpec<'_>) -> Result<u64> {
        let (vm, _) = self
            .0
            .create_vm(&native_spec(spec))
            .map_err(control_error)?;
        Ok(vm)
    }

    fn list_vms(&self) -> Result<Vec<VmInfo>> {
        let vms = self.0.vm_list().map_err(control_error)?;
        Ok(vms.into_iter().map(vm_info).collect())
    }

    fn destroy_vm(&mut self, vm: u64) -> Result<()> {
        self.0.vm_destroy(vm).map_err(control_error)
    }

    fn blk_discard(&mut self, disk: u64, extent: DiskExtent) -> Result<()> {
        extent.end()?;
        self.0
            .blk_discard(disk, extent.sector, extent.sectors)
            .map_err(control_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DiskVolume;

    #[test]
    fn translates_storage_flags_and_vm_status() {
        let volumes = [DiskVolume {
            disk: 6,
            read_only: true,
            volatile: true,
            extents: vec![
                DiskExtent {
                    sector: 16384,
                    sectors: 8192,
                },
                DiskExtent {
                    sector: 0,
                    sectors: 8192,
                },
            ],
        }];
        let spec = native_spec(&VmSpec {
            name: "workload",
            memory_bytes: 64 << 20,
            vcpus: 2,
            initrd: &[3, 5],
            cmdline: "quiet",
            volumes: &volumes,
        });
        assert_eq!(spec.mem, 64 << 20);
        assert_eq!(spec.vcpus, 2);
        assert_eq!(spec.initrd, &[3, 5]);
        assert_eq!(spec.volumes, [(6, 3, vec![(16384, 8192), (0, 8192)])]);

        for (native, state) in [
            (l4hctl::VM_CREATED, VmState::Created),
            (l4hctl::VM_RUNNING, VmState::Running),
            (l4hctl::VM_STOPPED, VmState::Stopped),
            (99, VmState::Unknown(99)),
        ] {
            let info = vm_info(l4hctl::Vm {
                id: 1,
                name: "workload".into(),
                ram: 64 << 20,
                vcpus: 2,
                state: native,
            });
            assert_eq!(info.state, state);
        }
    }
}
