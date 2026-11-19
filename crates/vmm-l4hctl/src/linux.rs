// SPDX-License-Identifier: AGPL-3.0-or-later

use super::{DiskExtent, Error, HypervisorCtl, Result, VmInfo, VmSpec, VmState};

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

impl HypervisorCtl for LinuxL4hCtl {
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
