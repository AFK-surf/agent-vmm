use agent_vmm_proto::host::v1 as h;
use std::{collections::HashSet, sync::Arc};
use tonic::Code;
use vmm_compat::operator::{DiskSample, LocalInventory};
use vmm_core::{EgressMode, Limits, Origin, RemoteScopePolicy, Scope, ScopeState, Store};

fn scope(id: &str, origin: Origin) -> Scope {
    Scope {
        id: id.into(),
        origin,
        generation: 7,
        revision: 1,
        limits: Limits {
            disk_quota_bytes: 4096,
            pids_max: 16,
            vcpus: 1,
            boot_ram_mb: 64,
            max_ram_mb: 64,
        },
        egress_mode: EgressMode::DenyAll,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    }
}

#[test]
fn pages_include_offline_allocations_and_share_boot_id_with_disk_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(tmp.path().join("db")).unwrap());
    let view = LocalInventory::new(store.clone());
    for i in 0..32 {
        let id = format!("local-{i:02}");
        store
            .create_scope(&scope(&id, Origin::Local), None)
            .unwrap();
    }
    store
        .create_scope(&scope("system.builder", Origin::Local), None)
        .unwrap();
    store
        .install_appliance("service", "appliance-private")
        .unwrap();
    store.create_registration("remote-reg").unwrap();
    for reg in ["appliance-private", "remote-reg"] {
        let channel = store.connect(reg, 1).unwrap();
        let allocation = store
            .ensure_remote_scope(
                &channel,
                "allocation-A",
                "test",
                scope(
                    "internal",
                    Origin::Remote {
                        registration_id: reg.into(),
                    },
                ),
                &RemoteScopePolicy {
                    revision: 1,
                    max_pids: 16,
                    max_disk_quota_bytes: 4096,
                    max_egress_mode: EgressMode::DenyAll,
                },
            )
            .unwrap();
        let next = store
            .transition_scope(
                &allocation.scope.id,
                allocation.scope.revision,
                ScopeState::Draining,
                Some(&channel),
            )
            .unwrap();
        store
            .transition_scope(
                &next.id,
                next.revision,
                ScopeState::Retained,
                Some(&channel),
            )
            .unwrap();
        store.disconnect(&channel).unwrap();
    }
    let active = HashSet::from(["local-00".into()]);
    let read = |after: &str, vm| {
        view.environments(
            h::ListLocalEnvironmentPageRequest {
                after_id: after.into(),
            },
            vm,
            &active,
        )
        .unwrap()
    };
    let page = read("", h::VmState::Running);
    assert_eq!(page.environments.len(), 32);
    assert_eq!(page.next_cursor, "local-31");
    let first = &page.environments[0];
    assert_eq!(first.origin, "local");
    assert_eq!(first.revision, 1);
    assert_eq!(first.state, "observed_active");
    assert_eq!(first.private_disk_bytes, Some(0));
    let disk = view.disk(
        h::VmState::Running,
        DiskSample {
            used: 0,
            capacity: 4096,
            import_reservations: 0,
        },
    );
    assert_eq!(disk.boot_id, page.boot_id);
    let counters = disk.guest_state_fs.unwrap();
    assert_eq!(
        (
            counters.used_bytes,
            counters.capacity_bytes,
            counters.import_reservation_bytes
        ),
        (Some(0), Some(4096), Some(0))
    );
    let next = read(&page.next_cursor, h::VmState::Running);
    assert_eq!(next.boot_id, page.boot_id);
    assert_eq!(next.next_cursor, "");
    assert_eq!(next.environments.len(), 1);
    let row = &next.environments[0];
    assert_eq!(row.state, "retired");
    assert_eq!(row.origin, "remote");
    assert_eq!(row.registration_id, "remote-reg");
    assert_eq!(row.allocation_id, "allocation-A");
    assert_eq!(row.allocation_generation, 7);
    assert!(row.revision >= 1);
    assert_eq!(
        read("", h::VmState::Stopped).environments[0].state,
        "vm_stopped"
    );
    let long = h::ListLocalEnvironmentPageRequest {
        after_id: "x".repeat(201),
    };
    assert_eq!(
        view.environments(long, h::VmState::Running, &active)
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
}

#[path = "operator/disposal.rs"]
mod disposal;

#[path = "operator/rotation.rs"]
mod rotation;

#[path = "operator/offline.rs"]
mod offline;
