mod support;

use std::sync::Arc;
use support::Fixture;
use vmm_core::{
    backend::{Dom0Backend, ProcessState, WorkspaceRequest},
    fake::Fault,
    *,
};

#[tokio::test]
async fn cue_sleep_wake_and_replace_generation() {
    let f = Fixture::new();
    f.provision("cue").await;
    let mut def = Fixture::definition("cue", "worker", AccessMode::ReadWrite);
    let core = &f.core;
    core.define_sandbox(&def).await.unwrap();
    let first = core.start(&def.key).await.unwrap();
    for path in ["/root-file", "/data/volume-file"] {
        f.backend
            .workspace(
                &first.boot,
                WorkspaceRequest::Write {
                    path: path.into(),
                    bytes: b"persistent".to_vec(),
                },
            )
            .await
            .unwrap();
    }
    let process = f.backend.exec(&first.boot, &["cue".into()]).await.unwrap();
    f.backend
        .finish_process(&first.boot, &process, 0, b"ok".to_vec())
        .unwrap();
    assert_eq!(
        f.backend.process(&first.boot, &process).await.unwrap(),
        ProcessState::Exited(0)
    );
    assert_eq!(f.backend.logs(&first.boot, &process).await.unwrap(), b"ok");
    core.quiesce(&first.boot).await.unwrap();
    assert_eq!(
        core.store.sandbox(&def.key).unwrap().state,
        SandboxState::Quiesced
    );
    core.stop(&first.boot, false).await.unwrap();
    assert_eq!(f.backend.overlay(&def.key).unwrap(), def.root_overlay);
    assert_eq!(
        core.store.volume("cue", "data").unwrap(),
        Fixture::volume("cue")
    );
    let next = core.start(&def.key).await.unwrap();
    for path in ["/root-file", "/data/volume-file"] {
        assert_eq!(
            f.backend
                .workspace(&next.boot, WorkspaceRequest::Read(path.into()))
                .await
                .unwrap(),
            b"persistent"
        );
    }
    assert_ne!(first.boot.instance_id, next.boot.instance_id);
    assert!(matches!(
        core.stop(&first.boot, true).await,
        Err(Error::Fenced)
    ));
    let old = def.key.clone();
    def.key.generation_id = "g2".into();
    assert!(core.define_sandbox(&def).await.is_err());
    core.stop(&next.boot, false).await.unwrap();
    core.delete_sandbox(&old).await.unwrap();
    assert!(f.backend.overlay(&old).unwrap().is_none());
    assert_eq!(
        core.store.sandbox(&old).unwrap().state,
        SandboxState::Deleted
    );
    core.define_sandbox(&def).await.unwrap();
    let replacement = core.start(&def.key).await.unwrap();
    assert_eq!(
        f.backend
            .workspace(
                &replacement.boot,
                WorkspaceRequest::Read("/data/volume-file".into())
            )
            .await
            .unwrap(),
        b"persistent"
    );
    assert!(matches!(
        f.backend
            .workspace(
                &replacement.boot,
                WorkspaceRequest::Read("/root-file".into())
            )
            .await,
        Err(Error::Missing(_))
    ));
    assert_ne!(replacement.l4h_vm_id, next.l4h_vm_id);
    assert_eq!(
        core.store.volume_mounts("cue", "data").unwrap(),
        [(def.key, AccessMode::ReadWrite)]
    );
}

#[tokio::test]
async fn only_last_scope_or_sandbox_reference_reclaims_erofs() {
    let f = Fixture::new();
    f.provision("one").await;
    f.provision("two").await;
    assert_eq!(
        f.core.store.resolve_image("one", "cue").unwrap(),
        f.core.store.resolve_image("two", "cue").unwrap()
    );
    let def = Fixture::definition("one", "work", AccessMode::ReadOnly);
    f.core.define_sandbox(&def).await.unwrap();
    let vm = f.core.start(&def.key).await.unwrap();
    for scope in ["one", "two"] {
        assert_eq!(
            f.core
                .remove_image_reference(scope, "cue", [7; 32])
                .await
                .unwrap(),
            0
        );
    }
    assert_eq!(f.backend.reclaimed_image_bytes().unwrap(), 0);
    f.core.stop(&vm.boot, false).await.unwrap();
    assert!(f.core.store.image_artifact([7; 32]).is_ok());
    f.core.delete_sandbox(&def.key).await.unwrap();
    assert_eq!(f.backend.reclaimed_image_bytes().unwrap(), 1024);
    assert!(matches!(
        f.core.store.image_artifact([7; 32]),
        Err(Error::Missing(_))
    ));
    f.core
        .import_image("two", "again", &Fixture::image())
        .await
        .unwrap();
    assert!(matches!(
        f.core.remove_image_reference("two", "again", [8; 32]).await,
        Err(Error::Fenced)
    ));
    assert_eq!(
        f.core
            .remove_image_reference("two", "again", [7; 32])
            .await
            .unwrap(),
        1024
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volume_writer_race_and_shared_readers() {
    let f = Fixture::new();
    f.provision("one").await;
    let mut keys = vec![];
    for (id, mode) in [
        ("a", AccessMode::ReadWrite),
        ("b", AccessMode::ReadWrite),
        ("c", AccessMode::ReadOnly),
        ("d", AccessMode::ReadOnly),
    ] {
        let def = Fixture::definition("one", id, mode);
        f.core.define_sandbox(&def).await.unwrap();
        keys.push(def.key);
    }
    let spawn = |index: usize| {
        let core = f.core.clone();
        let key = keys[index].clone();
        tokio::spawn(async move { core.start(&key).await })
    };
    let (left, right) = tokio::join!(spawn(0), spawn(1));
    let (winner, denied) = match (left.unwrap(), right.unwrap()) {
        (Ok(vm), err) | (err, Ok(vm)) => (vm, err),
        _ => panic!("neither writer won"),
    };
    assert!(matches!(denied, Err(Error::Conflict(_))));
    assert!(matches!(
        f.core.start(&keys[2]).await,
        Err(Error::Conflict(_))
    ));
    f.core.stop(&winner.boot, false).await.unwrap();
    let (left, right) = tokio::join!(spawn(2), spawn(3));
    left.unwrap().unwrap();
    right.unwrap().unwrap();
    assert_eq!(f.core.store.volume_mounts("one", "data").unwrap().len(), 2);
    assert!(matches!(
        f.core.start(&keys[0]).await,
        Err(Error::Conflict(_))
    ));
}

#[tokio::test]
async fn restart_adopts_authenticated_boot_and_converges_missing_vm() {
    let f = Fixture::new();
    f.provision("one").await;
    let def = Fixture::definition("one", "vm", AccessMode::ReadWrite);
    f.core.define_sandbox(&def).await.unwrap();
    let vm = f.core.start(&def.key).await.unwrap();
    f.backend
        .workspace(
            &vm.boot,
            WorkspaceRequest::Write {
                path: "hello".into(),
                bytes: b"alive".to_vec(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        Store::open(f.directory.path().join("host.db")),
        Err(Error::Conflict(_))
    ));
    drop(f.core);
    let reopened = Core::new(
        Arc::new(Store::open(f.directory.path().join("host.db")).unwrap()),
        f.backend.clone(),
    );
    assert_eq!(
        reopened
            .reconcile_after_restart()
            .await
            .unwrap()
            .adopted
            .as_slice(),
        std::slice::from_ref(&vm)
    );
    assert_eq!(
        f.backend
            .workspace(&vm.boot, WorkspaceRequest::Read("hello".into()))
            .await
            .unwrap(),
        b"alive"
    );
    f.backend.destroy_vm(&vm.boot).await.unwrap();
    assert_eq!(
        reopened
            .reconcile_after_restart()
            .await
            .unwrap()
            .stopped
            .as_slice(),
        std::slice::from_ref(&def.key)
    );
    assert_eq!(f.backend.shutdown_calls().unwrap(), 0);
    let sb = reopened.store.sandbox(&def.key).unwrap();
    assert_eq!(sb.state, SandboxState::Stopped);
    assert!(sb.exit_reason.unwrap().contains("absent"));
    assert!(
        reopened
            .store
            .volume_mounts("one", "data")
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn capacity_and_interrupted_start_preserve_identity_and_locks() {
    let f = Fixture::new();
    f.provision("one").await;
    let def = Fixture::definition("one", "vm", AccessMode::ReadWrite);
    f.core.define_sandbox(&def).await.unwrap();
    for (fault, expected) in [
        (Fault::StorageHeadroom, CapacityDimension::StorageHeadroom),
        (Fault::ComputeSlots, CapacityDimension::ComputeSlots),
    ] {
        f.backend.inject(fault).unwrap();
        assert!(matches!(f.core.start(&def.key).await,Err(Error::Capacity(d)) if d==expected));
        assert!(
            f.core
                .store
                .volume_mounts("one", "data")
                .unwrap()
                .is_empty()
        );
    }
    f.backend.inject(Fault::InterruptedStart).unwrap();
    assert!(matches!(
        f.core.start(&def.key).await,
        Err(Error::Interrupted)
    ));
    assert_eq!(
        f.core
            .reconcile_after_restart()
            .await
            .unwrap()
            .stopped
            .len(),
        1
    );
    f.backend.inject(Fault::LostStartReply).unwrap();
    assert!(matches!(
        f.core.start(&def.key).await,
        Err(Error::Interrupted)
    ));
    let pending = f.core.store.sandbox(&def.key).unwrap();
    assert_eq!(pending.state, SandboxState::Starting);
    assert_eq!(f.core.store.volume_mounts("one", "data").unwrap().len(), 1);
    let adopted = f.core.reconcile_after_restart().await.unwrap().adopted;
    assert_eq!(adopted.len(), 1);
    assert_eq!(Some(adopted[0].boot.clone()), pending.boot);
    assert_eq!(f.backend.list_live_vms().await.unwrap().len(), 1);
}

#[tokio::test]
async fn routes_replay_exact_revision_and_reject_stale_or_unverified_endpoints() {
    let f = Fixture::new();
    f.provision("one").await;
    let def = Fixture::definition("one", "vm", AccessMode::ReadOnly);
    f.core.define_sandbox(&def).await.unwrap();
    let vm = f.core.start(&def.key).await.unwrap();
    let export = ServiceExport {
        id: "web".into(),
        boot: vm.boot.clone(),
        port: 80,
    };
    f.core.publish_export(&export).await.unwrap();
    let binding = RouteBinding {
        id: "route".into(),
        source: vm.boot.clone(),
        export,
        vip: [192, 0, 2, 1].into(),
    };
    let first = f.core.bind_route(&binding).await.unwrap();
    assert!(first.revision > 0);
    assert_eq!(first, f.core.bind_route(&binding).await.unwrap());
    f.core.revoke_route("route", first.revision).await.unwrap();
    let second = f.core.bind_route(&binding).await.unwrap();
    assert!(second.revision > first.revision);
    assert!(matches!(
        f.core.revoke_route("route", first.revision).await,
        Err(Error::Fenced)
    ));
    assert_eq!(f.backend.route("route").unwrap(), Some(second));
    f.backend.inject(Fault::HandshakeMismatch).unwrap();
    let report = f.core.reconcile_after_restart().await.unwrap();
    assert!(report.adopted.is_empty());
    assert_eq!(report.mismatched.as_slice(), std::slice::from_ref(&def.key));
    assert_eq!(
        f.core.store.sandbox(&def.key).unwrap().state,
        SandboxState::Unverified
    );
    assert_eq!(f.core.store.volume_mounts("one", "data").unwrap().len(), 1);
    assert!(f.backend.route("route").unwrap().is_none());
    assert!(matches!(
        f.core.bind_route(&binding).await,
        Err(Error::Fenced)
    ));
}

#[tokio::test]
async fn quota_and_mounts_are_isolated_by_scope() {
    let f = Fixture::new();
    f.provision("left").await;
    f.provision("right").await;
    let mut v = Fixture::volume("left");
    v.volume_id = "overflow".into();
    v.quota_bytes = u64::MAX;
    assert!(matches!(
        f.core.create_volume(&v).await,
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    let left = Fixture::definition("left", "same", AccessMode::ReadWrite);
    let right = Fixture::definition("right", "same", AccessMode::ReadWrite);
    f.core.define_sandbox(&left).await.unwrap();
    f.core.define_sandbox(&right).await.unwrap();
    let left_vm = f.core.start(&left.key).await.unwrap();
    let right_vm = f.core.start(&right.key).await.unwrap();
    f.backend
        .workspace(
            &left_vm.boot,
            WorkspaceRequest::Write {
                path: "/data/file".into(),
                bytes: b"private".to_vec(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        f.backend
            .workspace(&right_vm.boot, WorkspaceRequest::Read("/data/file".into()))
            .await,
        Err(Error::Missing(_))
    ));
}

#[tokio::test]
async fn sparse_volumes_and_overlay_share_actual_usage() {
    let f = Fixture::new();
    let store = &f.core.store;
    let mut scope = Fixture::scope("sparse");
    scope.limits.disk_quota_bytes = 1024;
    store.create_scope(&scope, None).unwrap();
    f.core
        .import_image("sparse", "cue", &Fixture::image())
        .await
        .unwrap();
    let mut first = Fixture::volume("sparse");
    first.quota_bytes = 1024;
    let second = Volume {
        volume_id: "second".into(),
        ..first.clone()
    };
    f.core.create_volume(&first).await.unwrap();
    f.core
        .create_volume(&second)
        .await
        .expect("second empty sparse volume");
    let mut definition = Fixture::definition("sparse", "worker", AccessMode::ReadWrite);
    definition.root_overlay.as_mut().unwrap().quota_bytes = 1024;
    f.core.define_sandbox(&definition).await.unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), 0);

    let mut third = Volume {
        volume_id: "third".into(),
        quota_bytes: 1025,
        ..first.clone()
    };
    assert!(matches!(
        f.core.create_volume(&third).await,
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    third.quota_bytes = 1024;
    store.record_volume_usage("sparse", "data", 600).unwrap();
    store.record_volume_usage("sparse", "second", 500).unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), 1100);
    assert_eq!(store.volume("sparse", "data").unwrap().used_bytes, 600);
    f.core.create_volume(&first).await.unwrap();
    assert_eq!(store.volume("sparse", "data").unwrap().used_bytes, 600);
    assert!(matches!(
        f.core.create_volume(&third).await,
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    let mut other = definition.clone();
    other.key.sandbox_id = "other".into();
    assert!(matches!(
        f.core.define_sandbox(&other).await,
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));

    store.record_volume_usage("sparse", "second", 400).unwrap();
    third.used_bytes = 25;
    assert!(matches!(
        f.core.create_volume(&third).await,
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    let vm = f.core.start(&definition.key).await.unwrap();
    store.record_overlay_usage(&definition.key, 24).unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), 1024);
    f.core.stop(&vm.boot, false).await.unwrap();
    let resumed = f.core.start(&definition.key).await.unwrap();
    f.core.stop(&resumed.boot, false).await.unwrap();
    f.core.delete_sandbox(&definition.key).await.unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), 1000);
    assert!(matches!(
        store.record_overlay_usage(&definition.key, 99),
        Err(Error::Missing(_))
    ));
    f.core.discard_volume("sparse", "second").await.unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), 600);
    f.core.create_volume(&third).await.unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), 625);

    store
        .record_volume_usage("sparse", "data", u64::MAX)
        .unwrap();
    assert!(matches!(
        store.scope_used_bytes("sparse"),
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    store.record_volume_usage("sparse", "third", 0).unwrap();
    assert_eq!(store.scope_used_bytes("sparse").unwrap(), u64::MAX);
}
