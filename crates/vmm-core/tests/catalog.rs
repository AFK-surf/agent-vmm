mod support;

use std::sync::Arc;
use support::Fixture;
use vmm_core::*;

#[test]
fn ensure_scope_replays_current_policy_and_fences_identity_and_epoch() {
    let f = Fixture::new();
    let store = &f.core.store;
    store.create_registration("remote").unwrap();
    let old = store.connect("remote", 1).unwrap();
    let mut desired = Fixture::scope("worker");
    desired.origin = Origin::Remote {
        registration_id: "remote".into(),
    };
    desired.revision = 0;
    assert!(matches!(
        store.create_scope(&desired, Some(&old)),
        Err(Error::Invalid("scope revision"))
    ));
    assert!(matches!(
        store.ensure_scope(&desired, Some(&old)),
        Err(Error::Invalid("scope revision"))
    ));
    desired.revision = 1;
    assert_eq!(store.ensure_scope(&desired, Some(&old)).unwrap(), desired);
    let current = store.connect("remote", 2).unwrap();
    assert!(matches!(
        store.ensure_scope(&desired, Some(&old)),
        Err(Error::Fenced)
    ));
    let wrong_origin = Scope {
        origin: Origin::Local,
        ..desired.clone()
    };
    assert!(matches!(
        store.ensure_scope(&wrong_origin, None),
        Err(Error::Fenced)
    ));
    let wrong_generation = Scope {
        generation: 5,
        ..desired.clone()
    };
    assert!(matches!(
        store.ensure_scope(&wrong_generation, Some(&current)),
        Err(Error::Fenced)
    ));
    store.set_admission("remote", true).unwrap();
    let older = desired.clone();
    desired.revision = 8;
    desired.limits.vcpus = 4;
    desired.egress_mode = EgressMode::PublicInternet;
    desired.host_socket_rules = vec![HostSocketRule {
        port: 42,
        socket_path: "/run/test".into(),
    }];
    assert_eq!(
        store.ensure_scope(&desired, Some(&current)).unwrap(),
        desired
    );
    assert_eq!(store.ensure_scope(&older, Some(&current)).unwrap(), desired);
    desired.limits.pids_max = 8096;
    assert_eq!(
        store.ensure_scope(&desired, Some(&current)).unwrap(),
        desired
    );
    assert_eq!(
        store.ensure_scope(&desired, Some(&current)).unwrap(),
        desired
    );
    let draining = store
        .transition_scope("worker", 8, ScopeState::Draining, Some(&current))
        .unwrap();
    desired.revision = 10;
    assert_eq!(
        store.ensure_scope(&desired, Some(&current)).unwrap(),
        draining
    );
    desired.id = "new".into();
    assert!(matches!(
        store.ensure_scope(&desired, Some(&current)),
        Err(Error::Draining)
    ));
}

fn check_pages(expected: &[&str], list: impl Fn(Option<&str>, usize) -> Vec<String>) {
    assert_eq!(list(None, 0), Vec::<String>::new());
    assert_eq!(list(None, usize::MAX), expected);
    assert_eq!(list(None, 1), expected[..1]);
    assert_eq!(list(Some(expected[0]), 10), expected[1..]);
    assert!(list(expected.last().copied(), 1).is_empty());
}

#[tokio::test]
async fn enumerate_scoped_resources_in_key_order_and_preserve_definition_metadata() {
    let f = Fixture::new();
    for id in ["z", "a"] {
        f.provision(id).await;
        f.core.store.create_registration(id).unwrap();
    }
    let channel = f.core.store.connect("a", u64::MAX).unwrap();
    let mut second_volume = Fixture::volume("a");
    second_volume.volume_id = "scratch".into();
    second_volume.quota_bytes = 128;
    f.core.create_volume(&second_volume).await.unwrap();
    f.core
        .import_image("a", "alias", &Fixture::image())
        .await
        .unwrap();
    let mut inherited = Fixture::definition("a", "worker", AccessMode::ReadOnly);
    inherited.command = None;
    inherited.image_reference = "alias".into();
    f.core.define_sandbox(&inherited).await.unwrap();
    f.core.delete_sandbox(&inherited.key).await.unwrap();
    inherited.key.generation_id = "g2".into();
    f.core.define_sandbox(&inherited).await.unwrap();
    let mut explicit = Fixture::definition("a", "cue", AccessMode::ReadOnly);
    explicit.entrypoint = Some(vec![]);
    explicit.command = Some(vec!["serve".into(), "--port=80".into()]);
    explicit.workdir = Some("".into());
    explicit.user = Some("1000:1000".into());
    explicit.log_limit_bytes = u64::MAX;
    explicit.host_shares = vec![HostShareMount {
        share_id: "src".into(),
        guest_path: "/work".into(),
        read_only: true,
    }];
    f.core.define_sandbox(&explicit).await.unwrap();
    let vm = f.core.start(&explicit.key).await.unwrap();
    let mut scoped_out = explicit.clone();
    scoped_out.key.scope_id = "z".into();
    scoped_out.key.sandbox_id = "private".into();
    f.core.define_sandbox(&scoped_out).await.unwrap();
    let removed = Fixture::definition("a", "removed", AccessMode::ReadOnly);
    f.core.define_sandbox(&removed).await.unwrap();
    f.core.delete_sandbox(&removed.key).await.unwrap();
    drop(f.core);
    let core = Core::new(
        Arc::new(Store::open(f.directory.path().join("host.db")).unwrap()),
        f.backend,
    );
    let store = &core.store;
    check_pages(&["a", "z"], |after, n| {
        store
            .list_registrations(after, n)
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect()
    });
    assert_eq!(
        store.list_registrations(None, 1).unwrap()[0].connection_epoch,
        channel.connection_epoch
    );
    check_pages(&["a", "z"], |after, n| {
        store
            .list_scopes(after, n)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect()
    });
    check_pages(&["alias", "cue"], |after, n| {
        store
            .list_images("a", after, n)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    });
    let images = store.list_images("a", None, 2).unwrap();
    assert_eq!(images[0].1, store.resolve_image("a", "alias").unwrap());
    assert_eq!(images[0].1, images[1].1);
    check_pages(&["data", "scratch"], |after, n| {
        store
            .list_volumes("a", after, n)
            .unwrap()
            .into_iter()
            .map(|v| v.volume_id)
            .collect()
    });
    assert_eq!(
        store.list_volumes("z", None, 2).unwrap(),
        [Fixture::volume("z")]
    );
    assert_eq!(store.list_images("z", None, 2).unwrap().len(), 1);
    check_pages(&["cue", "worker"], |after, n| {
        store
            .list_sandboxes("a", after, n)
            .unwrap()
            .into_iter()
            .map(|s| s.definition.key.sandbox_id)
            .collect()
    });
    let current = store.list_sandboxes("a", None, 2).unwrap();
    assert_eq!(current[0].vm.as_ref(), Some(&vm));
    assert_eq!(current[0].definition, explicit);
    assert_eq!(current[1].definition, inherited);
}
