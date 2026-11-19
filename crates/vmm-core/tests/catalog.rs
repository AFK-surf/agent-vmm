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

#[tokio::test]
async fn image_copy_grants_shared_content_once_and_checks_each_allocation_on_replay() {
    let f = Fixture::new();
    let store = &f.core.store;
    store.create_registration("reg").unwrap();
    let channel = store.connect("reg", 1).unwrap();
    for id in ["source", "target"] {
        let mut scope = Fixture::scope(id);
        scope.origin = Origin::Remote {
            registration_id: "reg".into(),
        };
        store.create_scope(&scope, Some(&channel)).unwrap();
    }
    let original = f
        .core
        .import_image("source", "app", &Fixture::image())
        .await
        .unwrap();
    let (session, _) = store.issue_session("target", Some(&channel)).unwrap();
    let auth = CopyAuthority {
        session: &session,
        registration: "reg",
        target: "target",
        generation: u64::MAX,
        source: &CopySource::Allocation {
            id: "source".into(),
            generation: u64::MAX,
        },
    };
    let copy = |fingerprint, digest| {
        store.copy_image_grant(
            &auth,
            "copy",
            fingerprint,
            "app",
            "docker.io/library/app:latest",
            digest,
        )
    };
    assert!(matches!(
        copy([0; 32], [1; 32]).await,
        Err(Error::Conflict("copy digest"))
    ));
    assert!(store.resolve_image("target", "app").is_err());
    let first = copy([2; 32], original.manifest_digest).await.unwrap();
    assert_eq!(store.resolve_image("target", "app").unwrap(), original);
    assert_eq!(
        f.core
            .remove_image_reference("source", "app", original.manifest_digest)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        copy([2; 32], original.manifest_digest)
            .await
            .unwrap()
            .digest,
        first.digest
    );
    assert!(matches!(
        copy([3; 32], original.manifest_digest).await,
        Err(Error::Conflict("request fingerprint"))
    ));
    let stale = CopyAuthority {
        generation: 1,
        ..auth
    };
    assert!(matches!(
        store.observe_image_copy(&stale, "copy", [2; 32]),
        Err(Error::Fenced)
    ));
    store.revoke_session(&session.id).unwrap();
    assert!(matches!(
        store.observe_image_copy(&auth, "copy", [2; 32]),
        Err(Error::Fenced)
    ));
    drop(f.core);
    let reopened = Store::open(f.directory.path().join("host.db")).unwrap();
    let fresh_channel = reopened.connect("reg", 2).unwrap();
    let (session, _) = reopened
        .issue_session("target", Some(&fresh_channel))
        .unwrap();
    let fresh = CopyAuthority {
        session: &session,
        ..auth
    };
    assert_eq!(
        reopened
            .observe_image_copy(&fresh, "copy", [2; 32])
            .unwrap()
            .digest,
        original.manifest_digest
    );
}

#[tokio::test]
async fn retained_builds_pin_the_extent_independently_and_only_the_owner_can_copy_them() {
    let f = Fixture::new();
    let store = &f.core.store;
    let mut sessions = vec![];
    for (reg, env) in [("owner", "builder"), ("other", "stranger")] {
        store.create_registration(reg).unwrap();
        let channel = store.connect(reg, 1).unwrap();
        let mut scope = Fixture::scope(env);
        scope.origin = Origin::Remote {
            registration_id: reg.into(),
        };
        store.create_scope(&scope, Some(&channel)).unwrap();
        sessions.push(store.issue_session(env, Some(&channel)).unwrap().0);
    }
    let digest = f
        .core
        .import_image("builder", "app", &Fixture::image())
        .await
        .unwrap()
        .manifest_digest;
    for id in ["first", "second"] {
        store
            .retain_build_output(
                "owner",
                &sessions[0],
                &RetainedImage {
                    reference: "app".into(),
                    digest,
                    generation: id.into(),
                    build_request: id.into(),
                    build_receipt: id.as_bytes().to_vec(),
                    logical_bytes: 5,
                },
            )
            .unwrap();
    }
    assert_eq!(
        f.core
            .remove_image_reference("builder", "app", digest)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .list_retained_images("owner", &sessions[0])
            .unwrap()
            .len(),
        2
    );
    assert!(matches!(
        store.list_retained_images("owner", &sessions[1]),
        Err(Error::Fenced)
    ));
    let source = CopySource::Retained {
        build_request: "second".into(),
    };
    let auth = CopyAuthority {
        session: &sessions[1],
        registration: "other",
        target: "stranger",
        generation: u64::MAX,
        source: &source,
    };
    assert!(matches!(
        store
            .copy_image_grant(&auth, "copy", [4; 32], "app", "app", digest)
            .await,
        Err(Error::Missing("retained image"))
    ));
    let owner = CopyAuthority {
        session: &sessions[0],
        registration: "owner",
        target: "builder",
        ..auth
    };
    store
        .copy_image_grant(&owner, "copy", [4; 32], "app", "app", digest)
        .await
        .unwrap();
    f.core
        .remove_retained_image("owner", &sessions[0], "app", "app", "second", digest)
        .await
        .unwrap();
    assert_eq!(
        store.list_retained_images("owner", &sessions[0]).unwrap()[0].generation,
        "first"
    );
    assert_eq!(
        store
            .resolve_image("builder", "app")
            .unwrap()
            .manifest_digest,
        digest
    );
}
