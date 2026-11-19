mod support;
use std::sync::Arc;
use support::Fixture;
use vmm_core::*;

#[tokio::test]
async fn whole_registration_disposal_fences_offline_sessions_and_resumes_after_restart() {
    let f = Fixture::new();
    let store = &f.core.store;
    f.provision("unrelated").await;
    store.create_registration("external").unwrap();
    let channel = store.connect("external", 8).unwrap();
    let mut sessions = Vec::new();
    for id in ["first", "second"] {
        let mut scope = Fixture::scope(id);
        scope.origin = Origin::Remote {
            registration_id: "external".into(),
        };
        store.create_scope(&scope, Some(&channel)).unwrap();
        f.core
            .import_image(id, "cue", &Fixture::image())
            .await
            .unwrap();
        f.core.create_volume(&Fixture::volume(id)).await.unwrap();
        let mut hidden = Fixture::volume(id);
        hidden.volume_id = "__default_workspace".into();
        f.core.create_volume(&hidden).await.unwrap();
        let sandbox = Fixture::definition(id, "worker", AccessMode::ReadWrite);
        f.core.define_sandbox(&sandbox).await.unwrap();
        if id == "first" {
            f.core.start(&sandbox.key).await.unwrap();
        }
        sessions.push(store.issue_session(id, Some(&channel)).unwrap());
    }
    store.disconnect(&channel).unwrap();
    store
        .begin_operator_registration_disposal("external", "dispose")
        .unwrap();
    let stopped = Fixture::definition("second", "worker", AccessMode::ReadWrite);
    assert!(matches!(
        f.core.start(&stopped.key).await,
        Err(Error::Draining)
    ));
    let owner = store.registration("external").unwrap();
    assert!(!owner.connected && owner.draining);
    assert_eq!(owner.connection_epoch, 9);
    for (session, capability) in sessions {
        assert!(matches!(
            store.authenticate_session(&session.id, &capability),
            Err(Error::Fenced)
        ));
    }
    assert!(matches!(
        store.remote_inventory(&channel, 32),
        Err(Error::Fenced)
    ));
    assert!(matches!(store.connect("external", 100), Err(Error::Fenced)));
    assert!(matches!(
        store.set_admission("external", false),
        Err(Error::Fenced)
    ));
    drop(f.core);
    let core = Core::new(
        Arc::new(Store::open(f.directory.path().join("host.db")).unwrap()),
        f.backend,
    );
    for _ in 0..2 {
        core.dispose_operator_registration("external", "dispose")
            .await
            .unwrap();
    }
    assert_eq!(
        core.store
            .registration("external")
            .unwrap()
            .connection_epoch,
        9
    );
    for id in ["first", "second"] {
        assert_eq!(core.store.scope(id).unwrap().state, ScopeState::Released);
        assert!(core.store.list_sandboxes(id, None, 32).unwrap().is_empty());
        assert!(core.store.list_volumes(id, None, 32).unwrap().is_empty());
        assert!(core.store.list_images(id, None, 32).unwrap().is_empty());
    }
    assert!(core.store.resolve_image("unrelated", "cue").is_ok());
    assert!(matches!(
        core.store
            .begin_operator_registration_disposal("external", "different"),
        Err(Error::Conflict(_))
    ));
}

#[test]
fn missing_private_and_conflicting_registration_disposals_have_no_side_effects() {
    let f = Fixture::new();
    let store = &f.core.store;
    assert!(matches!(
        store.begin_operator_registration_disposal("missing", "op"),
        Err(Error::Missing("registration"))
    ));
    store.install_appliance("appliance", "private").unwrap();
    store.connect("private", 1).unwrap();
    assert!(matches!(
        store.begin_operator_registration_disposal("private", "op"),
        Err(Error::Conflict("private appliance registration"))
    ));
    assert!(store.registration("private").unwrap().connected);
    assert!(store.check_external_connector("private").is_err());
    store.create_registration("external").unwrap();
    let channel = store.connect("external", 3).unwrap();
    let mut scope = Fixture::scope("child");
    scope.origin = Origin::Remote {
        registration_id: "external".into(),
    };
    store.create_scope(&scope, Some(&channel)).unwrap();
    let (session, capability) = store.issue_session("child", Some(&channel)).unwrap();
    // A previously accepted remote disposal can survive interruption before release.
    let conn = rusqlite::Connection::open(f.directory.path().join("host.db")).unwrap();
    conn.execute(
        "UPDATE scopes SET disposal_request='prior' WHERE id='child'",
        [],
    )
    .unwrap();
    assert!(matches!(
        store.begin_operator_registration_disposal("external", "different"),
        Err(Error::Conflict(_))
    ));
    let owner = store.registration("external").unwrap();
    assert!(owner.connected && !owner.draining);
    store.check_external_connector("external").unwrap();
    assert_eq!(owner.connection_epoch, 3);
    assert!(store.authenticate_session(&session.id, &capability).is_ok());
    store
        .begin_operator_registration_disposal("external", "prior")
        .unwrap();
    assert!(matches!(
        store.check_external_connector("external"),
        Err(Error::Draining)
    ));
}

#[tokio::test]
async fn local_environment_disposal_is_revision_bound_and_rejects_remote_children() {
    let f = Fixture::new();
    f.provision("owned").await;
    let store = &f.core.store;
    let (session, capability) = store.issue_session("owned", None).unwrap();
    assert!(matches!(
        store.begin_operator_environment_disposal("owned", 2, "op"),
        Err(Error::Fenced)
    ));
    assert!(store.authenticate_session(&session.id, &capability).is_ok());
    store
        .begin_operator_environment_disposal("owned", 1, "op")
        .unwrap();
    assert!(matches!(
        store.issue_session("owned", None),
        Err(Error::Draining)
    ));
    assert!(matches!(
        store.transition_scope("owned", 1, ScopeState::Active, None),
        Err(Error::Fenced)
    ));
    for _ in 0..2 {
        f.core
            .dispose_operator_environment("owned", 1, "op")
            .await
            .unwrap();
    }
    assert_eq!(store.scope("owned").unwrap().state, ScopeState::Released);
    assert!(store.list_volumes("owned", None, 10).unwrap().is_empty());
    store.create_registration("external").unwrap();
    let channel = store.connect("external", 1).unwrap();
    let mut scope = Fixture::scope("remote");
    scope.origin = Origin::Remote {
        registration_id: "external".into(),
    };
    store.create_scope(&scope, Some(&channel)).unwrap();
    assert!(matches!(
        store.begin_operator_environment_disposal("remote", 1, "op"),
        Err(Error::Conflict(_))
    ));
    assert_eq!(store.scope("remote").unwrap().state, ScopeState::Active);
}
