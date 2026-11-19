use std::sync::Arc;
use tempfile::tempdir;
use vmm_core::*;

#[test]
fn replacement_epoch_fences_commands_without_revoking_accepted_scope_or_session() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.db");
    let store = Store::open(&path).unwrap();
    store.create_registration("reg").unwrap();
    let old = store.connect("reg", i64::MAX as u64 + 1).unwrap();
    let scope = Scope {
        id: "alloc".into(),
        origin: Origin::Remote {
            registration_id: "reg".into(),
        },
        generation: u64::MAX,
        revision: 1,
        limits: Limits {
            pids_max: 64,
            disk_quota_bytes: 1024,
            vcpus: 1,
            boot_ram_mb: 64,
            max_ram_mb: 128,
        },
        egress_mode: EgressMode::PublicInternet,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    };
    store.create_scope(&scope, Some(&old)).unwrap();
    let (session, token) = store.issue_session("alloc", Some(&old)).unwrap();
    let new = store.connect("reg", u64::MAX - 1).unwrap();
    assert!(matches!(
        store.issue_session("alloc", Some(&old)),
        Err(Error::Fenced)
    ));
    assert!(matches!(store.disconnect(&old), Err(Error::Fenced)));
    assert!(matches!(
        store.accept_command(&old, "alloc", u64::MAX),
        Err(Error::Fenced)
    ));
    assert_eq!(
        store.accept_command(&new, "alloc", u64::MAX).unwrap(),
        scope
    );
    store.set_admission("reg", true).unwrap();
    assert_eq!(
        store.authenticate_session(&session.id, &token).unwrap(),
        session
    );
    let mut other = scope.clone();
    other.id = "second".into();
    assert!(matches!(
        store.create_scope(&other, Some(&new)),
        Err(Error::Draining)
    ));
    let (replacement, new_token) = store.issue_session("alloc", Some(&new)).unwrap();
    assert!(matches!(
        store.authenticate_session(&session.id, &token),
        Err(Error::Fenced)
    ));
    drop(store);
    let store = Store::open(&path).unwrap();
    assert!(matches!(
        store.accept_command(&new, "alloc", u64::MAX),
        Err(Error::Fenced)
    ));
    let latest = store.connect("reg", u64::MAX).unwrap();
    store
        .transition_scope("alloc", 1, ScopeState::Draining, Some(&latest))
        .unwrap();
    assert_eq!(
        store
            .authenticate_session(&replacement.id, &new_token)
            .unwrap(),
        replacement
    );
    store
        .transition_scope("alloc", 2, ScopeState::Retained, Some(&latest))
        .unwrap();
    assert!(matches!(
        store.authenticate_session(&replacement.id, &new_token),
        Err(Error::Fenced)
    ));
    assert!(matches!(
        store.transition_scope("alloc", 2, ScopeState::Active, Some(&latest)),
        Err(Error::Fenced)
    ));
    assert_eq!(
        store
            .transition_scope("alloc", 3, ScopeState::Released, Some(&latest))
            .unwrap()
            .revision,
        4
    );
}

#[test]
fn receipt_concurrent_acceptance_replay_conflict_and_exact_reset_fence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.db");
    let store = Arc::new(Store::open(&path).unwrap());
    let key = RequestKey {
        domain: "install",
        scope_id: "org",
        request_id: "one",
    };
    let accepted = std::thread::scope(|threads| {
        let readers = (0..8)
            .map(|_| threads.spawn(|| store.begin_operation(key, b"plan").unwrap()))
            .collect::<Vec<_>>();
        readers
            .into_iter()
            .map(|r| r.join().unwrap())
            .filter(|r| matches!(r, BeginOperation::Accepted))
            .count()
    });
    assert_eq!(accepted, 1);
    store
        .finish_operation(key, b"plan", ReceiptState::Succeeded, b"receipt-v1")
        .unwrap();
    assert!(matches!(
        store.begin_operation(key, b"other"),
        Err(Error::Conflict(_))
    ));
    let retry = RequestKey {
        request_id: "two",
        ..key
    };
    store.begin_operation(retry, b"second").unwrap();
    store
        .finish_operation(retry, b"second", ReceiptState::Retryable, b"unavailable")
        .unwrap();
    store
        .fence_reset_requests(&[ResetFence { key: retry }])
        .unwrap();
    let interrupted = RequestKey {
        request_id: "unfinished",
        ..key
    };
    store.begin_operation(interrupted, b"start").unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert!(matches!(
        store.begin_operation(interrupted, b"start").unwrap(),
        BeginOperation::Replay(OperationReceipt {
            state: ReceiptState::Interrupted,
            ..
        })
    ));
    let missing = RequestKey {
        request_id: "planned",
        ..key
    };
    store
        .fence_reset_requests(&[ResetFence { key: missing }])
        .unwrap();
    assert_eq!(
        store.receipt(missing).unwrap().unwrap().state,
        ReceiptState::Fenced
    );
    assert!(matches!(
        store.begin_operation(missing, b"late"),
        Err(Error::Fenced)
    ));
    let receipt = store.receipt(key).unwrap().unwrap();
    assert_eq!(receipt.response, b"receipt-v1");
    assert_eq!(
        store.begin_operation(key, b"plan").unwrap(),
        BeginOperation::Replay(receipt)
    );
    assert_eq!(
        store.receipt(retry).unwrap().unwrap().state,
        ReceiptState::Fenced
    );
    assert!(matches!(
        store.finish_operation(retry, b"second", ReceiptState::Succeeded, b"late"),
        Err(Error::Fenced)
    ));
    assert!(matches!(
        store.begin_operation(retry, b"second"),
        Err(Error::Fenced)
    ));
    assert_eq!(
        store
            .begin_operation(
                RequestKey {
                    domain: "setup",
                    ..retry
                },
                b"second"
            )
            .unwrap(),
        BeginOperation::Accepted
    );
    assert_eq!(
        store
            .begin_operation(
                RequestKey {
                    scope_id: "elsewhere",
                    ..retry
                },
                b"second"
            )
            .unwrap(),
        BeginOperation::Accepted
    );
}

#[test]
fn appliance_and_registration_commit_or_roll_back_together() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let first = store.install_appliance("app", "private").unwrap();
    assert_eq!(
        store.registration("private").unwrap().id,
        first.registration_id
    );
    assert!(store.install_appliance("app", "other").is_err());
    assert!(matches!(
        store.registration("other"),
        Err(Error::Missing(_))
    ));
    assert!(matches!(
        store.transition_appliance(
            "app",
            "other",
            ApplianceState::Installed,
            ApplianceState::Active
        ),
        Err(Error::Fenced)
    ));
    store
        .transition_appliance(
            "app",
            "private",
            ApplianceState::Installed,
            ApplianceState::Active,
        )
        .unwrap();
    store
        .transition_appliance(
            "app",
            "private",
            ApplianceState::Active,
            ApplianceState::Failed,
        )
        .unwrap();
}
