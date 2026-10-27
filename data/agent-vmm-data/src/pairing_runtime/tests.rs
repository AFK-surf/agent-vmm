use super::*;
use agent_vmm_data::{pairing::OwnedPairingContext, pairing_session::PAIRING_TTL};
use iroh::endpoint::presets;
use tokio::io::AsyncWriteExt;

fn context() -> OwnedPairingContext {
    OwnedPairingContext {
        session_id: vec![9; 16],
        mesh_id: b"mesh".to_vec(),
        invite_revision: 1,
        manager_identity: b"manager".to_vec(),
        manager_root_key: vec![2; 33],
        joiner_identity: b"joiner".to_vec(),
        joiner_root_key: vec![3; 33],
        roles_and_features: vec![1],
    }
}
fn session(context: OwnedPairingContext, now: Instant) -> PairingSession {
    PairingSession::new(
        context,
        agent_vmm_data::pairing::PairingSecret::from_pin(b"12345678").unwrap(),
        now,
    )
    .unwrap()
}
async fn endpoints() -> (Endpoint, Endpoint, Connection, Connection) {
    let manager = Endpoint::builder(presets::Minimal)
        .alpns(vec![agent_vmm_data::PAIRING_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let joiner = Endpoint::builder(presets::Minimal)
        .alpns(vec![agent_vmm_data::PAIRING_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let (peer, local) = tokio::join!(
        joiner.connect(manager.addr(), agent_vmm_data::PAIRING_ALPN),
        async { manager.accept().await.unwrap().await.unwrap() }
    );
    (manager, joiner, local, peer.unwrap())
}

#[tokio::test]
async fn status_remains_readable_during_confirmation_and_terminal_handles_expire() {
    let sessions = PairingSessions::default();
    let now = Instant::now() - PAIRING_TTL + Duration::from_millis(100);
    let session = session(context(), now);
    let (mut commands, publisher, mut status) =
        registry::register(&sessions, &session, PairingRole::Manager)
            .await
            .unwrap();
    let pending_sessions = sessions.clone();
    let pending = tokio::spawn(async move {
        registry::send_command(&pending_sessions, &[9; 16], PairingCommand::Confirm).await
    });
    let command = commands.recv().await.unwrap();
    let response = tokio::time::timeout(
        Duration::from_millis(20),
        registry::status_json(&sessions, &[9; 16]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response).unwrap()["state"],
        "invited"
    );
    let PairingCommand::Confirm(reply) = command else {
        panic!("confirm expected")
    };
    status.state = PairingSessionState::Rejected;
    publisher.send_replace(status);
    reply.send(Ok(())).unwrap();
    pending.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(sessions.lock().await.is_empty());
}

#[tokio::test]
async fn stalled_hello_and_pake_release_their_pairing_slots() {
    let hello_case = async {
        let (_manager, _joiner, connection, peer) = endpoints().await;
        let sessions = PairingSessions::default();
        let limit = Arc::new(Semaphore::new(1));
        let local_limit = limit.clone();
        let incoming = tokio::spawn(handle_incoming(connection, sessions, local_limit));
        let (mut send, _receive) = peer.open_bi().await.unwrap();
        send.write_all(&[0]).await.unwrap();
        let failure = tokio::time::timeout(
            PAIRING_UNAUTHENTICATED_TIMEOUT + Duration::from_secs(2),
            incoming,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
        assert!(failure.to_string().contains("hello timed out"));
        assert_eq!(limit.available_permits(), 1);
        tokio::time::timeout(Duration::from_secs(2), peer.closed())
            .await
            .unwrap();
    };
    let pake_case = async {
        let (_manager, _joiner, connection, peer) = endpoints().await;
        let sessions = PairingSessions::default();
        let limit = Arc::new(Semaphore::new(1));
        let mut invitation = context();
        invitation.joiner_identity.clear();
        invitation.joiner_root_key.clear();
        let manager_session = session(invitation, Instant::now());
        let (commands, publisher, mut status) =
            registry::register(&sessions, &manager_session, PairingRole::Manager)
                .await
                .unwrap();
        let mut observer = { sessions.lock().await.get(&[9; 16]).unwrap().status.clone() };
        tokio::spawn(async move {
            run_manager_task(
                manager_session,
                Zeroizing::new(b"acceptance".to_vec()),
                commands,
                publisher,
                &mut status,
            )
            .await;
        });
        let incoming = tokio::spawn(handle_incoming(connection, sessions.clone(), limit.clone()));
        let (mut send, _receive) = peer.open_bi().await.unwrap();
        let mut joiner_session = session(context(), Instant::now());
        let (share, _state) = joiner_session.begin_initiator(Instant::now()).unwrap();
        let hello =
            serde_json::to_vec(&serde_json::json!({"context":context(),"share":share})).unwrap();
        send.write_u32(hello.len() as u32).await.unwrap();
        send.write_all(&hello).await.unwrap();
        incoming.await.unwrap().unwrap();
        tokio::time::timeout(
            PAIRING_UNAUTHENTICATED_TIMEOUT + Duration::from_secs(2),
            async {
                loop {
                    if observer.borrow().last_failure == Some(PairingFailure::Timeout) {
                        break;
                    }
                    observer.changed().await.unwrap();
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(observer.borrow().state, PairingSessionState::Invited);
        assert_eq!(observer.borrow().attempts_remaining, 4);
        assert!(observer.borrow().peer_identity.is_none());
        assert_eq!(limit.available_permits(), 1);
        tokio::time::timeout(Duration::from_secs(2), peer.closed())
            .await
            .unwrap();
    };
    tokio::join!(hello_case, pake_case);
}

#[tokio::test]
async fn confirmed_peer_still_expires_and_releases_the_connection() {
    let (_manager, _joiner, connection, peer) = endpoints().await;
    let mut manager_session = session(context(), Instant::now());
    let mut joiner_session = session(context(), Instant::now());
    let (mut peer_send, mut peer_receive) = peer.open_bi().await.unwrap();
    // Write the hello before accepting the peer stream.
    let joiner = tokio::spawn(async move {
        let result = run_joiner(
            &mut peer_send,
            &mut peer_receive,
            &mut joiner_session,
            b"proposal",
            Instant::now(),
        )
        .await
        .unwrap();
        (peer_send, peer_receive, result)
    });
    let (mut send, mut receive) = connection.accept_bi().await.unwrap();
    let hello = agent_vmm_data::pairing_wire::receive_manager_hello(&mut receive)
        .await
        .unwrap();
    let mut result = agent_vmm_data::pairing_wire::run_manager_after_hello(
        &mut send,
        &mut receive,
        &mut manager_session,
        hello,
        b"acceptance",
        Instant::now(),
    )
    .await
    .unwrap();
    let (_peer_send, _peer_receive, _result) = joiner.await.unwrap();
    // Start the expiry window after the network handshake.
    let now = Instant::now();
    let mut manager_session = session(context(), now - PAIRING_TTL + Duration::from_millis(200));
    manager_session
        .begin_manager_attempt(&context(), now)
        .unwrap();
    manager_session.key_confirmed(now).unwrap();
    let (_commands, receiver) = mpsc::channel(1);
    let mut status = PairingStatus::new(&manager_session, PairingRole::Manager);
    let (publisher, observer) = watch::channel(status.clone());
    let channel = Channel {
        connection,
        send,
        receive,
        records: result.take_record_layer().unwrap(),
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        run_connected(
            &mut manager_session,
            channel,
            receiver,
            &publisher,
            &mut status,
        ),
    )
    .await
    .unwrap();
    assert_eq!(observer.borrow().state, PairingSessionState::Expired);
    tokio::time::timeout(Duration::from_secs(2), peer.closed())
        .await
        .unwrap();
}
