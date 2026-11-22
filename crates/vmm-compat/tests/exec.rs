use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;
use tokio_stream::{StreamExt, iter, pending};
use vmm_compat::{
    Containers, RuntimeContext, RuntimeDriver,
    guest::{Duplex, GuestDriver},
};
use vmm_core::{
    BootIdentity, Core, EgressMode, HostShareMount, ImageSource, Limits, Origin, Scope, ScopeState,
    Store, fake::FakeDom0Backend,
};
use vmm_guest_proto::{
    Secret,
    client::Client,
    frame::{ClientFrame, ServerFrame, read_frame, write_frame},
    rpc as g,
};
use vmm_rpc::{Request, Status};

struct MockGuest(SocketAddr);

#[async_trait]
impl GuestDriver for MockGuest {
    async fn client(&self, _: &BootIdentity) -> Result<Client, Status> {
        Ok(Client::new(self.0, Secret::new("fixture")))
    }
    async fn connect_port(&self, _: &BootIdentity, _: u16) -> Result<Box<dyn Duplex>, Status> {
        Err(Status::unavailable("no forwarded service"))
    }
}

#[async_trait]
impl RuntimeDriver for MockGuest {
    async fn authorize_shares(&self, _: &str, _: &[HostShareMount]) -> Result<(), Status> {
        Ok(())
    }
    async fn health(&self, boot: &BootIdentity) -> Result<g::HealthReply, Status> {
        Ok(g::HealthReply {
            instance_id: boot.instance_id.clone(),
            boot_token_proof: [0; 32],
            main: g::MainState::Idle,
            quiesced: false,
            uptime_ms: 0,
            orphans_reaped: 0,
            usage: Default::default(),
        })
    }
}

async fn setup_running_container(
    driver: Arc<MockGuest>,
) -> (tempfile::TempDir, RuntimeContext, Containers, String) {
    let dir = tempfile::tempdir().unwrap();
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("db")).unwrap()),
        Arc::new(FakeDom0Backend::default()),
    );
    core.store
        .create_scope(
            &Scope {
                id: "env".into(),
                origin: Origin::Local,
                generation: 1,
                revision: 1,
                limits: Limits {
                    pids_max: 64,
                    disk_quota_bytes: 4096,
                    vcpus: 1,
                    boot_ram_mb: 64,
                    max_ram_mb: 64,
                },
                egress_mode: EgressMode::DenyAll,
                host_socket_rules: vec![],
                state: ScopeState::Active,
            },
            None,
        )
        .unwrap();
    core.import_image(
        "env",
        "base",
        &ImageSource {
            manifest_digest: [7; 32],
            staged_extent: "fixture".into(),
            physical_bytes: 1,
        },
    )
    .await
    .unwrap();
    let mut req = Request::new(());
    req.extensions_mut()
        .insert(core.store.issue_session("env", None).unwrap().0);
    let ctx = RuntimeContext::from_request(&core, &req).unwrap();
    let containers = Containers::open(dir.path().join("protocol"), driver).unwrap();
    let made = containers
        .create(
            &ctx,
            h::CreateContainerRequest {
                request_id: "create".into(),
                container_id: "worker".into(),
                image: "base".into(),
                log_limit_bytes: 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .container
        .unwrap();
    let live = containers
        .start(
            &ctx,
            h::StartContainerRequest {
                request_id: "start".into(),
                container_id: "worker".into(),
                expected_generation_id: made.generation_id,
            },
        )
        .await
        .unwrap()
        .container
        .unwrap();
    (dir, ctx, containers, live.instance_id)
}

fn future_deadline() -> i64 {
    (SystemTime::now() + Duration::from_secs(60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

#[tokio::test]
async fn exec_forwards_timeout_ms_and_terminal_metadata() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let guest_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::task::spawn_blocking(move || {
            let mut socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            let Some(ClientFrame::Hello(hello)) = read_frame(&mut socket).unwrap() else {
                panic!("missing hello");
            };
            let g::Call::Exec(req) = hello.call else {
                panic!("expected exec");
            };
            assert_eq!(req.timeout_ms, 1500);
            write_frame(
                &mut socket,
                &ServerFrame::Done(Ok(g::Reply::Exec(g::ExecResult {
                    exit: g::ExitStatus {
                        code: 137,
                        signal: Some(9),
                    },
                    timed_out: true,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    oom_killed: true,
                    memory_peak_bytes: Some(33_554_432),
                }))),
            )
            .unwrap();
        })
        .await
        .unwrap();
    });

    let driver = Arc::new(MockGuest(addr));
    let (_dir, ctx, containers, instance) = setup_running_container(driver.clone()).await;

    let start = h::ExecRequest {
        event: Some(h::exec_request::Event::Start(h::ExecStart {
            request_id: "exec-timeout".into(),
            container_id: "worker".into(),
            argv: vec!["sleep".into(), "10".into()],
            expected_instance_id: instance.clone(),
            execution_id: "exec-timeout-1".into(),
            execution_kind: h::ExecutionKind::Exec as i32,
            execution_deadline_unix_nano: future_deadline(),
            timeout_ms: 1500,
            ..Default::default()
        })),
    };
    let mut out = containers
        .exec(&ctx, driver.as_ref(), iter([Ok(start)]))
        .await
        .unwrap();
    let reply = out.next().await.unwrap().unwrap();
    assert_eq!(reply.event, Some(h::exec_response::Event::ExitCode(137)));
    assert!(reply.timed_out);
    assert!(reply.oom_killed);
    assert_eq!(reply.memory_peak_bytes, Some(33_554_432));
    assert!(out.next().await.is_none());
    guest_task.await.unwrap();

    let execs = containers
        .executions(
            &ctx,
            h::ListExecutionsRequest {
                container_id: "worker".into(),
                expected_instance_id: instance,
            },
        )
        .unwrap();
    assert!(execs.executions.is_empty());
}

#[tokio::test]
async fn exec_cancels_guest_and_releases_lease_when_output_drops() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let cancel_done = Arc::new(Notify::new());
    let saw_cancel_srv = saw_cancel.clone();
    let cancel_done_srv = cancel_done.clone();

    let guest_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::task::spawn_blocking(move || {
            let mut socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let Some(ClientFrame::Hello(hello)) = read_frame(&mut socket).unwrap() else {
                panic!("missing hello");
            };
            assert!(matches!(hello.call, g::Call::Exec(_)));
            write_frame(&mut socket, &ServerFrame::Stdout(b"ready".to_vec())).unwrap();
            // Wait for host to cancel with Signal(Kill) or EOF.
            match read_frame(&mut socket) {
                Ok(Some(ClientFrame::Signal(g::Signal::Kill))) | Ok(None) | Err(_) => {
                    saw_cancel_srv.store(true, Ordering::Release);
                    let _ = write_frame(
                        &mut socket,
                        &ServerFrame::Done(Ok(g::Reply::Exec(g::ExecResult {
                            exit: g::ExitStatus {
                                code: 137,
                                signal: Some(9),
                            },
                            timed_out: false,
                            stdout_truncated: false,
                            stderr_truncated: false,
                            oom_killed: false,
                            memory_peak_bytes: None,
                        }))),
                    );
                }
                other => panic!("expected cancel signal or EOF, got {other:?}"),
            }
            cancel_done_srv.notify_one();
        })
        .await
        .unwrap();
    });

    let driver = Arc::new(MockGuest(addr));
    let (_dir, ctx, containers, instance) = setup_running_container(driver.clone()).await;

    let start = h::ExecRequest {
        event: Some(h::exec_request::Event::Start(h::ExecStart {
            request_id: "exec-cancel".into(),
            container_id: "worker".into(),
            argv: vec!["sleep".into(), "300".into()],
            expected_instance_id: instance.clone(),
            execution_id: "exec-cancel-1".into(),
            execution_kind: h::ExecutionKind::Exec as i32,
            execution_deadline_unix_nano: future_deadline(),
            ..Default::default()
        })),
    };
    let input = iter([Ok(start)]).chain(pending());
    let mut out = containers.exec(&ctx, driver.as_ref(), input).await.unwrap();
    let first = out.next().await.unwrap().unwrap();
    assert_eq!(
        first.event,
        Some(h::exec_response::Event::Stdout(b"ready".to_vec()))
    );

    // Lease is active while stream is open.
    let active = containers
        .executions(
            &ctx,
            h::ListExecutionsRequest {
                container_id: "worker".into(),
                expected_instance_id: instance.clone(),
            },
        )
        .unwrap();
    assert_eq!(active.executions.len(), 1);

    // Dropping the output stream cancels the guest execution and releases the lease.
    drop(out);
    tokio::time::timeout(Duration::from_secs(5), cancel_done.notified())
        .await
        .expect("guest did not observe cancel");
    assert!(saw_cancel.load(Ordering::Acquire));
    guest_task.await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let after = containers
            .executions(
                &ctx,
                h::ListExecutionsRequest {
                    container_id: "worker".into(),
                    expected_instance_id: instance.clone(),
                },
            )
            .unwrap();
        if after.executions.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution lease was not released after output stream dropped"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn exec_cancels_promptly_after_close_stdin_and_request_eof() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let saw_signal = Arc::new(AtomicBool::new(false));
    let cancel_done = Arc::new(Notify::new());
    let saw_signal_srv = saw_signal.clone();
    let cancel_done_srv = cancel_done.clone();

    let guest_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::task::spawn_blocking(move || {
            let mut socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let Some(ClientFrame::Hello(hello)) = read_frame(&mut socket).unwrap() else {
                panic!("missing hello");
            };
            assert!(matches!(hello.call, g::Call::Exec(_)));
            // Matches salix-vmm-worker: CloseStdin(true) followed by immediate request stream EOF.
            assert!(matches!(
                read_frame(&mut socket).unwrap(),
                Some(ClientFrame::End)
            ));
            write_frame(&mut socket, &ServerFrame::Stdout(b"running".to_vec())).unwrap();
            match read_frame(&mut socket) {
                Ok(Some(ClientFrame::Signal(g::Signal::Kill))) | Ok(None) | Err(_) => {
                    saw_signal_srv.store(true, Ordering::Release);
                    let _ = write_frame(
                        &mut socket,
                        &ServerFrame::Done(Ok(g::Reply::Exec(g::ExecResult {
                            exit: g::ExitStatus {
                                code: 137,
                                signal: Some(9),
                            },
                            timed_out: false,
                            stdout_truncated: false,
                            stderr_truncated: false,
                            oom_killed: false,
                            memory_peak_bytes: None,
                        }))),
                    );
                }
                other => panic!("expected cancel signal or EOF, got {other:?}"),
            }
            cancel_done_srv.notify_one();
        })
        .await
        .unwrap();
    });

    let driver = Arc::new(MockGuest(addr));
    let (_dir, ctx, containers, instance) = setup_running_container(driver.clone()).await;

    let frames = [
        h::ExecRequest {
            event: Some(h::exec_request::Event::Start(h::ExecStart {
                request_id: "exec-eof-cancel".into(),
                container_id: "worker".into(),
                argv: vec!["sleep".into(), "300".into()],
                expected_instance_id: instance.clone(),
                execution_id: "exec-eof-cancel-1".into(),
                execution_kind: h::ExecutionKind::Exec as i32,
                execution_deadline_unix_nano: future_deadline(),
                ..Default::default()
            })),
        },
        h::ExecRequest {
            event: Some(h::exec_request::Event::CloseStdin(true)),
        },
    ];
    let mut out = containers
        .exec(&ctx, driver.as_ref(), iter(frames.into_iter().map(Ok)))
        .await
        .unwrap();
    let first = out.next().await.unwrap().unwrap();
    assert_eq!(
        first.event,
        Some(h::exec_response::Event::Stdout(b"running".to_vec()))
    );
    // Give feed/writer time to finish after CloseStdin + request EOF.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let dropped_at = tokio::time::Instant::now();
    drop(out);
    tokio::time::timeout(Duration::from_millis(500), cancel_done.notified())
        .await
        .expect("guest did not observe cancel within 500ms");
    assert!(saw_signal.load(Ordering::Acquire));
    guest_task.await.unwrap();

    loop {
        let after = containers
            .executions(
                &ctx,
                h::ListExecutionsRequest {
                    container_id: "worker".into(),
                    expected_instance_id: instance.clone(),
                },
            )
            .unwrap();
        if after.executions.is_empty() {
            break;
        }
        assert!(
            dropped_at.elapsed() < Duration::from_millis(500),
            "execution lease was not released within 500ms after output stream dropped"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(dropped_at.elapsed() < Duration::from_millis(500));
}
