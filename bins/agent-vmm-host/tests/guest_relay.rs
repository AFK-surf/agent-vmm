#![cfg(target_os = "macos")]

use agent_vmm_host::carrier::CarrierClientAdapter;
use std::{collections::BTreeMap, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use vmm_carrier_macos::{
    client::CarrierClient,
    mock::MockCarrierHypervisor,
    protocol::{CONTROL_PORT, GUEST_PORT, Request},
    server::serve,
};
use vmm_compat::guest::GuestDriver;
use vmm_core::{BootIdentity, SandboxKey};
use vmm_guest_proto::{
    Secret,
    frame::{ClientFrame, ServerFrame, read_frame, write_frame},
    rpc::{Call, Reply},
};

async fn answer(stream: &mut UnixStream, reply: serde_json::Value) -> serde_json::Value {
    let length = stream.read_u32().await.unwrap();
    let mut buf = vec![0; length as usize];
    stream.read_exact(&mut buf).await.unwrap();
    let request: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    let buf = serde_json::to_vec(&reply).unwrap();
    stream.write_u32(buf.len() as u32).await.unwrap();
    stream.write_all(&buf).await.unwrap();
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_guest_call_rechecks_the_boot_after_the_relay_connects() {
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let path = |name: &str| dir.path().join(name);
    let control = UnixListener::bind(path("dom0")).unwrap();
    let rpc = UnixListener::bind(path("relay")).unwrap();
    let machine = MockCarrierHypervisor::new(BTreeMap::from([
        (CONTROL_PORT, path("dom0")),
        (GUEST_PORT, path("relay")),
    ]));
    let carrier = CarrierClient::new(path("carrier"));
    let server = tokio::spawn(serve(
        UnixListener::bind(path("carrier")).unwrap(),
        Arc::new(Mutex::new(machine)),
        std::future::pending(),
    ));
    carrier
        .control(Request::EnsureStarted {
            operation_id: "start".into(),
        })
        .await
        .unwrap();
    let boot = BootIdentity {
        sandbox: SandboxKey {
            scope_id: "scope".into(),
            sandbox_id: "workload".into(),
            generation_id: "gen".into(),
        },
        instance_id: "one".into(),
        boot_token_hash: [1; 32],
    };
    let client = CarrierClientAdapter(carrier).client(&boot).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let control_server = tokio::spawn(async move {
        for pass in 0..2 {
            let (mut s, _) = control.accept().await.unwrap();
            assert_eq!(
                answer(&mut s, serde_json::json!({"Ok":"10.77.0.2:8181"})).await,
                serde_json::json!([1,{"GuestAddress":boot}])
            );
            rx.recv().await.unwrap();
            let (mut s, _) = control.accept().await.unwrap();
            let reply = if pass == 0 {
                serde_json::json!({"Ok":{"address":"10.77.0.2:8181","token":"test-only"}})
            } else {
                serde_json::json!({"Err":"Fenced"})
            };
            assert_eq!(
                answer(&mut s, reply).await,
                serde_json::json!([1,{"OpenGuestRuntime":boot}])
            );
        }
    });
    let relay = tokio::spawn(async move {
        for pass in 0..2 {
            let (mut stream, _) = rpc.accept().await.unwrap();
            let mut tuple = [0u8; 6];
            stream.read_exact(&mut tuple).await.unwrap();
            assert_eq!(tuple, [10, 77, 0, 2, 0x1f, 0xf5]);
            stream.write_u8(0).await.unwrap();
            tx.send(()).await.unwrap();
            if pass == 1 {
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
                continue;
            }
            let mut stream = stream.into_std().unwrap();
            stream.set_nonblocking(false).unwrap();
            tokio::task::spawn_blocking(move || {
                let Some(ClientFrame::Hello(hello)) = read_frame(&mut stream).unwrap() else {
                    panic!("missing guest handshake")
                };
                assert_eq!(hello.token, Secret::new("test-only"));
                assert!(matches!(hello.call, Call::ProcessList));
                write_frame(
                    &mut stream,
                    &ServerFrame::Done(Ok(Reply::ProcessList(Vec::new()))),
                )
                .unwrap();
            })
            .await
            .unwrap();
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::task::spawn_blocking(move || {
            assert!(client.process_list().unwrap().is_empty());
            assert!(client.process_list().is_err());
        })
        .await
        .unwrap();
        relay.await.unwrap();
        control_server.await.unwrap();
    })
    .await
    .unwrap();
    server.abort();
}
