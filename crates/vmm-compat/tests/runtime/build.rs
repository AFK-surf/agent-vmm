use super::*;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};
use vmm_compat::image::build::{BuildBackend, BuildGuest, GuestLease};
use vmm_guest_proto::{
    Secret,
    client::Client,
    frame::{ClientFrame, ServerFrame, read_frame, write_frame},
    rpc as g,
};
use vmm_rpc::Status;

pub struct Builder(SocketAddr, Arc<AtomicUsize>);
struct Lease(Arc<AtomicUsize>);
#[async_trait]
impl GuestLease for Lease {
    async fn close(self: Box<Self>) -> Result<(), Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl BuildBackend for Builder {
    async fn acquire(
        &self,
        _: &str,
        scope: &Scope,
        network: EgressMode,
        _: u64,
    ) -> Result<BuildGuest, Status> {
        assert_eq!(scope.id, "env");
        assert_eq!(network, EgressMode::DenyAll);
        Ok(BuildGuest {
            client: Client::new(self.0, Secret::new("builder")),
            instance_id: "isolated".into(),
            lease: Box::new(Lease(self.1.clone())),
        })
    }
    async fn settle(&self, _: &str) -> Result<(), Status> {
        Ok(())
    }
}
pub async fn builder() -> (Arc<Builder>, tokio::task::JoinHandle<()>) {
    let socket = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let task = tokio::spawn(async move {
        for step in 0..5 {
            let (io, _) = socket.accept().await.unwrap();
            tokio::task::spawn_blocking(move || {
                let mut io = io.into_std().unwrap();
                io.set_nonblocking(false).unwrap();
                let Some(ClientFrame::Hello(hello)) = read_frame(&mut io).unwrap() else {
                    panic!("guest hello required")
                };
                assert_eq!(hello.token, Secret::new("builder"));
                let reply = match hello.call {
                    g::Call::WorkspaceWrite(q) => {
                        let mut bytes = Vec::new();
                        loop {
                            match read_frame(&mut io).unwrap() {
                                Some(ClientFrame::Data(v)) => bytes.extend(v),
                                Some(ClientFrame::End) => break,
                                _ => panic!("unexpected frame"),
                            }
                        }
                        if step == 1 {
                            assert_eq!(q.target.path, "secret-test");
                            assert_eq!(bytes, b"private");
                        } else {
                            assert_eq!(q.target.path, "context.tar");
                        }
                        let hash = Sha256::digest(&bytes).into();
                        g::Reply::WorkspaceWrite(g::WorkspaceWriteReply {
                            entry: g::WorkspaceEntry {
                                path: q.target.path,
                                kind: g::EntryKind::File,
                                size: bytes.len() as u64,
                                mode: q.mode,
                                modified_unix_nanos: 0,
                                sha256: Some(hash),
                            },
                            sha256: hash,
                            root_used_bytes: None,
                        })
                    }
                    g::Call::Exec(q) => {
                        assert_eq!(q.command.user.as_deref(), Some("1000:1000"));
                        if step == 3 {
                            assert_eq!(q.command.argv[0], "/usr/bin/buildctl-daemonless.sh");
                            assert!(
                                q.command
                                    .argv
                                    .contains(&"id=test,src=/workspace/secret-test".into())
                            );
                            write_frame(&mut io, &ServerFrame::Stdout(b"progress".to_vec()))
                                .unwrap();
                        }
                        g::Reply::Exec(g::ExecResult {
                            exit: g::ExitStatus {
                                code: 0,
                                signal: None,
                            },
                            timed_out: false,
                            stdout_truncated: false,
                            stderr_truncated: false,
                            oom_killed: false,
                            memory_peak_bytes: None,
                        })
                    }
                    g::Call::WorkspaceRead(q) => {
                        assert_eq!(q.target.path, "image.tar");
                        let (archive, _) = oci::fixture(b"expected", "arm64");
                        let size = archive.len() as u64;
                        let sha256 = Sha256::digest(&archive).into();
                        write_frame(&mut io, &ServerFrame::Data(archive)).unwrap();
                        g::Reply::WorkspaceRead(g::WorkspaceReadReply {
                            end_offset: size,
                            file_size: size,
                            sha256,
                        })
                    }
                    _ => panic!("unexpected guest call"),
                };
                write_frame(&mut io, &ServerFrame::Done(Ok(reply))).unwrap();
            })
            .await
            .unwrap();
        }
    });
    (Arc::new(Builder(addr, Default::default())), task)
}

pub async fn exercise(
    c: &mut AgentRuntimeServiceClient,
    instance: &str,
    deadline: i64,
    backend: &Builder,
) {
    use h::build_image_request::Event;
    let header = h::BuildImageHeader {
        request_id: "build-once".into(),
        dockerfile_path: "Dockerfile".into(),
        image: "example.com/build:v1".into(),
        network_mode: h::BuildNetworkMode::None as i32,
        secrets: vec![h::BuildSecret { id: "test".into() }],
        container_id: "worker".into(),
        expected_instance_id: instance.into(),
        execution_id: "build-exec".into(),
        execution_deadline_unix_nano: deadline,
        ..Default::default()
    };
    let frame = |event| h::BuildImageRequest { event: Some(event) };
    let mut tar = tar::Builder::new(vec![]);
    let content = b"FROM scratch\n";
    let mut member = tar::Header::new_ustar();
    member.set_mode(0o644);
    member.set_size(content.len() as u64);
    tar.append_data(&mut member, "Dockerfile", &content[..])
        .unwrap();
    let frames = [
        Event::Header(header.clone()),
        Event::Context(h::BuildContextChunk {
            data: tar.into_inner().unwrap(),
            r#final: true,
        }),
        Event::Secret(h::BuildSecretChunk {
            id: "test".into(),
            data: b"private".to_vec(),
            r#final: true,
        }),
    ]
    .map(frame);
    let mut stream = c
        .build_image(tokio_stream::iter(frames))
        .await
        .unwrap()
        .into_inner();
    let mut completed = None;
    while let Some(reply) = stream.message().await.unwrap() {
        if let Some(h::build_image_response::Event::Result(data)) = reply.event {
            completed = Some(data)
        }
    }
    let image = completed.unwrap();
    assert_eq!(
        image.digest,
        oci::fixture(b"expected", "arm64").1.manifest_digest
    );
    assert_eq!(backend.1.load(Ordering::SeqCst), 1);
    let list = c
        .list_images(h::ListImagesRequest::default())
        .await
        .unwrap()
        .into_inner()
        .images;
    let receipt = list
        .into_iter()
        .find(|i| i.reference == header.image)
        .unwrap()
        .build_receipt
        .unwrap();
    assert!(receipt.writer_settled);
    assert_eq!(receipt.runtime_instance_id, "isolated");
    let observe = h::ObserveImageOperationRequest {
        request_id: header.request_id.clone(),
        kind: h::ImageOperationKind::Build as i32,
        input_sha256: receipt.input_sha256.clone(),
        execution_id: header.execution_id.clone(),
        container_id: "worker".into(),
        expected_instance_id: instance.into(),
        ..Default::default()
    };
    assert_eq!(
        c.observe_image_operation(observe.clone())
            .await
            .unwrap()
            .into_inner()
            .receipt,
        Some(receipt)
    );
    let mut stale = observe.clone();
    stale.expected_instance_id.push('x');
    assert_eq!(
        c.observe_image_operation(stale).await.unwrap_err().code(),
        Code::Aborted
    );
    let mut replay = c
        .build_image(tokio_stream::iter([frame(Event::Header(header.clone()))]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        replay.message().await.unwrap().unwrap().event,
        Some(h::build_image_response::Event::Result(image))
    );
    assert_eq!(backend.1.load(Ordering::SeqCst), 1);

    // A client may keep the input stream open indefinitely after admission.
    let mut partial = header;
    partial.request_id = "cancel-upload".into();
    partial.execution_id = "cancel-exec".into();
    let (upload, rx) = tokio::sync::mpsc::channel(1);
    upload
        .send(frame(Event::Header(partial.clone())))
        .await
        .unwrap();
    let mut pending = c
        .build_image(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut observation = observe;
    observation.request_id = partial.request_id.clone();
    observation.execution_id = partial.execution_id.clone();
    // Stream creation is local. Observe admission before issuing a separate cancel RPC.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match c.observe_image_operation(observation.clone()).await {
                Ok(response) => {
                    assert_eq!(
                        response.into_inner().receipt.unwrap().state(),
                        h::ImageOperationState::Running
                    );
                    break;
                }
                Err(status) if status.code() == Code::NotFound => tokio::task::yield_now().await,
                Err(status) => panic!("could not observe build admission: {status}"),
            }
        }
    })
    .await
    .unwrap();
    let cancelled = c
        .cancel_image_operation(h::CancelImageOperationRequest {
            request_id: partial.request_id,
            kind: observation.kind,
            execution_id: partial.execution_id,
            container_id: observation.container_id.clone(),
            expected_instance_id: observation.expected_instance_id.clone(),
            input_sha256: observation.input_sha256.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .receipt
        .unwrap();
    assert_eq!(cancelled.state(), h::ImageOperationState::Running);
    let err = tokio::time::timeout(std::time::Duration::from_secs(5), pending.message())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), Code::Cancelled);
    let settled = c
        .observe_image_operation(observation)
        .await
        .unwrap()
        .into_inner()
        .receipt
        .unwrap();
    assert_eq!(settled.state(), h::ImageOperationState::Stopped);
    assert!(settled.writer_settled);
    assert_eq!(backend.1.load(Ordering::SeqCst), 1);
    drop(upload);
}
