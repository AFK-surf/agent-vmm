use agent_vmm_proto::host::v1::{
    ImportImageHeader, ImportImageRequest, import_image_request::Payload,
};
#[path = "image/fixture.rs"]
mod oci;
use oci::fixture;
use std::path::Path;
use tonic::{Code, Status};
use vmm_compat::image::VerifiedArchive;

async fn receive(
    dir: &Path,
    header: ImportImageHeader,
    bytes: &[u8],
) -> Result<VerifiedArchive, Status> {
    let input = std::iter::once(Payload::Header(header))
        .chain(
            bytes
                .chunks(247)
                .map(|b| Payload::ArchiveChunk(b.to_owned())),
        )
        .map(|payload| {
            Ok(ImportImageRequest {
                payload: Some(payload),
            })
        });
    VerifiedArchive::receive(dir, tokio_stream::iter(input)).await
}

#[tokio::test]
async fn import_binds_archive_platform_manifest_and_every_layer_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let (bytes, header) = fixture(b"expected", "arm64");
    let good = receive(dir.path(), header.clone(), &bytes).await.unwrap();
    assert_eq!(std::fs::read(good.path()).unwrap(), bytes);
    assert_eq!(
        format!("sha256:{}", hex::encode(good.manifest_digest())),
        header.manifest_digest
    );
    drop(good);
    for (bytes, header) in [
        fixture(b"modified", "arm64"),
        fixture(b"expected", "amd64"),
        {
            let mut other = header.clone();
            other.manifest_digest = oci::sha(b"another manifest");
            (bytes.clone(), other)
        },
        {
            let mut other = header;
            other.archive_sha256[0] ^= 1;
            (bytes, other)
        },
    ] {
        assert_eq!(
            receive(dir.path(), header, &bytes)
                .await
                .err()
                .unwrap()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn idle_import_frees_staging_and_reports_invalid_fields() {
    use prost::Message;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic_types::pb::BadRequest;
    let dir = tempfile::tempdir().unwrap();
    let (_, header) = fixture(b"expected", "arm64");
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    sender
        .send(Ok(ImportImageRequest {
            payload: Some(Payload::Header(header.clone())),
        }))
        .await
        .unwrap();
    let err = VerifiedArchive::receive(dir.path(), ReceiverStream::new(receiver))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    let err = receive(
        dir.path(),
        ImportImageHeader {
            archive_size: 0,
            ..header
        },
        &[],
    )
    .await
    .err()
    .unwrap();
    let rich = agent_vmm_proto::google::rpc::Status::decode(err.details()).unwrap();
    assert_eq!(rich.code, Code::InvalidArgument as i32);
    let detail = &rich.details[0];
    assert_eq!(detail.type_url, "type.googleapis.com/google.rpc.BadRequest");
    assert_eq!(
        BadRequest::decode(detail.value.as_slice())
            .unwrap()
            .field_violations[0]
            .field,
        "header"
    );
}

#[tokio::test]
async fn staging_tokens_are_released_on_install_cache_hit_error_and_cancellation() {
    use std::sync::{Arc, Mutex, mpsc};
    use vmm_compat::image::ImageStaging;
    use vmm_core::{
        Core, EgressMode, ImageSource, Limits, Origin, Scope, ScopeState, Store,
        fake::FakeDom0Backend,
    };

    struct Stage {
        // Separate the backend side effect from the future's next poll.
        gate: Mutex<Option<(tokio::sync::oneshot::Sender<()>, mpsc::Receiver<()>)>>,
        discarded: tokio::sync::mpsc::UnboundedSender<String>,
    }
    impl ImageStaging for Stage {
        fn stage_image(&self, path: &Path, digest: [u8; 32]) -> vmm_core::Result<ImageSource> {
            assert!(path.is_file());
            if let Some((ready, unblock)) = self.gate.lock().unwrap().take() {
                ready.send(()).unwrap();
                unblock.recv().unwrap();
            }
            Ok(ImageSource {
                manifest_digest: digest,
                staged_extent: "owned-token".into(),
                physical_bytes: 4096,
            })
        }
        fn discard_staged_image(&self, token: &str) -> vmm_core::Result<()> {
            self.discarded.send(token.into()).unwrap();
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("core.db")).unwrap()),
        Arc::new(FakeDom0Backend::default()),
    );
    core.store
        .create_scope(
            &Scope {
                id: "scope".into(),
                generation: 1,
                origin: Origin::Local,
                revision: 1,
                limits: Limits {
                    pids_max: 100,
                    disk_quota_bytes: 8192,
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
    let (discarded, mut tokens) = tokio::sync::mpsc::unbounded_channel();
    let stage = Arc::new(Stage {
        gate: Mutex::default(),
        discarded,
    });
    let (bytes, header) = fixture(b"expected", "arm64");
    for scope in ["scope", "scope", "absent"] {
        let archive = receive(dir.path(), header.clone(), &bytes).await.unwrap();
        let result = archive.install(&core, scope, stage.clone()).await;
        assert_eq!(result.is_ok(), scope == "scope");
        assert_eq!(tokens.try_recv().unwrap(), "owned-token");
        assert!(tokens.try_recv().is_err());
    }
    let archive = receive(dir.path(), header, &bytes).await.unwrap();
    let (ready, started) = tokio::sync::oneshot::channel();
    let (unblock, gate) = mpsc::channel();
    *stage.gate.lock().unwrap() = Some((ready, gate));
    let task = tokio::spawn(async move { archive.install(&core, "scope", stage).await });
    started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    unblock.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), tokens.recv())
            .await
            .unwrap()
            .as_deref(),
        Some("owned-token")
    );
}
