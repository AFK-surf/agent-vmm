use agent_vmm_proto::host::v1::{
    ImportImageHeader, ImportImageRequest, import_image_request::Payload,
};
#[path = "image/fixture.rs"]
mod oci;
use oci::fixture;
use std::path::Path;
use vmm_rpc::{Code, Status};
use vmm_compat::image::VerifiedArchive;

struct NoopDriver;
#[async_trait::async_trait]
impl vmm_compat::RuntimeDriver for NoopDriver {
    async fn authorize_shares(
        &self,
        _: &str,
        _: &[vmm_core::HostShareMount],
    ) -> Result<(), Status> {
        Ok(())
    }
    async fn health(
        &self,
        _: &vmm_core::BootIdentity,
    ) -> Result<vmm_guest_proto::rpc::HealthReply, Status> {
        Err(Status::unimplemented("unused"))
    }
}

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
    use vmm_compat::details::BadRequest;
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

#[tokio::test]
async fn reentrant_import_retries_storage_failure_with_cached_part_and_reports_headroom() {
    use prost::Message;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use vmm_compat::{
        Containers, RuntimeContext,
        details::ErrorInfo,
        image::{ImageImports, ImageStaging},
    };
    use vmm_core::{
        CapacityDimension, Core, EgressMode, Error, ImageSource, Limits, Origin, ReceiptState,
        RequestKey, Scope, ScopeState, Store, fake::FakeDom0Backend,
    };

    struct FlakyStage {
        calls: AtomicUsize,
    }
    impl ImageStaging for FlakyStage {
        fn stage_image(&self, _: &Path, manifest: [u8; 32]) -> vmm_core::Result<ImageSource> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                return Err(Error::Capacity(CapacityDimension::StorageHeadroom));
            }
            Ok(ImageSource {
                manifest_digest: manifest,
                staged_extent: format!("token-{n}"),
                physical_bytes: 4096,
            })
        }
        fn discard_staged_image(&self, _: &str) -> vmm_core::Result<()> {
            Ok(())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host.db")).unwrap());
    let backend = Arc::new(FakeDom0Backend::default());
    let core = Core::new(store.clone(), backend);
    store
        .create_scope(
            &Scope {
                id: "scope-a".into(),
                generation: 1,
                origin: Origin::Local,
                revision: 1,
                limits: Limits {
                    pids_max: 100,
                    disk_quota_bytes: 1 << 20,
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
    let (session, _) = store.issue_session("scope-a", None).unwrap();
    let containers = Containers::open(dir.path().join("compat.db"), Arc::new(NoopDriver)).unwrap();
    let stage = Arc::new(FlakyStage {
        calls: AtomicUsize::new(0),
    });
    let staging_dir = dir.path().join("image-staging");
    std::fs::create_dir_all(staging_dir.join("parts")).unwrap();
    let imports = ImageImports::new(staging_dir.clone(), stage.clone());

    let (bytes, mut header) = fixture(b"expected", "arm64");
    header.request_id = "deterministic-req-1".into();
    header.archive_url = "https://example.invalid/shell.oci.tar".into();
    // Seed cached `.part` to simulate an already-downloaded archive (0 network fetch on retry).
    let part_file = staging_dir
        .join("parts")
        .join(format!("{}.part", hex::encode(&header.archive_sha256)));
    std::fs::write(&part_file, &bytes).unwrap();

    let make_ctx = || {
        let mut req = vmm_rpc::Request::new(());
        req.extensions_mut().insert(session.clone());
        RuntimeContext::from_request(&core, &req).unwrap()
    };
    let make_stream = || {
        tokio_stream::iter([Ok(ImportImageRequest {
            payload: Some(Payload::Header(header.clone())),
        })])
    };

    // Attempt 1: storage headroom failure -> import_headroom metadata + Retryable receipt.
    let err = imports
        .receive(make_ctx(), containers.clone(), make_stream())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    let rich = agent_vmm_proto::google::rpc::Status::decode(err.details()).unwrap();
    let info = ErrorInfo::decode(rich.details[0].value.as_slice()).unwrap();
    assert_eq!(info.domain, "agent-vmm");
    assert_eq!(info.reason, "RESOURCE_CAPACITY_EXHAUSTED");
    assert_eq!(
        info.metadata.get("stage").map(String::as_str),
        Some("import_admission")
    );
    assert_eq!(
        info.metadata.get("resource_dimension").map(String::as_str),
        Some("storage_headroom")
    );
    assert!(info.metadata["available_bytes"].parse::<u64>().is_ok());
    assert!(info.metadata["required_bytes"].parse::<u64>().is_ok());

    let receipt = store
        .receipt(RequestKey {
            domain: "runtime-import-image",
            scope_id: "scope-a",
            request_id: "deterministic-req-1",
        })
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, ReceiptState::Retryable);
    assert!(part_file.exists(), "retryable failure must keep cached .part");

    // Attempt 2: same request_id resumes without re-downloading.
    let ok = imports
        .receive(make_ctx(), containers.clone(), make_stream())
        .await
        .unwrap();
    assert_eq!(ok.image.unwrap().digest, header.manifest_digest);
    assert_eq!(stage.calls.load(Ordering::SeqCst), 2);
    assert!(!part_file.exists(), "successful import should clean .part");
    let receipt = store
        .receipt(RequestKey {
            domain: "runtime-import-image",
            scope_id: "scope-a",
            request_id: "deterministic-req-1",
        })
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, ReceiptState::Succeeded);
}

#[tokio::test]
async fn concurrent_running_import_returns_operation_in_progress_and_executes_once() {
    use prost::Message;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use vmm_compat::{
        Containers, RuntimeContext,
        details::ErrorInfo,
        image::{ImageImports, ImageStaging},
    };
    use vmm_core::{
        Core, EgressMode, ImageSource, Limits, Origin, Scope, ScopeState, Store,
        fake::FakeDom0Backend,
    };

    struct BlockingStage {
        calls: AtomicUsize,
        gate: Mutex<Option<(tokio::sync::oneshot::Sender<()>, mpsc::Receiver<()>)>>,
    }
    impl ImageStaging for BlockingStage {
        fn stage_image(&self, _: &Path, manifest: [u8; 32]) -> vmm_core::Result<ImageSource> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = self.gate.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = release.recv();
            }
            Ok(ImageSource {
                manifest_digest: manifest,
                staged_extent: "tok".into(),
                physical_bytes: 4096,
            })
        }
        fn discard_staged_image(&self, _: &str) -> vmm_core::Result<()> {
            Ok(())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host.db")).unwrap());
    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    store
        .create_scope(
            &Scope {
                id: "scope-run".into(),
                generation: 1,
                origin: Origin::Local,
                revision: 1,
                limits: Limits {
                    pids_max: 100,
                    disk_quota_bytes: 1 << 20,
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
    let (session, _) = store.issue_session("scope-run", None).unwrap();
    let containers = Containers::open(dir.path().join("compat.db"), Arc::new(NoopDriver)).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let stage = Arc::new(BlockingStage {
        calls: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered_tx, release_rx))),
    });
    let staging_dir = dir.path().join("image-staging");
    std::fs::create_dir_all(staging_dir.join("parts")).unwrap();
    let imports = ImageImports::new(staging_dir.clone(), stage.clone());

    let (bytes, mut header) = fixture(b"expected", "arm64");
    header.request_id = "in-flight-req".into();
    header.archive_url = "https://example.invalid/station.oci.tar".into();
    let part_file = staging_dir
        .join("parts")
        .join(format!("{}.part", hex::encode(&header.archive_sha256)));
    std::fs::write(&part_file, &bytes).unwrap();

    let make_ctx = || {
        let mut req = vmm_rpc::Request::new(());
        req.extensions_mut().insert(session.clone());
        RuntimeContext::from_request(&core, &req).unwrap()
    };
    let make_stream = || {
        tokio_stream::iter([Ok(ImportImageRequest {
            payload: Some(Payload::Header(header.clone())),
        })])
    };

    let imports_first = imports.clone();
    let containers_first = containers.clone();
    let ctx_first = make_ctx();
    let stream_first = make_stream();
    let first = tokio::spawn(async move {
        imports_first
            .receive(ctx_first, containers_first, stream_first)
            .await
    });
    entered_rx.await.unwrap();

    // Concurrent retry while first is Running -> OPERATION_IN_PROGRESS.
    let err = imports
        .receive(make_ctx(), containers.clone(), make_stream())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    let rich = agent_vmm_proto::google::rpc::Status::decode(err.details()).unwrap();
    let info = ErrorInfo::decode(rich.details[0].value.as_slice()).unwrap();
    assert_eq!(info.domain, "agent-vmm");
    assert_eq!(info.reason, "OPERATION_IN_PROGRESS");
    assert_eq!(
        info.metadata.get("stage").map(String::as_str),
        Some("image_import")
    );

    release_tx.send(()).unwrap();
    assert!(first.await.unwrap().is_ok());
    assert_eq!(stage.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_failed_host_store_failure_receipt_recovers_on_preserve_upgrade_and_same_request_id()
{
    use agent_vmm_proto::google::rpc;
    use prost::Message;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use vmm_compat::{
        Containers, RuntimeContext,
        image::{ImageImports, ImageStaging},
    };
    use vmm_core::{
        BeginOperation, Core, EgressMode, ImageSource, Limits, Origin, ReceiptState, RequestKey,
        Scope, ScopeState, Store, fake::FakeDom0Backend,
    };

    struct CountingStage {
        calls: AtomicUsize,
    }
    impl ImageStaging for CountingStage {
        fn stage_image(&self, _: &Path, manifest: [u8; 32]) -> vmm_core::Result<ImageSource> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ImageSource {
                manifest_digest: manifest,
                staged_extent: format!("token-{n}"),
                physical_bytes: 4096,
            })
        }
        fn discard_staged_image(&self, _: &str) -> vmm_core::Result<()> {
            Ok(())
        }
    }
    fn key(request_id: &str) -> RequestKey<'_> {
        RequestKey {
            domain: "runtime-import-image",
            scope_id: "scope-legacy",
            request_id,
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host.db")).unwrap());
    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    store
        .create_scope(
            &Scope {
                id: "scope-legacy".into(),
                generation: 1,
                origin: Origin::Local,
                revision: 1,
                limits: Limits {
                    pids_max: 100,
                    disk_quota_bytes: 1 << 20,
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
    let (session, _) = store.issue_session("scope-legacy", None).unwrap();
    let containers = Containers::open(dir.path().join("compat.db"), Arc::new(NoopDriver)).unwrap();
    let stage = Arc::new(CountingStage {
        calls: AtomicUsize::new(0),
    });
    let staging_dir = dir.path().join("image-staging");
    std::fs::create_dir_all(staging_dir.join("parts")).unwrap();
    let imports = ImageImports::new(staging_dir.clone(), stage.clone());

    // The bytes that the previous `mutate` stored for `Status::internal("host store failure")`.
    let legacy_bytes = hex::decode("080d1212686f73742073746f7265206661696c757265").unwrap();
    let legacy = rpc::Status::decode(legacy_bytes.as_slice()).unwrap();
    assert_eq!(legacy.code, Code::Internal as i32);
    assert_eq!(legacy.message, "host store failure");
    let invalid_bytes = rpc::Status {
        code: Code::InvalidArgument as i32,
        message: "invalid archive".into(),
        details: vec![],
    }
    .encode_to_vec();

    let (bytes, inline) = fixture(b"expected", "arm64");
    let part_file = staging_dir
        .join("parts")
        .join(format!("{}.part", hex::encode(&inline.archive_sha256)));
    let request = |request_id: &str, archive_url: &str| ImportImageHeader {
        request_id: request_id.into(),
        archive_url: archive_url.into(),
        ..inline.clone()
    };
    let direct = request("legacy-direct-req-1", "");
    let upgrade = request(
        "legacy-upgrade-req-1",
        "https://example.invalid/shell.oci.tar",
    );
    let invalid = request("invalid-arg-req-1", "https://example.invalid/shell.oci.tar");
    for (header, response) in [
        (&direct, &legacy_bytes),
        (&upgrade, &legacy_bytes),
        (&invalid, &invalid_bytes),
    ] {
        let mut canonical = header.clone();
        canonical.archive_url.clear();
        let input = canonical.encode_to_vec();
        let key = key(&header.request_id);
        assert_eq!(
            store.begin_operation(key, &input).unwrap(),
            BeginOperation::Accepted
        );
        store
            .finish_operation(key, &input, ReceiptState::Failed, response)
            .unwrap();
    }
    let state = |request_id: &str| store.receipt(key(request_id)).unwrap().unwrap().state;
    let receive = |header: &ImportImageHeader| {
        let mut req = vmm_rpc::Request::new(());
        req.extensions_mut().insert(session.clone());
        let ctx = RuntimeContext::from_request(&core, &req).unwrap();
        let chunks = if header.archive_url.is_empty() {
            bytes.chunks(247).collect()
        } else {
            vec![]
        };
        let input = std::iter::once(Payload::Header(header.clone()))
            .chain(
                chunks
                    .into_iter()
                    .map(|b| Payload::ArchiveChunk(b.to_owned())),
            )
            .map(|payload| {
                Ok(ImportImageRequest {
                    payload: Some(payload),
                })
            })
            .collect::<Vec<_>>();
        imports.receive(ctx, containers.clone(), tokio_stream::iter(input))
    };

    // Without the startup migration, the same request ID runs the inline import again.
    let ok = receive(&direct).await.unwrap();
    assert_eq!(ok.image.unwrap().digest, direct.manifest_digest);
    assert_eq!(stage.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state("legacy-direct-req-1"), ReceiptState::Succeeded);

    // The startup migration changes only the receipt that holds a non-deterministic error.
    assert_eq!(
        ImageImports::migrate_legacy_failed_receipts(&store).unwrap(),
        1
    );
    assert_eq!(state("legacy-upgrade-req-1"), ReceiptState::Retryable);
    assert_eq!(state("invalid-arg-req-1"), ReceiptState::Failed);

    std::fs::write(&part_file, &bytes).unwrap();
    let ok = receive(&upgrade).await.unwrap();
    assert_eq!(ok.image.unwrap().digest, upgrade.manifest_digest);
    assert_eq!(stage.calls.load(Ordering::SeqCst), 2);
    assert_eq!(state("legacy-upgrade-req-1"), ReceiptState::Succeeded);

    // A deterministic error replays and does not stage the archive.
    std::fs::write(&part_file, &bytes).unwrap();
    let err = receive(&invalid).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "invalid archive");
    assert_eq!(stage.calls.load(Ordering::SeqCst), 2);
    assert_eq!(state("invalid-arg-req-1"), ReceiptState::Failed);
}

#[tokio::test]
async fn verified_url_imports_rejects_forged_sha256_and_private_inline_imports() {
    use std::sync::Arc;
    use vmm_core::{
        Core, EgressMode, ImageSource, Limits, Origin, Scope, ScopeState, Store,
        fake::FakeDom0Backend,
    };

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host.db")).unwrap());
    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    for id in ["scope-a", "scope-b"] {
        store
            .create_scope(
                &Scope {
                    id: id.into(),
                    generation: 1,
                    origin: Origin::Local,
                    revision: 1,
                    limits: Limits {
                        pids_max: 100,
                        disk_quota_bytes: 1 << 20,
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
    }

    // 1. Private/inline import does NOT populate verified_url_imports.
    let private_digest = [0x11; 32];
    let private_sha = [0x22; 32];
    core.import_image(
        "scope-a",
        "private/image:latest",
        &ImageSource {
            manifest_digest: private_digest,
            staged_extent: "priv".into(),
            physical_bytes: 4096,
        },
    )
    .await
    .unwrap();
    assert!(
        core.try_grant_verified_url_import(
            "scope-b",
            "private/image:latest",
            private_digest,
            private_sha,
            1234,
        )
        .await
        .unwrap()
        .is_none()
    );

    // 2. Public verified URL import allows cross-scope grant ONLY on exact 3-tuple match.
    let pub_digest = [0x33; 32];
    let pub_sha = [0x44; 32];
    core.import_verified_url_image(
        "scope-a",
        "public/shell:v1",
        &ImageSource {
            manifest_digest: pub_digest,
            staged_extent: "pub".into(),
            physical_bytes: 8192,
        },
        pub_sha,
        9999,
    )
    .await
    .unwrap();

    // Forged archive_sha256 -> rejected.
    let mut forged_sha = pub_sha;
    forged_sha[0] ^= 1;
    assert!(
        core.try_grant_verified_url_import(
            "scope-b",
            "public/shell:v1",
            pub_digest,
            forged_sha,
            9999,
        )
        .await
        .unwrap()
        .is_none()
    );

    // Forged archive_size -> rejected.
    assert!(
        core.try_grant_verified_url_import(
            "scope-b",
            "public/shell:v1",
            pub_digest,
            pub_sha,
            9998,
        )
        .await
        .unwrap()
        .is_none()
    );

    // Exact 3-tuple match -> granted!
    let granted = core
        .try_grant_verified_url_import(
            "scope-b",
            "public/shell:v1",
            pub_digest,
            pub_sha,
            9999,
        )
        .await
        .unwrap()
        .expect("exact verified URL import should short-circuit across scopes");
    assert_eq!(granted.manifest_digest, pub_digest);
    assert_eq!(
        store
            .resolve_image("scope-b", "public/shell:v1")
            .unwrap()
            .manifest_digest,
        pub_digest
    );
}
