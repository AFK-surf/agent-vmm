use std::sync::Arc;
use tempfile::TempDir;
use vmm_core::{fake::FakeDom0Backend, *};

pub struct Fixture {
    pub core: Core,
    pub backend: Arc<FakeDom0Backend>,
    pub directory: TempDir,
}
impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path().join("host.db")).unwrap());
        let backend = Arc::new(FakeDom0Backend::default());
        Self {
            core: Core::new(store, backend.clone()),
            backend,
            directory,
        }
    }
    pub fn scope(id: &str) -> Scope {
        Scope {
            id: id.into(),
            origin: Origin::Local,
            generation: u64::MAX,
            revision: 1,
            limits: Limits {
                pids_max: 2048,
                disk_quota_bytes: 8192,
                vcpus: 2,
                boot_ram_mb: 128,
                max_ram_mb: 1024,
            },
            egress_mode: EgressMode::DenyAll,
            host_socket_rules: vec![],
            state: ScopeState::Active,
        }
    }
    pub fn volume(id: &str) -> Volume {
        Volume {
            scope_id: id.into(),
            volume_id: "data".into(),
            quota_bytes: 4096,
            used_bytes: 0,
            owner_uid: 1000,
            owner_gid: 1000,
            mode: 0o700,
            key_id: Some("key".into()),
        }
    }
    pub fn image() -> ImageSource {
        ImageSource {
            manifest_digest: [7; 32],
            staged_extent: "staged".into(),
            physical_bytes: 1024,
        }
    }
    pub fn definition(scope: &str, id: &str, mode: AccessMode) -> SandboxDefinition {
        SandboxDefinition {
            key: SandboxKey {
                scope_id: scope.into(),
                sandbox_id: id.into(),
                generation_id: "g1".into(),
            },
            manifest_digest: [7; 32],
            read_only_root: false,
            root_overlay: Some(RootOverlay {
                extent_id: format!("root-{id}"),
                quota_bytes: 128,
                used_bytes: 0,
            }),
            mounts: vec![Mount {
                volume_id: "data".into(),
                guest_path: "/data".into(),
                access: mode,
            }],
            image_reference: "cue".into(),
            host_shares: vec![],
            entrypoint: None,
            command: Some(vec!["cue".into()]),
            workdir: None,
            user: None,
            log_limit_bytes: 4096,
            env: Default::default(),
        }
    }
    pub async fn provision(&self, id: &str) {
        self.core
            .store
            .create_scope(&Self::scope(id), None)
            .unwrap();
        self.core.create_volume(&Self::volume(id)).await.unwrap();
        self.core
            .import_image(id, "cue", &Self::image())
            .await
            .unwrap();
    }
}
