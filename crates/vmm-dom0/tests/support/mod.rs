use vmm_core::*;
pub fn scope(id: &str) -> Scope {
    Scope {
        id: id.into(),
        origin: Origin::Local,
        generation: u64::MAX,
        revision: 1,
        limits: Limits {
            pids_max: 2048,
            disk_quota_bytes: 1 << 30,
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
        quota_bytes: 4 << 20,
        used_bytes: 0,
        owner_uid: 1000,
        owner_gid: 1000,
        mode: 0o700,
        key_id: Some("key".into()),
    }
}
pub fn definition(scope: &str, id: &str, mode: AccessMode) -> SandboxDefinition {
    SandboxDefinition {
        pids_max: Some(23),
        key: SandboxKey {
            scope_id: scope.into(),
            sandbox_id: id.into(),
            generation_id: "g1".into(),
        },
        manifest_digest: [7; 32],
        read_only_root: false,
        root_overlay: Some(RootOverlay {
            extent_id: format!("root-{id}"),
            quota_bytes: 4 << 20,
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
