use crate::{
    Connector, Error,
    remote::{
        self, Allocation, CapacityDimension, CommandOutcome, CommandResult, ControllerCommand,
        ErrorReason, command_result, controller_command::Command,
    },
    sessions::Sessions,
};
use vmm_core::{Channel, Origin, RemoteAllocation, RemoteScopePolicy, Scope, ScopeState};

pub(crate) async fn execute(
    connector: &Connector,
    channel: &Channel,
    command: &ControllerCommand,
    sessions: &mut Sessions<'_>,
) -> CommandResult {
    let result = match command.command.as_ref().expect("validated control command") {
        Command::EnsureAllocation(request) => return ensure(connector, channel, request),
        Command::OpenSession(request) => sessions
            .open(&command.command_id, request)
            .map(command_result::Result::SessionReady),
        Command::ReleaseAllocation(request) => {
            return retire(
                connector,
                channel,
                sessions,
                &request.allocation_id,
                request.expected_generation,
                request.expected_revision,
                None,
            )
            .await;
        }
        Command::DisposeAllocation(request) => {
            return retire(
                connector,
                channel,
                sessions,
                &request.allocation_id,
                request.expected_generation,
                request.expected_revision,
                Some(&request.request_id),
            )
            .await;
        }
        _ => return failure(ErrorReason::InvalidArgument, CommandOutcome::Rejected),
    };
    match result {
        Ok(value) => success(value),
        Err(Error::Core(error)) => core_failure(error),
        Err(_) => failure(ErrorReason::Unavailable, CommandOutcome::Unknown),
    }
}

fn ensure(
    connector: &Connector,
    channel: &Channel,
    request: &remote::EnsureAllocation,
) -> CommandResult {
    let limits = request.limits.as_ref().expect("validated resource limits");
    let (Some(pids), Some(disk_bytes)) = (limits.pids, limits.disk_bytes) else {
        return failure(ErrorReason::InvalidArgument, CommandOutcome::Rejected);
    };
    if pids == 0 || disk_bytes == 0 {
        return failure(ErrorReason::InvalidArgument, CommandOutcome::Rejected);
    }
    let egress_mode = match request.egress_mode() {
        remote::EgressMode::DenyAll => vmm_core::EgressMode::DenyAll,
        remote::EgressMode::PublicInternet => vmm_core::EgressMode::PublicInternet,
        _ => return failure(ErrorReason::InvalidArgument, CommandOutcome::Rejected),
    };
    let desired = Scope {
        id: String::new(),
        origin: Origin::Remote {
            registration_id: channel.registration_id.clone(),
        },
        generation: request.generation,
        revision: request.policy_revision,
        limits: vmm_core::Limits {
            pids_max: u64::from(pids),
            disk_quota_bytes: disk_bytes,
            ..connector.config.scope_defaults.clone()
        },
        egress_mode,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    };
    let mut label = request.display_label.trim();
    if label.len() > 256 {
        label = &label[..label.floor_char_boundary(256)];
    }
    let pool = &connector.config.connection.pool_policy;
    let policy = RemoteScopePolicy {
        revision: connector.config.connection.policy_revision,
        max_pids: u64::from(pool.per_environment_limits.pids),
        max_disk_quota_bytes: pool.per_environment_limits.disk_bytes,
        max_egress_mode: match pool.max_egress_mode {
            crate::EgressMode::DenyAll => vmm_core::EgressMode::DenyAll,
            crate::EgressMode::PublicInternet => vmm_core::EgressMode::PublicInternet,
        },
    };
    match connector.core.store.ensure_remote_scope(
        channel,
        &request.allocation_id,
        label,
        desired,
        &policy,
    ) {
        Ok(allocation) => success(command_result::Result::Allocation(allocation_proto(
            allocation,
        ))),
        Err(error) => core_failure(error),
    }
}

async fn retire(
    connector: &Connector,
    channel: &Channel,
    sessions: &mut Sessions<'_>,
    id: &str,
    generation: u64,
    revision: u64,
    disposal_request: Option<&str>,
) -> CommandResult {
    match connector.core.store.remote_allocation(channel, id) {
        Ok(allocation) => {
            if allocation.scope.generation != generation {
                return failure(ErrorReason::StaleRevision, CommandOutcome::Rejected);
            }
            if allocation.scope.revision != revision {
                if disposal_request.is_some() {
                    let mut result = failure(
                        ErrorReason::DisposalRevisionNotAccepted,
                        CommandOutcome::Rejected,
                    );
                    result.result = Some(command_result::Result::Allocation(allocation_proto(
                        allocation,
                    )));
                    return result;
                }
                return failure(ErrorReason::StaleRevision, CommandOutcome::Rejected);
            }
        }
        Err(error) => return core_failure(error),
    }
    if sessions.close(id).is_err() {
        return failure(ErrorReason::Unavailable, CommandOutcome::Unknown);
    }
    match connector
        .core
        .retire_remote_scope(channel, id, generation, revision, disposal_request)
        .await
    {
        Ok(allocation) => success(command_result::Result::Allocation(allocation_proto(
            allocation,
        ))),
        Err(error) => core_failure(error),
    }
}

pub(crate) fn allocation_proto(allocation: RemoteAllocation) -> Allocation {
    let scope = allocation.scope;
    Allocation {
        allocation_id: allocation.allocation_id,
        display_label: allocation.display_label,
        limits: Some(remote::ResourceLimits {
            pids: u32::try_from(scope.limits.pids_max).ok(),
            disk_bytes: Some(scope.limits.disk_quota_bytes),
        }),
        egress_mode: match scope.egress_mode {
            vmm_core::EgressMode::DenyAll => remote::EgressMode::DenyAll.into(),
            vmm_core::EgressMode::PublicInternet => remote::EgressMode::PublicInternet.into(),
        },
        revision: scope.revision,
        state: match scope.state {
            ScopeState::Active => remote::AllocationState::Ready.into(),
            ScopeState::Draining => remote::AllocationState::Draining.into(),
            ScopeState::Retained => remote::AllocationState::Retained.into(),
            ScopeState::Released => remote::AllocationState::Discarded.into(),
        },
        generation: scope.generation,
    }
}

pub(crate) fn success(value: command_result::Result) -> CommandResult {
    CommandResult {
        outcome: CommandOutcome::Succeeded.into(),
        result: Some(value),
        ..Default::default()
    }
}

pub(crate) fn failure(reason: ErrorReason, outcome: CommandOutcome) -> CommandResult {
    CommandResult {
        outcome: outcome.into(),
        reason: reason.into(),
        ..Default::default()
    }
}

fn capacity_failure(dimension: CapacityDimension) -> CommandResult {
    CommandResult {
        capacity_dimension: dimension.into(),
        ..failure(ErrorReason::CapacityExhausted, CommandOutcome::Rejected)
    }
}

fn core_failure(error: vmm_core::Error) -> CommandResult {
    use ErrorReason as Reason;
    use vmm_core::Error as Core;
    let reason = match error {
        Core::Missing(_) => Reason::AllocationNotFound,
        Core::Invalid(_) => Reason::InvalidArgument,
        Core::Fenced => Reason::StaleRevision,
        Core::Conflict(_) => Reason::PolicyConflict,
        Core::Draining => Reason::AdmissionPaused,
        Core::Capacity(vmm_core::CapacityDimension::Pids) => {
            return capacity_failure(CapacityDimension::Pids);
        }
        Core::Capacity(vmm_core::CapacityDimension::StorageHeadroom) => {
            return capacity_failure(CapacityDimension::StorageHeadroom);
        }
        Core::Capacity(_) => return capacity_failure(CapacityDimension::Unspecified),
        Core::Interrupted => return failure(Reason::Unavailable, CommandOutcome::Unknown),
        _ => return failure(Reason::Internal, CommandOutcome::Unknown),
    };
    failure(reason, CommandOutcome::Rejected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConnectorConfig, EgressMode, Gateway, PoolPolicy, RemoteConnection, ResourceLimits,
    };
    use std::sync::Arc;
    use vmm_core::{Core, Limits, Store, fake::FakeDom0Backend};
    use vmm_signing::FileCredentialStore;

    #[tokio::test]
    async fn ensure_replays_admitted_spec_after_pool_policy_changes() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path().join("host.db")).unwrap());
        let registration = "registration";
        store.create_registration(registration).unwrap();
        let channel = store.connect(registration, 1).unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        use base64::{Engine, engine::general_purpose::STANDARD};
        let config = RemoteConnection {
            gateway_endpoint: "localhost:1".into(),
            trust_bundle: STANDARD.encode(cert.cert.pem()),
            policy_revision: 7,
            pool_policy: PoolPolicy {
                per_environment_limits: ResourceLimits {
                    pids: 64,
                    disk_bytes: 2048,
                },
                max_egress_mode: EgressMode::PublicInternet,
                stop_grace_seconds: 30,
            },
        };
        let gateway = Gateway::new(
            registration,
            &config,
            Arc::new(FileCredentialStore::new(
                directory.path().join("credentials"),
            )),
        )
        .unwrap();
        let mut connector = Connector::new(
            Core::new(store.clone(), Arc::new(FakeDom0Backend::default())),
            gateway,
            ConnectorConfig {
                connection: config,
                scope_defaults: Limits {
                    pids_max: 0,
                    disk_quota_bytes: 0,
                    vcpus: 1,
                    boot_ram_mb: 128,
                    max_ram_mb: 128,
                },
                runtime: None,
                observe: None,
            },
        );
        let mut request = remote::EnsureAllocation {
            allocation_id: "existing".into(),
            display_label: "Original".into(),
            limits: Some(remote::ResourceLimits {
                pids: Some(32),
                disk_bytes: Some(1024),
            }),
            egress_mode: remote::EgressMode::PublicInternet.into(),
            policy_revision: 7,
            generation: 1,
        };
        let admitted = ensure(&connector, &channel, &request);
        assert_eq!(admitted.outcome(), CommandOutcome::Succeeded);
        let watermark = store.remote_inventory(&channel, 0).unwrap().watermark;

        connector.config.connection.policy_revision = 8;
        connector.config.connection.pool_policy.max_egress_mode = EgressMode::DenyAll;
        connector
            .config
            .connection
            .pool_policy
            .per_environment_limits = ResourceLimits {
            pids: 16,
            disk_bytes: 512,
        };
        connector.config.scope_defaults.vcpus = 2;
        request.display_label = "Already admitted".into();

        let replay = ensure(&connector, &channel, &request);
        assert_eq!(replay, admitted);
        assert_eq!(
            store.remote_inventory(&channel, 0).unwrap().watermark,
            watermark
        );

        store.set_admission(registration, true).unwrap();
        assert_eq!(
            ensure(&connector, &channel, &request).reason(),
            ErrorReason::AdmissionPaused
        );
        store.set_admission(registration, false).unwrap();

        request.allocation_id = "new".into();
        assert_eq!(
            ensure(&connector, &channel, &request).reason(),
            ErrorReason::StaleRevision
        );
        request.policy_revision = 8;
        request.egress_mode = remote::EgressMode::DenyAll.into();
        assert_eq!(
            ensure(&connector, &channel, &request).capacity_dimension(),
            CapacityDimension::StorageHeadroom
        );
        request.limits.as_mut().unwrap().disk_bytes = Some(512);
        assert_eq!(
            ensure(&connector, &channel, &request).capacity_dimension(),
            CapacityDimension::Pids
        );
        assert!(matches!(
            store.remote_allocation(&channel, "new"),
            Err(vmm_core::Error::Missing(_))
        ));
    }
}
