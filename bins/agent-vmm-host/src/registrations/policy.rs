//! Translate the public operator policy to the persisted Core policy and Salix dial configuration.
use agent_vmm_proto::host::v1 as h;
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{net::IpAddr, time::SystemTime};
use vmm_rpc::Status;
use vmm_core::{
    EgressMode, ManagedConnection, ManagedPhase, ManagedPool, ManagedRegistration,
    RemoteAllocation, RemoteInventory, ScopeState,
};
use vmm_remote_salix as salix;

pub(super) fn parse(
    req: h::RemoteRegistrationConnection,
    max_disk: u64,
) -> Result<ManagedConnection, Status> {
    use vmm_compat::error::invalid_field;
    if req.registration_id.is_empty() || req.request_id.is_empty() || req.request_id.len() > 256 {
        return Err(invalid_field(
            "registration/request_id",
            "registration identity and request ID are required",
        ));
    }
    let host = req.endpoint.rsplit_once(':').map(|(h, _)| h).unwrap_or("");
    if host.is_empty()
        || host.len() > 253
        || host.starts_with('[')
        || host.parse::<IpAddr>().is_ok()
    {
        return Err(invalid_field(
            "endpoint",
            "expected a TLS DNS name and port",
        ));
    }
    let pool = req
        .pool
        .ok_or_else(|| invalid_field("pool", "complete pool policy is required"))?;
    let limits = pool
        .per_environment_limits
        .as_ref()
        .ok_or_else(|| invalid_field("per_environment_limits", "PID and disk limits required"))?;
    let pids = limits
        .pids
        .filter(|v| (1..=2048).contains(v))
        .ok_or_else(|| {
            invalid_field(
                "per_environment_limits.pids",
                "PID limit exceeds host capacity",
            )
        })?;
    let disk_bytes = limits
        .disk_bytes
        .filter(|v| *v > 0 && *v <= max_disk.min(1 << 50))
        .ok_or_else(|| {
            invalid_field(
                "per_environment_limits.disk_bytes",
                "disk limit exceeds host capacity",
            )
        })?;
    let egress = match pool.max_egress_mode() {
        h::EgressMode::DenyAll => EgressMode::DenyAll,
        h::EgressMode::PublicInternet => EgressMode::PublicInternet,
        _ => return Err(invalid_field("max_egress_mode", "invalid egress policy")),
    };
    let grace = pool
        .stop_grace
        .filter(|g| {
            (1..=300).contains(&g.seconds)
                && (0..1_000_000_000).contains(&g.nanos)
                && (g.seconds < 300 || g.nanos == 0)
        })
        .ok_or_else(|| invalid_field("stop_grace", "expected one second through five minutes"))?;
    Ok(ManagedConnection {
        id: req.registration_id,
        request: req.request_id,
        endpoint: req.endpoint,
        trust_bundle: req.trust_bundle,
        display_name: req.display_name,
        source_revision: req.source_policy_revision,
        pool: ManagedPool {
            pids,
            disk_bytes,
            egress,
            stop_grace_seconds: grace.seconds as u64,
        },
    })
}
pub(super) fn transport(config: &ManagedConnection, revision: u64) -> salix::RemoteConnection {
    let pool = &config.pool;
    salix::RemoteConnection {
        gateway_endpoint: config.endpoint.clone(),
        trust_bundle: STANDARD.encode(&config.trust_bundle),
        policy_revision: revision,
        pool_policy: salix::PoolPolicy {
            per_environment_limits: salix::ResourceLimits {
                pids: pool.pids,
                disk_bytes: pool.disk_bytes,
            },
            max_egress_mode: match pool.egress {
                EgressMode::DenyAll => salix::EgressMode::DenyAll,
                EgressMode::PublicInternet => salix::EgressMode::PublicInternet,
            },
            stop_grace_seconds: pool.stop_grace_seconds,
        },
    }
}
fn egress(mode: EgressMode) -> i32 {
    match mode {
        EgressMode::DenyAll => h::EgressMode::DenyAll.into(),
        EgressMode::PublicInternet => h::EgressMode::PublicInternet.into(),
    }
}
fn allocation(allocation: RemoteAllocation) -> h::RemoteAllocation {
    let scope = allocation.scope;
    h::RemoteAllocation {
        allocation_id: allocation.allocation_id,
        display_label: allocation.display_label,
        limits: Some(h::ResourceLimits {
            pids: u32::try_from(scope.limits.pids_max).ok(),
            disk_bytes: Some(scope.limits.disk_quota_bytes),
        }),
        egress_mode: egress(scope.egress_mode),
        revision: scope.revision,
        state: match scope.state {
            ScopeState::Active => h::AllocationState::Ready,
            ScopeState::Draining => h::AllocationState::Draining,
            ScopeState::Retained => h::AllocationState::Retained,
            ScopeState::Released => h::AllocationState::Discarded,
        }
        .into(),
        environment_id: scope.id,
        generation: scope.generation,
    }
}
pub(super) fn project(
    record: ManagedRegistration,
    inventory: RemoteInventory,
    phase: h::ConnectorPhase,
) -> Result<h::RemoteRegistration, Status> {
    let cfg = record.config;
    let pool = cfg.pool;
    let request = cfg
        .request
        .strip_suffix(":managed-enrollment")
        .unwrap_or(&cfg.request);
    let source_request_id = request
        .strip_suffix(":registration")
        .unwrap_or(request)
        .into();
    Ok(h::RemoteRegistration {
        id: cfg.id,
        endpoint: cfg.endpoint,
        display_name: cfg.display_name,
        pool: Some(h::RemotePoolPolicy {
            per_environment_limits: Some(h::ResourceLimits {
                pids: Some(pool.pids),
                disk_bytes: Some(pool.disk_bytes),
            }),
            max_egress_mode: egress(pool.egress),
            stop_grace: Some(
                std::time::Duration::from_secs(pool.stop_grace_seconds)
                    .try_into()
                    .map_err(|_| Status::data_loss("invalid stored stop grace"))?,
            ),
        }),
        revision: record.revision,
        pool_policy_revision: record.pool_revision,
        source_request_id,
        state: match record.phase {
            ManagedPhase::PendingCredential | ManagedPhase::Enabled => {
                h::RemoteRegistrationState::Enabled
            }
            ManagedPhase::Draining | ManagedPhase::Revoking => h::RemoteRegistrationState::Draining,
            ManagedPhase::Revoked => h::RemoteRegistrationState::Revoked,
        }
        .into(),
        enrollment_state: match record.phase {
            ManagedPhase::PendingCredential => h::RemoteEnrollmentState::PendingCredential,
            ManagedPhase::Enabled | ManagedPhase::Draining => h::RemoteEnrollmentState::Ready,
            _ => h::RemoteEnrollmentState::Revoking,
        }
        .into(),
        connection_state: if phase == h::ConnectorPhase::Connected {
            h::RemoteConnectionState::Connected
        } else {
            h::RemoteConnectionState::Disconnected
        }
        .into(),
        connector_status: Some(h::ConnectorStatus {
            phase: phase.into(),
            observed_at: Some(SystemTime::now().into()),
            ..Default::default()
        }),
        owner_kind: h::RemoteRegistrationOwnerKind::External.into(),
        created_at: Some(record.created.into()),
        updated_at: Some(record.updated.into()),
        inventory_revision: inventory.watermark,
        allocations: inventory.allocations.into_iter().map(allocation).collect(),
        ..Default::default()
    })
}
