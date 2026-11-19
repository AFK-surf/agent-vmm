use crate::{Containers, RuntimeContext, context::core_status};
use agent_vmm_proto::host::v1 as h;
use std::time::SystemTime;
use tonic::Status;
use vmm_core::{EgressMode, Origin, SandboxState};

impl Containers {
    pub async fn environment(
        &self,
        ctx: &RuntimeContext,
    ) -> Result<h::GetEnvironmentResponse, Status> {
        let store = &ctx.core.store;
        let mut observed = vec![];
        let mut stale = false;
        let now: prost_types::Timestamp = SystemTime::now().into();
        let mut totals = h::Usage {
            sampled_at: Some(now),
            ..Default::default()
        };
        for sb in store
            .list_sandboxes(&ctx.scope.id, None, usize::MAX)
            .map_err(core_status)?
        {
            let container = match self.observe(ctx, &sb, None).await {
                Ok(c) => c,
                Err(e)
                    if matches!(
                        e.code(),
                        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                    ) =>
                {
                    stale = true;
                    h::Container {
                        id: sb.definition.key.sandbox_id.clone(),
                        generation_id: sb.definition.key.generation_id.clone(),
                        state: h::ContainerState::Unknown as i32,
                        image: sb.definition.image_reference.clone(),
                        ..Default::default()
                    }
                }
                Err(e) => return Err(e),
            };
            if let Some(sample) = &container.usage {
                totals.pids = totals.pids.saturating_add(sample.pids);
                totals.cpu_nanos = totals.cpu_nanos.saturating_add(sample.cpu_nanos);
                totals.memory_bytes = totals.memory_bytes.saturating_add(sample.memory_bytes);
                totals.read_bytes = totals.read_bytes.saturating_add(sample.read_bytes);
                totals.write_bytes = totals.write_bytes.saturating_add(sample.write_bytes);
                totals.network_rx_bytes = totals
                    .network_rx_bytes
                    .saturating_add(sample.network_rx_bytes);
                totals.network_tx_bytes = totals
                    .network_tx_bytes
                    .saturating_add(sample.network_tx_bytes);
                totals.stale |= sample.stale || !sample.unavailable_reason.is_empty();
            } else if matches!(sb.state, SandboxState::Running | SandboxState::Unverified) {
                totals.stale = true;
            }
            observed.push(container);
        }
        totals.disk_bytes = store.scope_used_bytes(&ctx.scope.id).map_err(core_status)?;
        totals.stale |= stale;
        if totals.stale {
            totals.unavailable_reason = "some runtime samples unavailable".into();
        }
        let origin = match ctx.scope.origin {
            Origin::Local => h::EnvironmentOrigin::Local,
            Origin::Remote { .. } => h::EnvironmentOrigin::Remote,
        };
        let egress = match ctx.scope.egress_mode {
            EgressMode::DenyAll => h::EgressMode::DenyAll,
            EgressMode::PublicInternet => h::EgressMode::PublicInternet,
        };
        // The runtime wire intentionally omits the host-private owner and namespace identity.
        Ok(h::GetEnvironmentResponse {
            environment: Some(h::AgentEnvironment {
                policy_revision: ctx.scope.revision,
                limits: Some(h::ResourceLimits {
                    pids: Some(ctx.scope.limits.pids_max.min(u32::MAX as u64) as u32),
                    disk_bytes: Some(ctx.scope.limits.disk_quota_bytes),
                }),
                egress_mode: egress as i32,
                origin: origin as i32,
                inventory_observed_at: Some(now),
                inventory_state: if stale {
                    h::InventoryState::Stale
                } else {
                    h::InventoryState::Available
                } as i32,
                inventory_message: if stale {
                    "Some runtime inventory could not be refreshed.".into()
                } else {
                    String::new()
                },
                health: Some(h::HealthStatus {
                    state: if stale {
                        h::HealthState::Degraded
                    } else {
                        h::HealthState::Healthy
                    } as i32,
                    ..Default::default()
                }),
                usage: Some(totals),
                containers: observed,
                volumes: ctx.list_volumes()?.volumes,
                ..Default::default()
            }),
        })
    }
}

impl RuntimeContext {
    pub fn release_session(
        &self,
        r: h::ReleaseSessionRequest,
    ) -> Result<h::ReleaseSessionResponse, Status> {
        if r.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is required"));
        }
        self.core
            .store
            .revoke_session(&self.session.id)
            .map_err(core_status)?;
        Ok(h::ReleaseSessionResponse {})
    }
}
