use super::{LocalOperator, h};
use crate::error::core_status;
use std::time::SystemTime;
use tonic::Status;
use vmm_core::Origin;

impl LocalOperator {
    pub(super) async fn environment(
        &self,
        req: h::OperatorServiceGetEnvironmentRequest,
    ) -> Result<h::OperatorServiceGetEnvironmentResponse, Status> {
        let store = &self.inventory.store;
        let scope = store.scope(&req.environment_id).map_err(core_status)?;
        let vm_state = self.snapshot().await.ok().map(|vm| vm.state());
        let observed = match (&self.containers, vm_state) {
            (Some(containers), Some(h::VmState::Running)) => {
                match containers.observe_environment(store, &scope).await {
                    Ok(env) => Some(env),
                    Err(err) if err.code() == tonic::Code::Unavailable => None,
                    Err(err) => return Err(err),
                }
            }
            _ => None,
        };
        let mut env = if let Some(env) = observed {
            env
        } else {
            let stopped = vm_state == Some(h::VmState::Stopped);
            let now = Some(SystemTime::now().into());
            h::AgentEnvironment {
                inventory_state: if stopped {
                    h::InventoryState::VmStopped
                } else {
                    h::InventoryState::Unavailable
                } as i32,
                inventory_message: if stopped {
                    "Start the virtual machine to inspect containers, images, and volumes."
                } else {
                    "Runtime inventory is temporarily unavailable."
                }
                .into(),
                inventory_observed_at: now,
                usage: Some(h::Usage {
                    sampled_at: now,
                    stale: true,
                    unavailable_reason: "guest runtime is unavailable".into(),
                    ..Default::default()
                }),
                health: Some(h::HealthStatus {
                    state: h::HealthState::Unavailable as i32,
                    ..Default::default()
                }),
                containers: store
                    .list_sandboxes(&scope.id, None, usize::MAX)
                    .map_err(core_status)?
                    .into_iter()
                    .map(|s| h::Container {
                        id: s.definition.key.sandbox_id,
                        generation_id: s.definition.key.generation_id,
                        image: s.definition.image_reference,
                        state: h::ContainerState::Unknown as i32,
                        ..Default::default()
                    })
                    .collect(),
                ..crate::environment::environment_policy(&scope)
            }
        };
        let mut owner = h::EnvironmentOwner::default();
        env.id.clone_from(&scope.id);
        env.namespace_id.clone_from(&scope.id);
        match scope.origin {
            Origin::Local => {
                owner.kind = h::EnvironmentOwnerKind::Local as i32;
                env.display_name = scope.id;
            }
            Origin::Remote { registration_id } => {
                let allocation = store
                    .operator_remote_allocation(&scope.id)
                    .map_err(core_status)?;
                owner.kind = h::EnvironmentOwnerKind::RemoteRegistration as i32;
                owner.registration_id = registration_id;
                owner.allocation_id = allocation.allocation_id;
                env.display_name = allocation.display_label;
            }
        }
        env.owner = Some(owner);
        Ok(h::OperatorServiceGetEnvironmentResponse {
            environment: Some(env),
        })
    }
}
