//! Host-local views used by the operator socket.
use crate::error::{core_status, invalid_field};
use agent_vmm_proto::host::v1 as h;
use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
    time::SystemTime,
};
use tonic::Status;
use uuid::Uuid;
use vmm_core::{Origin, ScopeState, Store};

const PAGE: usize = 32;
static BOOT_ID: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().to_string());

/// Sample of the shared guest backing store in bytes. Capacity is its configured
/// virtual capacity, and usage is the backing store's measured consumption.
pub struct DiskSample {
    pub used: u64,
    pub capacity: u64,
    pub import_reservations: u64,
}

/// Host observations shared across requests to the same running daemon.
pub struct LocalInventory {
    store: Arc<Store>,
}

impl LocalInventory {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    pub fn disk(
        &self,
        vm: h::VmState,
        sample: impl Into<Option<DiskSample>>,
    ) -> h::GetLocalDiskSummaryResponse {
        let sample = sample.into();
        let unavailable_reason = if sample.is_some() {
            String::new()
        } else {
            "guest state disk is unavailable".into()
        };
        h::GetLocalDiskSummaryResponse {
            boot_id: BOOT_ID.clone(),
            collected_at: Some(SystemTime::now().into()),
            vm_state: vm as i32,
            guest_state_fs: sample.map(|sample| h::DiskAccounting {
                used_bytes: Some(sample.used),
                capacity_bytes: Some(sample.capacity),
                import_reservation_bytes: Some(sample.import_reservations),
                ..Default::default()
            }),
            unavailable_reason,
        }
    }

    pub fn registrations(
        &self,
        req: h::ListLocalRegistrationPageRequest,
        control: &dyn RegistrationControl,
    ) -> Result<h::ListLocalRegistrationPageResponse, Status> {
        if req.after_id.len() > 200 {
            return Err(invalid_field("after_id", "registration cursor is too long"));
        }
        let page = self
            .store
            .operator_registration_page(&req.after_id, PAGE)
            .map_err(core_status)?;
        let next_cursor = page
            .last()
            .filter(|_| page.len() == PAGE)
            .map(|(r, _, _)| r.id.clone())
            .unwrap_or_default();
        let mut registrations = Vec::new();
        for (reg, private, revoked) in page {
            if private {
                continue;
            }
            let (revision, _) = control.metadata(&reg.id)?;
            if revision == 0 {
                return Err(Status::data_loss("invalid stored operator revision"));
            }
            let state = if revoked {
                h::RemoteRegistrationState::Revoked
            } else if reg.draining {
                h::RemoteRegistrationState::Draining
            } else {
                h::RemoteRegistrationState::Enabled
            };
            registrations.push(h::LocalRegistrationMetadata {
                id: reg.id,
                revision,
                state: state as i32,
            });
        }
        Ok(h::ListLocalRegistrationPageResponse {
            registrations,
            next_cursor,
        })
    }

    pub fn environments(
        &self,
        req: h::ListLocalEnvironmentPageRequest,
        vm: h::VmState,
        active_scopes: &HashSet<String>,
    ) -> Result<h::ListLocalEnvironmentPageResponse, Status> {
        if req.after_id.len() > 200 {
            return Err(invalid_field("after_id", "environment cursor is too long"));
        }
        let scopes = self
            .store
            .list_operator_scopes(&req.after_id, PAGE)
            .map_err(core_status)?;
        let collected = SystemTime::now().into();
        let mut items = Vec::with_capacity(scopes.len());
        for scope in scopes {
            let mut row = h::LocalEnvironmentSummary {
                id: scope.id.clone(),
                revision: scope.revision,
                private_disk_bytes: Some(
                    self.store
                        .scope_used_bytes(&scope.id)
                        .map_err(core_status)?,
                ),
                state_observed_at: Some(collected),
                partial: true,
                ..Default::default()
            };
            row.origin = match scope.origin {
                Origin::Local => "local".into(),
                Origin::Remote { registration_id } => {
                    let allocation = self
                        .store
                        .operator_remote_allocation(&scope.id)
                        .map_err(core_status)?;
                    row.registration_id = registration_id;
                    row.allocation_id = allocation.allocation_id;
                    row.allocation_generation = allocation.scope.generation;
                    "remote".into()
                }
            };
            row.state = if vm != h::VmState::Running {
                "vm_stopped"
            } else if matches!(scope.state, ScopeState::Retained | ScopeState::Released) {
                "retired"
            } else if active_scopes.contains(&scope.id) {
                "observed_active"
            } else {
                "unknown"
            }
            .into();
            items.push(row);
        }
        let next_cursor = if items.len() == PAGE {
            items.last().expect("full page").id.clone()
        } else {
            String::new()
        };
        Ok(h::ListLocalEnvironmentPageResponse {
            environments: items,
            next_cursor,
            collected_at: Some(collected),
            boot_id: BOOT_ID.clone(),
        })
    }
}

mod disposal;
pub use disposal::{LocalDisposals, RegistrationControl};
mod local;
pub use local::{LocalOperator, LocalOperatorServer};

mod mcp;
pub use mcp::{McpManagement,ApplianceOperations};
