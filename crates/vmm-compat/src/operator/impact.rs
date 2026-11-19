//! The whole-VM fence carries a bounded observation of durable owners.
use super::{LocalInventory, h};
use std::time::SystemTime;
use vmm_core::Origin;

const LIMIT: usize = 100;

fn partial(summary: &mut h::GetVmImpactSummaryResponse, reason: &str, overflow: bool) {
    summary.complete = false;
    summary.overflow |= overflow;
    if summary.issue.is_empty() {
        summary.issue = reason.to_owned();
    }
}

impl LocalInventory {
    pub(super) fn impact(&self, vm: Option<h::Vm>) -> h::GetVmImpactSummaryResponse {
        let mut summary = h::GetVmImpactSummaryResponse {
            vm,
            collected_at: Some(SystemTime::now().into()),
            complete: true,
            ..Default::default()
        };
        match self.store.list_scopes(None, LIMIT + 1) {
            Ok(scopes) => {
                if scopes.len() > LIMIT {
                    partial(
                        &mut summary,
                        "environment summary exceeds the 100 item bound",
                        true,
                    );
                }
                for scope in scopes.into_iter().take(LIMIT) {
                    let generation = match scope.origin {
                        Origin::Local => scope.revision,
                        Origin::Remote { .. } => scope.generation,
                    };
                    summary.known_workloads.push(h::ResourceReference {
                        kind: "environment".into(),
                        environment_id: scope.id,
                        generation,
                        ..Default::default()
                    });
                }
            }
            Err(_) => partial(&mut summary, "environment summary is unavailable", false),
        }
        match self.store.registration_inventory_watermarks(LIMIT + 1) {
            Ok(rows) => {
                if rows.len() > LIMIT {
                    partial(
                        &mut summary,
                        "registration summary exceeds the 100 item bound",
                        true,
                    );
                }
                for (id, generation) in rows {
                    if summary.known_workloads.len() == LIMIT {
                        partial(
                            &mut summary,
                            "known workload summary exceeds the 100 item bound",
                            true,
                        );
                        break;
                    }
                    summary.known_workloads.push(h::ResourceReference {
                        kind: "registration".into(),
                        resource_id: id,
                        generation,
                        ..Default::default()
                    });
                }
            }
            Err(_) => partial(&mut summary, "registration summary is unavailable", false),
        }
        summary
    }
}
