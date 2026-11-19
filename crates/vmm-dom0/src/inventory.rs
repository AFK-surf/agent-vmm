//! Logical objects in a complete, stable core inventory, including pending
//! allocations. Physical coordinates stay exclusively in the pool database.
use crate::{
    backend::LocalDom0Backend,
    catalog::{image_id, overlay_id, volume_id},
};
use std::collections::BTreeSet;
use vmm_core::{Digest, Result, SandboxKey};

#[derive(Default)]
pub struct AllocationInventory {
    pub images: BTreeSet<Digest>,
    pub volumes: BTreeSet<(String, String)>,
    pub overlays: BTreeSet<(SandboxKey, String)>,
}
impl LocalDom0Backend {
    /// The host holds its core lifecycle lock while building and submitting
    /// the inventory. A missing reference stops all orphan reclamation.
    pub async fn reconcile_allocations(
        &self,
        inventory: &AllocationInventory,
    ) -> Result<Vec<String>> {
        let _lock = self.effects.lock().await;
        let mut keep = inventory
            .images
            .iter()
            .copied()
            .map(image_id)
            .collect::<BTreeSet<_>>();
        if let Some(d) = self.cfg.helper_image {
            keep.insert(image_id(d));
        }
        for (s, v) in &inventory.volumes {
            keep.insert(volume_id(s, v)?);
        }
        for (k, o) in &inventory.overlays {
            keep.insert(overlay_id(k, o)?);
        }
        keep.extend(self.catalog().transient_objects()?);
        for b in self.catalog().boots()?.into_iter().filter(|b| !b.retired) {
            keep.insert(image_id(b.definition.manifest_digest));
            keep.extend(b.disks);
        }
        self.pool.reconcile(&keep)
    }
}
