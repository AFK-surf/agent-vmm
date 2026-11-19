//! Select build guests through the backend owning their boot and their work disks.
use crate::Dom0Workspaces;
use async_trait::async_trait;
use tonic::Status;
use vmm_compat::{
    error::core_status,
    image::build::{BuildBackend, BuildGuest, GuestLease},
};
use vmm_core::{EgressMode, Scope};

struct LocalLease(vmm_dom0::build_vm::BuildVm);
#[async_trait]
impl GuestLease for LocalLease {
    async fn close(self: Box<Self>) -> Result<(), Status> {
        self.0.close().await.map_err(core_status)
    }
}

#[async_trait]
impl BuildBackend for Dom0Workspaces {
    async fn acquire(
        &self,
        id: &str,
        scope: &Scope,
        network: EgressMode,
        quota: u64,
    ) -> Result<BuildGuest, Status> {
        let lease = self
            .0
            .begin_owned_build(scope, network, quota, id)
            .await
            .map_err(core_status)?;
        Ok(BuildGuest {
            client: self.0.bound_guest_client(lease.boot.clone()),
            instance_id: lease.boot.instance_id.clone(),
            lease: Box::new(LocalLease(lease)),
        })
    }
    async fn settle(&self, id: &str) -> Result<(), Status> {
        self.0.settle_build(id).await.map_err(core_status)
    }
}

#[cfg(target_os = "macos")]
mod carrier {
    use super::*;
    use std::sync::Arc;
    use vmm_compat::guest::GuestDriver;
    use vmm_dom0::remote::{RemoteBuild, RemoteDom0Client};
    struct CarrierLease(RemoteBuild);
    #[async_trait]
    impl GuestLease for CarrierLease {
        async fn close(self: Box<Self>) -> Result<(), Status> {
            self.0.close().await.map_err(core_status)
        }
    }
    #[async_trait]
    impl BuildBackend for crate::carrier::CarrierClientAdapter {
        async fn acquire(
            &self,
            id: &str,
            scope: &Scope,
            network: EgressMode,
            quota: u64,
        ) -> Result<BuildGuest, Status> {
            let lease = RemoteBuild::acquire_owned(&self.0, scope, network, quota, id)
                .await
                .map_err(core_status)?;
            Ok(BuildGuest {
                client: self.client(&lease.boot).await?,
                instance_id: lease.boot.instance_id.clone(),
                lease: Box::new(CarrierLease(lease)),
            })
        }
        async fn settle(&self, id: &str) -> Result<(), Status> {
            RemoteDom0Client::transport(Arc::new(self.0.clone()))
                .settle_build(id)
                .await
                .map_err(core_status)
        }
    }
}
