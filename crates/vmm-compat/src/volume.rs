use crate::{
    RuntimeContext,
    context::{core_status, resource_id},
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as host;
use prost::Message;
use tonic::Status;
use vmm_core::{Sandbox, Volume};

impl RuntimeContext {
    pub async fn create_volume(
        &self,
        req: host::CreateVolumeRequest,
    ) -> Result<host::CreateVolumeResponse, Status> {
        resource_id(&req.volume_id)?;
        if req.owner_uid.is_some() != req.owner_gid.is_some() {
            return Err(Status::invalid_argument(
                "owner_uid and owner_gid must be provided together",
            ));
        }
        let mode = req.mode.unwrap_or(0o700);
        if mode & !0o1777 != 0 {
            return Err(Status::invalid_argument("invalid volume permissions"));
        }
        let volume = Volume {
            scope_id: self.scope.id.clone(),
            volume_id: req.volume_id.clone(),
            quota_bytes: self.scope.limits.disk_quota_bytes,
            used_bytes: 0,
            owner_uid: req.owner_uid.unwrap_or(0),
            owner_gid: req.owner_gid.unwrap_or(0),
            mode,
            key_id: None,
        };
        let core = self.core.clone();
        mutate(
            core.store.clone(),
            self.scope.id.clone(),
            "runtime-create-volume",
            req.request_id.clone(),
            req.encode_to_vec(),
            async move {
                core.create_volume(&volume).await.map_err(core_status)?;
                Ok(host::CreateVolumeResponse {
                    volume: Some(project(volume, &[])),
                })
            },
        )
        .await
    }

    pub async fn delete_volume(
        &self,
        req: host::DeleteVolumeRequest,
    ) -> Result<host::DeleteVolumeResponse, Status> {
        resource_id(&req.volume_id)?;
        let core = self.core.clone();
        let scope = self.scope.id.clone();
        mutate(
            core.store.clone(),
            scope.clone(),
            "runtime-delete-volume",
            req.request_id.clone(),
            req.encode_to_vec(),
            async move {
                core.discard_volume(&scope, &req.volume_id)
                    .await
                    .map_err(core_status)?;
                Ok(host::DeleteVolumeResponse {})
            },
        )
        .await
    }

    pub fn get_volume(
        &self,
        req: host::GetVolumeRequest,
    ) -> Result<host::GetVolumeResponse, Status> {
        resource_id(&req.volume_id)?;
        let volume = self
            .core
            .store
            .volume(&self.scope.id, &req.volume_id)
            .map_err(core_status)?;
        let sandboxes = self
            .core
            .store
            .list_sandboxes(&self.scope.id, None, usize::MAX)
            .map_err(core_status)?;
        Ok(host::GetVolumeResponse {
            volume: Some(project(volume, &sandboxes)),
        })
    }

    pub fn list_volumes(&self) -> Result<host::ListVolumesResponse, Status> {
        Ok(host::ListVolumesResponse {
            volumes: list_volumes(&self.core.store, &self.scope.id)?,
        })
    }
}

pub(crate) fn list_volumes(
    store: &vmm_core::Store,
    scope_id: &str,
) -> Result<Vec<host::Volume>, Status> {
    let volumes = store
        .list_volumes(scope_id, None, usize::MAX)
        .map_err(core_status)?;
    let sandboxes = store
        .list_sandboxes(scope_id, None, usize::MAX)
        .map_err(core_status)?;
    Ok(volumes
        .into_iter()
        .filter(|v| v.volume_id != crate::workspace::DEFAULT_VOLUME)
        .map(|v| project(v, &sandboxes))
        .collect())
}

fn project(volume: Volume, sandboxes: &[Sandbox]) -> host::Volume {
    let mounted_by = sandboxes
        .iter()
        .filter(|s| {
            s.definition
                .mounts
                .iter()
                .any(|m| m.volume_id == volume.volume_id)
        })
        .map(|s| s.definition.key.sandbox_id.clone())
        .collect();
    host::Volume {
        id: volume.volume_id,
        limit_bytes: volume.quota_bytes,
        mounted_by,
        usage: Some(host::Usage {
            disk_bytes: volume.used_bytes,
            ..Default::default()
        }),
    }
}
