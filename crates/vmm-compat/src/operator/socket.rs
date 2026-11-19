use super::{LocalOperator, h};
use crate::error::{core_status, invalid_field};
use vmm_rpc::Status;
use vmm_core::{Core, HostSocketApproval};

fn wire(row: HostSocketApproval) -> h::HostSocket {
    h::HostSocket {
        environment_id: row.scope,
        port: u32::from(row.port),
        path: row.path,
        revision: row.revision,
    }
}

fn port(value: u32, request: &str) -> Result<u16, Status> {
    if request.is_empty() {
        return Err(invalid_field("request_id", "request ID is required"));
    }
    u16::try_from(value)
        .ok()
        .filter(|p| !matches!(p, 0 | 53))
        .ok_or_else(|| invalid_field("port", "a TCP port other than 53 is required"))
}

impl LocalOperator {
    async fn publish_sockets(&self) -> Result<(), Status> {
        if let Some(backend) = &self.backend {
            let core = Core::new(self.inventory.store.clone(), backend.clone());
            core.project_host_sockets()
                .await
                .map_err(|_| crate::error::projection_failed())?;
        }
        Ok(())
    }

    pub(super) async fn set_socket(
        &self,
        r: h::SetHostSocketRequest,
    ) -> Result<h::SetHostSocketResponse, Status> {
        let port = port(r.port, &r.request_id)?;
        let rule = self
            .inventory
            .store
            .change_host_socket(&r.environment_id, port, Some(&r.path), &r.request_id)
            .await
            .map_err(|err| match err {
                vmm_core::Error::Invalid("host socket path") => {
                    invalid_field("path", "an absolute, clean Host path is required")
                }
                other => core_status(other),
            })?;
        self.publish_sockets().await?;
        Ok(h::SetHostSocketResponse {
            host_socket: rule.map(wire),
        })
    }

    pub(super) async fn remove_socket(
        &self,
        r: h::RemoveHostSocketRequest,
    ) -> Result<h::RemoveHostSocketResponse, Status> {
        let port = port(r.port, &r.request_id)?;
        self.inventory
            .store
            .change_host_socket(&r.environment_id, port, None, &r.request_id)
            .await
            .map_err(core_status)?;
        self.publish_sockets().await?;
        Ok(h::RemoveHostSocketResponse {})
    }

    pub(super) async fn list_sockets(
        &self,
        r: h::ListHostSocketsRequest,
    ) -> Result<h::ListHostSocketsResponse, Status> {
        let rows = self
            .inventory
            .store
            .host_sockets(r.environment_id.as_deref().unwrap_or_default())
            .map_err(core_status)?;
        Ok(h::ListHostSocketsResponse {
            host_sockets: rows.into_iter().map(wire).collect(),
        })
    }
}
