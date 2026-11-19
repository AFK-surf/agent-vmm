//! Service declarations exposed to a session on this device.
use crate::{
    RuntimeContext,
    error::{core_status, host_status, invalid_field, with_resource},
    runtime::MeshRuntime,
};
use agent_vmm_proto::{host::v1 as h, service::v1 as s};
use vmm_rpc::{Code, Status};
use vmm_core::{
    Error,
    mesh::{LocalServiceImport, MeshExport, MeshImport, MeshResource},
};

pub struct ServiceMesh {
    pub device_id: String,
}

fn export(e: MeshExport) -> s::ServiceExport {
    s::ServiceExport {
        export_id: e.id,
        environment_id: e.owner.scope_id,
        container_id: e.owner.sandbox_id,
        container_generation_id: e.owner.generation_id,
        container_instance_id: e.instance,
        port: e.port.into(),
        protocol: e.protocol,
        revision: e.revision,
        revoked: e.revoked,
        created_at: Some(prost_types::Timestamp {
            seconds: e.created,
            nanos: 0,
        }),
    }
}
fn import(i: MeshImport) -> s::ServiceImport {
    s::ServiceImport {
        import_id: i.id,
        source_environment_id: i.owner.scope_id,
        source_container_id: i.owner.sandbox_id,
        source_container_generation_id: i.owner.generation_id,
        source_container_instance_id: i.instance,
        destination_device_id: i.device,
        destination_export_id: i.destination,
        destination_port: i.port.into(),
        virtual_service_name: i.alias,
        revision: i.revision,
        revoked: i.revoked,
        ..Default::default()
    }
}
fn status(code: Code, reason: h::ErrorReason, msg: &str, kind: &str, id: &str) -> Status {
    with_resource(host_status(code, reason, msg), kind, id)
}
#[async_trait::async_trait]
impl MeshRuntime for ServiceMesh {
    async fn create_service_export(
        &self,
        ctx: &RuntimeContext,
        r: h::CreateServiceExportRequest,
    ) -> Result<h::CreateServiceExportResponse, Status> {
        if r.request_id.is_empty()
            || r.container_id.is_empty()
            || r.container_instance_id.is_empty()
            || !(1..=65535).contains(&r.port)
            || !matches!(
                r.protocol(),
                s::ServiceProtocol::Tcp | s::ServiceProtocol::Http
            )
        {
            return Err(invalid_field(
                "request_id/container_instance/port/protocol",
                "complete current service export is required",
            ));
        }
        let value = ctx
            .core
            .create_mesh_export(
                &ctx.session,
                &r.request_id,
                &r.container_id,
                &r.container_instance_id,
                r.port as u16,
                r.protocol,
            )
            .await
            .map_err(core_status)?;
        Ok(h::CreateServiceExportResponse {
            export: Some(export(value)),
        })
    }
    async fn list_service_exports(
        &self,
        ctx: &RuntimeContext,
        _: h::ListServiceExportsRequest,
    ) -> Result<h::ListServiceExportsResponse, Status> {
        let exports = ctx
            .core
            .store
            .mesh_exports(&ctx.session)
            .map_err(core_status)?
            .into_iter()
            .map(export)
            .collect();
        Ok(h::ListServiceExportsResponse {
            exports: Some(s::ListServiceExportsResponse { exports }),
        })
    }
    async fn revoke_service_export(
        &self,
        ctx: &RuntimeContext,
        r: h::RevokeServiceExportRequest,
    ) -> Result<h::RevokeServiceExportResponse, Status> {
        if r.request_id.is_empty() || r.export_id.is_empty() || r.expected_revision == 0 {
            return Err(invalid_field(
                "request_id/export_id/expected_revision",
                "complete export revoke is required",
            ));
        }
        ctx.core
            .revoke_mesh_service(
                &ctx.session,
                MeshResource::Export,
                &r.export_id,
                r.expected_revision,
                &r.request_id,
            )
            .await
            .map_err(|e| match e {
                Error::Fenced => status(
                    Code::Aborted,
                    h::ErrorReason::ServiceExportRevisionConflict,
                    "export owner or revision changed",
                    "service_export",
                    &r.export_id,
                ),
                other => core_status(other),
            })?;
        Ok(h::RevokeServiceExportResponse {})
    }
    async fn create_service_import(
        &self,
        ctx: &RuntimeContext,
        r: h::CreateServiceImportRequest,
    ) -> Result<h::CreateServiceImportResponse, Status> {
        if r.request_id.is_empty()
            || r.source_container_id.is_empty()
            || r.source_container_instance_id.is_empty()
            || r.destination_device_id.is_empty()
            || r.destination_export_id.is_empty()
            || !vmm_core::mesh::valid_alias(&r.virtual_service_name)
        {
            return Err(invalid_field(
                "service_import",
                "source, destination, and virtual service name are required",
            ));
        }
        if r.destination_device_id != "local" && r.destination_device_id != self.device_id {
            return Err(status(
                Code::FailedPrecondition,
                h::ErrorReason::DestinationEndpointStale,
                "a current signed destination endpoint is required",
                "device",
                &r.destination_device_id,
            ));
        }
        if r.destination_port != 0 {
            return Err(invalid_field(
                "destination_port",
                "destination_port must be omitted for a local route",
            ));
        }
        if r.capability.is_some() {
            return Err(status(
                Code::PermissionDenied,
                h::ErrorReason::RouteCapabilityDenied,
                "route capability has no verified issuer",
                "service_export",
                &r.destination_export_id,
            ));
        }
        let i = ctx
            .core
            .create_mesh_import(
                &ctx.session,
                LocalServiceImport {
                    request: &r.request_id,
                    container: &r.source_container_id,
                    instance: &r.source_container_instance_id,
                    device: &self.device_id,
                    export: &r.destination_export_id,
                    alias: &r.virtual_service_name,
                },
            )
            .await
            .map_err(|e| match e {
                Error::Missing("service export") => status(
                    Code::NotFound,
                    h::ErrorReason::ServiceExportNotFound,
                    "destination export is unavailable",
                    "service_export",
                    &r.destination_export_id,
                ),
                Error::Conflict("cross-domain service route") => status(
                    Code::PermissionDenied,
                    h::ErrorReason::CrossDomainServiceRoute,
                    "destination is outside the approved controller domain",
                    "service_export",
                    &r.destination_export_id,
                ),
                Error::Conflict("destination instance stale") => status(
                    Code::FailedPrecondition,
                    h::ErrorReason::DestinationContainerInstanceStale,
                    "exported instance is unavailable",
                    "service_export",
                    &r.destination_export_id,
                ),
                Error::Fenced => status(
                    Code::FailedPrecondition,
                    h::ErrorReason::SourceContainerInstanceStale,
                    "source instance changed",
                    "container",
                    &r.source_container_id,
                ),
                other => core_status(other),
            })?;
        Ok(h::CreateServiceImportResponse {
            import: Some(import(i)),
        })
    }
    async fn create_personal_service_import(
        &self,
        _: &RuntimeContext,
        r: h::CreatePersonalServiceImportRequest,
    ) -> Result<h::CreatePersonalServiceImportResponse, Status> {
        Err(status(
            Code::PermissionDenied,
            h::ErrorReason::PersonalRouteRequestDenied,
            "current mesh membership is required",
            "device",
            &r.destination_device_id,
        ))
    }
    async fn list_service_imports(
        &self,
        ctx: &RuntimeContext,
        _: h::ListServiceImportsRequest,
    ) -> Result<h::ListServiceImportsResponse, Status> {
        Ok(h::ListServiceImportsResponse {
            imports: ctx
                .core
                .store
                .mesh_imports(&ctx.session)
                .map_err(core_status)?
                .into_iter()
                .map(import)
                .collect(),
        })
    }
    async fn revoke_service_import(
        &self,
        ctx: &RuntimeContext,
        r: h::RevokeServiceImportRequest,
    ) -> Result<h::RevokeServiceImportResponse, Status> {
        if r.request_id.is_empty() || r.import_id.is_empty() || r.expected_revision == 0 {
            return Err(invalid_field(
                "request_id/import_id/expected_revision",
                "complete import revoke is required",
            ));
        }
        ctx.core
            .revoke_mesh_service(
                &ctx.session,
                MeshResource::Import,
                &r.import_id,
                r.expected_revision,
                &r.request_id,
            )
            .await
            .map_err(|e| match e {
                Error::Fenced => status(
                    Code::Aborted,
                    h::ErrorReason::ServiceImportRevisionConflict,
                    "import owner or revision changed",
                    "service_import",
                    &r.import_id,
                ),
                other => core_status(other),
            })?;
        Ok(h::RevokeServiceImportResponse {})
    }
}
