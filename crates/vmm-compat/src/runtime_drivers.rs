use crate::{RuntimeContext, guest::Output};
use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
use tonic::{Status, Streaming};

#[async_trait]
pub trait ImageRuntime: Send + Sync {
    async fn build_image(
        &self,
        ctx: &RuntimeContext,
        req: Streaming<h::BuildImageRequest>,
    ) -> Result<Output<h::BuildImageResponse>, Status>;
    async fn observe_image_operation(
        &self,
        ctx: &RuntimeContext,
        req: h::ObserveImageOperationRequest,
    ) -> Result<h::ObserveImageOperationResponse, Status>;
    async fn cancel_image_operation(
        &self,
        ctx: &RuntimeContext,
        req: h::CancelImageOperationRequest,
    ) -> Result<h::CancelImageOperationResponse, Status>;
}

#[async_trait]
pub trait MeshRuntime: Send + Sync {
    async fn create_service_export(
        &self,
        ctx: &RuntimeContext,
        req: h::CreateServiceExportRequest,
    ) -> Result<h::CreateServiceExportResponse, Status>;
    async fn list_service_exports(
        &self,
        ctx: &RuntimeContext,
        req: h::ListServiceExportsRequest,
    ) -> Result<h::ListServiceExportsResponse, Status>;
    async fn revoke_service_export(
        &self,
        ctx: &RuntimeContext,
        req: h::RevokeServiceExportRequest,
    ) -> Result<h::RevokeServiceExportResponse, Status>;
    async fn create_service_import(
        &self,
        ctx: &RuntimeContext,
        req: h::CreateServiceImportRequest,
    ) -> Result<h::CreateServiceImportResponse, Status>;
    async fn create_personal_service_import(
        &self,
        ctx: &RuntimeContext,
        req: h::CreatePersonalServiceImportRequest,
    ) -> Result<h::CreatePersonalServiceImportResponse, Status>;
    async fn list_service_imports(
        &self,
        ctx: &RuntimeContext,
        req: h::ListServiceImportsRequest,
    ) -> Result<h::ListServiceImportsResponse, Status>;
    async fn revoke_service_import(
        &self,
        ctx: &RuntimeContext,
        req: h::RevokeServiceImportRequest,
    ) -> Result<h::RevokeServiceImportResponse, Status>;
}
