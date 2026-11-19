//! Session-scoped public gRPC boundary shared by direct sockets and controller tunnels.
use crate::{
    Containers, RuntimeContext,
    guest::{self, GuestDriver, Output},
    workspace::WorkspaceService,
};
use agent_vmm_proto::host::v1::{self as h, agent_runtime_service_server::AgentRuntimeService};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use vmm_core::Core;

pub use crate::runtime_drivers::{ImageRuntime, MeshRuntime};

pub struct RuntimeService {
    pub core: Core,
    pub containers: Containers,
    pub workspace: WorkspaceService,
    pub guest: Arc<dyn GuestDriver>,
    pub imports: crate::image::ImageImports,
    pub images: Arc<dyn ImageRuntime>,
    pub mesh: Arc<dyn MeshRuntime>,
}

#[tonic::async_trait]
impl AgentRuntimeService for RuntimeService {
    type PullImageStream = Output<h::PullImageResponse>;
    type LogsStream = Output<h::LogsResponse>;
    type ExecStream = Output<h::ExecResponse>;
    type ForwardTCPStream = Output<h::ForwardTcpResponse>;
    type BuildImageStream = Output<h::BuildImageResponse>;
    type ReadWorkspaceStream = Output<h::ReadWorkspaceResponse>;
    type ArchiveWorkspaceStream = Output<h::ArchiveWorkspaceResponse>;
    async fn get_environment(
        &self,
        req: Request<h::GetEnvironmentRequest>,
    ) -> Result<Response<h::GetEnvironmentResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers.environment(&ctx).await.map(Response::new)
    }
    async fn pull_image(
        &self,
        req: Request<h::PullImageRequest>,
    ) -> Result<Response<Self::PullImageStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.imports
            .pull(ctx, self.containers.clone(), req.into_inner())
            .map(Response::new)
    }
    async fn import_image(
        &self,
        req: Request<Streaming<h::ImportImageRequest>>,
    ) -> Result<Response<h::ImportImageResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.imports
            .receive(ctx, self.containers.clone(), req.into_inner())
            .await
            .map(Response::new)
    }
    async fn list_images(
        &self,
        req: Request<h::ListImagesRequest>,
    ) -> Result<Response<h::ListImagesResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        let request = req.into_inner();
        match h::ImageOrigin::try_from(request.origin) {
            Ok(h::ImageOrigin::CurrentAllocation) => {
                self.containers.list_grants(&ctx).await.map(Response::new)
            }
            Ok(h::ImageOrigin::ControllerRetained) => {
                crate::image::retained::list(&ctx).map(Response::new)
            }
            Err(_) => Err(crate::error::invalid_field(
                "origin",
                "invalid image origin",
            )),
        }
    }
    async fn delete_image(
        &self,
        req: Request<h::DeleteImageRequest>,
    ) -> Result<Response<h::DeleteImageResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        let request = req.into_inner();
        match h::ImageOrigin::try_from(request.origin) {
            Ok(h::ImageOrigin::CurrentAllocation) => self
                .containers
                .delete_grant(ctx, request)
                .await
                .map(Response::new),
            Ok(h::ImageOrigin::ControllerRetained) => crate::image::retained::delete(ctx, request)
                .await
                .map(Response::new),
            Err(_) => Err(crate::error::invalid_field(
                "origin",
                "invalid image origin",
            )),
        }
    }
    async fn create_container(
        &self,
        req: Request<h::CreateContainerRequest>,
    ) -> Result<Response<h::CreateContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .create(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn start_container(
        &self,
        req: Request<h::StartContainerRequest>,
    ) -> Result<Response<h::StartContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .start(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn stop_container(
        &self,
        req: Request<h::StopContainerRequest>,
    ) -> Result<Response<h::StopContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .stop(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn acquire_execution(
        &self,
        req: Request<h::AcquireExecutionRequest>,
    ) -> Result<Response<h::AcquireExecutionResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .acquire(&ctx, req.into_inner())
            .map(Response::new)
    }
    async fn release_execution(
        &self,
        req: Request<h::ReleaseExecutionRequest>,
    ) -> Result<Response<h::ReleaseExecutionResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .release(&ctx, req.into_inner())
            .map(Response::new)
    }
    async fn list_executions(
        &self,
        req: Request<h::ListExecutionsRequest>,
    ) -> Result<Response<h::ListExecutionsResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .executions(&ctx, req.into_inner())
            .map(Response::new)
    }
    async fn quiesce_container(
        &self,
        req: Request<h::QuiesceContainerRequest>,
    ) -> Result<Response<h::QuiesceContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .quiesce(&ctx, req.into_inner())
            .map(Response::new)
    }
    async fn wait_container(
        &self,
        req: Request<h::WaitContainerRequest>,
    ) -> Result<Response<h::WaitContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .wait(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn delete_container(
        &self,
        req: Request<h::DeleteContainerRequest>,
    ) -> Result<Response<h::DeleteContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .delete(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn get_container(
        &self,
        req: Request<h::GetContainerRequest>,
    ) -> Result<Response<h::GetContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .get(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn list_containers(
        &self,
        req: Request<h::ListContainersRequest>,
    ) -> Result<Response<h::ListContainersResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers.list(&ctx).await.map(Response::new)
    }
    async fn logs(
        &self,
        req: Request<h::LogsRequest>,
    ) -> Result<Response<Self::LogsStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        guest::logs(&ctx, self.guest.as_ref(), req.into_inner())
            .await
            .map(Response::new)
    }
    async fn exec(
        &self,
        req: Request<Streaming<h::ExecRequest>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.containers
            .exec(&ctx, self.guest.as_ref(), req.into_inner())
            .await
            .map(Response::new)
    }
    async fn signal_container(
        &self,
        req: Request<h::SignalContainerRequest>,
    ) -> Result<Response<h::SignalContainerResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        guest::signal_container(&ctx, self.guest.clone(), req.into_inner())
            .await
            .map(Response::new)
    }
    async fn create_volume(
        &self,
        req: Request<h::CreateVolumeRequest>,
    ) -> Result<Response<h::CreateVolumeResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        ctx.create_volume(req.into_inner()).await.map(Response::new)
    }
    async fn delete_volume(
        &self,
        req: Request<h::DeleteVolumeRequest>,
    ) -> Result<Response<h::DeleteVolumeResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        ctx.delete_volume(req.into_inner()).await.map(Response::new)
    }
    async fn get_volume(
        &self,
        req: Request<h::GetVolumeRequest>,
    ) -> Result<Response<h::GetVolumeResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        ctx.get_volume(req.into_inner()).map(Response::new)
    }
    async fn list_volumes(
        &self,
        req: Request<h::ListVolumesRequest>,
    ) -> Result<Response<h::ListVolumesResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        ctx.list_volumes().map(Response::new)
    }
    async fn forward_tcp(
        &self,
        req: Request<Streaming<h::ForwardTcpRequest>>,
    ) -> Result<Response<Self::ForwardTCPStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        guest::forward(&ctx, self.guest.as_ref(), req.into_inner())
            .await
            .map(Response::new)
    }
    async fn build_image(
        &self,
        req: Request<Streaming<h::BuildImageRequest>>,
    ) -> Result<Response<Self::BuildImageStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.images
            .build_image(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn copy_image(
        &self,
        req: Request<h::CopyImageRequest>,
    ) -> Result<Response<h::CopyImageResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        crate::image::copy::copy(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn observe_image_operation(
        &self,
        req: Request<h::ObserveImageOperationRequest>,
    ) -> Result<Response<h::ObserveImageOperationResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        if req.get_ref().kind() == h::ImageOperationKind::Copy {
            let receipt = crate::image::copy::observe(&ctx, req.into_inner())?;
            return Ok(Response::new(h::ObserveImageOperationResponse {
                receipt: Some(receipt),
            }));
        }
        self.images
            .observe_image_operation(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn cancel_image_operation(
        &self,
        req: Request<h::CancelImageOperationRequest>,
    ) -> Result<Response<h::CancelImageOperationResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        if req.get_ref().kind() == h::ImageOperationKind::Copy {
            let receipt = crate::image::copy::cancel(&ctx, req.into_inner())?;
            return Ok(Response::new(h::CancelImageOperationResponse {
                receipt: Some(receipt),
            }));
        }
        self.images
            .cancel_image_operation(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn release_session(
        &self,
        req: Request<h::ReleaseSessionRequest>,
    ) -> Result<Response<h::ReleaseSessionResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        ctx.release_session(req.into_inner()).map(Response::new)
    }
    async fn stat_workspace(
        &self,
        req: Request<h::StatWorkspaceRequest>,
    ) -> Result<Response<h::StatWorkspaceResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.workspace
            .stat(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn list_workspace(
        &self,
        req: Request<h::ListWorkspaceRequest>,
    ) -> Result<Response<h::ListWorkspaceResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.workspace
            .list(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn read_workspace(
        &self,
        req: Request<h::ReadWorkspaceRequest>,
    ) -> Result<Response<Self::ReadWorkspaceStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.workspace
            .read(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn write_workspace(
        &self,
        req: Request<Streaming<h::WriteWorkspaceRequest>>,
    ) -> Result<Response<h::WriteWorkspaceResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.workspace
            .write(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn archive_workspace(
        &self,
        req: Request<h::ArchiveWorkspaceRequest>,
    ) -> Result<Response<Self::ArchiveWorkspaceStream>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.workspace
            .archive(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn extract_workspace(
        &self,
        req: Request<Streaming<h::ExtractWorkspaceRequest>>,
    ) -> Result<Response<h::ExtractWorkspaceResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.workspace
            .extract(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn create_service_export(
        &self,
        req: Request<h::CreateServiceExportRequest>,
    ) -> Result<Response<h::CreateServiceExportResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .create_service_export(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn list_service_exports(
        &self,
        req: Request<h::ListServiceExportsRequest>,
    ) -> Result<Response<h::ListServiceExportsResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .list_service_exports(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn revoke_service_export(
        &self,
        req: Request<h::RevokeServiceExportRequest>,
    ) -> Result<Response<h::RevokeServiceExportResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .revoke_service_export(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn create_service_import(
        &self,
        req: Request<h::CreateServiceImportRequest>,
    ) -> Result<Response<h::CreateServiceImportResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .create_service_import(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn create_personal_service_import(
        &self,
        req: Request<h::CreatePersonalServiceImportRequest>,
    ) -> Result<Response<h::CreatePersonalServiceImportResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .create_personal_service_import(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn list_service_imports(
        &self,
        req: Request<h::ListServiceImportsRequest>,
    ) -> Result<Response<h::ListServiceImportsResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .list_service_imports(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
    async fn revoke_service_import(
        &self,
        req: Request<h::RevokeServiceImportRequest>,
    ) -> Result<Response<h::RevokeServiceImportResponse>, Status> {
        let ctx = RuntimeContext::from_request(&self.core, &req)?;
        self.mesh
            .revoke_service_import(&ctx, req.into_inner())
            .await
            .map(Response::new)
    }
}
