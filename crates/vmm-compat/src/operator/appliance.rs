//! Operator-only appliance administration.
use super::{McpManagement, content::ContentStore};
use agent_vmm_proto::appliance::v1 as a;
use std::{path::Path, sync::Arc};
use tonic::{Response, Status};
#[derive(Clone)]
pub struct ApplianceOperations {
    mcp: Arc<McpManagement>,
    content: Arc<ContentStore>,
}
impl ApplianceOperations {
    pub fn open(
        mcp: Arc<McpManagement>,
        db: &Path,
        root: &Path,
        core: Arc<vmm_core::Store>,
        secrets: Arc<dyn vmm_signing::CredentialStore>,
        workload_disk: u64,
    ) -> Result<Self, Status> {
        Ok(Self {
            mcp,
            content: Arc::new(ContentStore::open(db, root, core, secrets, workload_disk)?),
        })
    }
    fn list_appliances(
        &self,
        req: a::ListAppliancesRequest,
    ) -> Result<a::ListAppliancesResponse, Status> {
        let mut result = self.mcp.list_appliances(req)?;
        result.appliances.extend(self.content.statuses(None)?);
        result
            .appliances
            .sort_by(|a, b| a.installation_id.cmp(&b.installation_id));
        Ok(result)
    }
    fn get_status(
        &self,
        req: a::GetApplianceStatusRequest,
    ) -> Result<a::GetApplianceStatusResponse, Status> {
        if req.installation_id == "agent-vmm-mcp" {
            return self.mcp.get_status(req);
        }
        let status = self
            .content
            .statuses(Some(&req.installation_id))?
            .pop()
            .ok_or_else(|| Status::not_found("appliance installation"))?;
        Ok(a::GetApplianceStatusResponse {
            status: Some(status),
        })
    }
}
use tonic::codegen::{Body, BoxFuture, Service, StdError, http};
impl tonic::server::NamedService for ApplianceOperations {
    const NAME: &'static str = "agentvmm.appliance.v1.ApplianceAdminService";
}
impl<B: Body + Send + 'static> Service<http::Request<B>> for ApplianceOperations
where
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        macro_rules! owner {
            (self) => {
                Arc::new(self.clone())
            };
            ($field:ident) => {
                self.$field.clone()
            };
        }
        macro_rules! unary {
            ($field:ident,$owner:ty,$method:ident,$request:ty,$response:ty $(,$await:ident)?) => {{
                struct Handler(Arc<$owner>);
                impl tonic::server::UnaryService<$request> for Handler {
                    type Response = $response;
                    type Future = BoxFuture<Response<Self::Response>, Status>;
                    fn call(&mut self, r: tonic::Request<$request>) -> Self::Future {
                        let owner = self.0.clone();
                        Box::pin(async move { owner.$method(r.into_inner())$(.$await)?.map(Response::new) })
                    }
                }
                let handler = Handler(owner!($field));
                return Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(handler, req)
                        .await)
                });
            }};
        }
        match req
            .uri()
            .path()
            .strip_prefix("/agentvmm.appliance.v1.ApplianceAdminService/")
        {
            Some("PlanInstall") => unary!(
                content,
                ContentStore,
                plan_install,
                a::PlanInstallRequest,
                a::PlanInstallResponse
            ),
            Some("Install") => unary!(
                content,
                ContentStore,
                install,
                a::InstallRequest,
                a::InstallResponse,
                await
            ),
            Some("ListAppliances") => unary!(
                self,
                ApplianceOperations,
                list_appliances,
                a::ListAppliancesRequest,
                a::ListAppliancesResponse
            ),
            Some("GetApplianceStatus") => unary!(
                self,
                ApplianceOperations,
                get_status,
                a::GetApplianceStatusRequest,
                a::GetApplianceStatusResponse
            ),
            Some("GetContentStatus") => unary!(
                content,
                ContentStore,
                status,
                a::GetContentStatusRequest,
                a::GetContentStatusResponse
            ),
            Some("StageArtifact") => unary!(
                content,
                ContentStore,
                stage_artifact,
                a::StageArtifactRequest,
                a::StageArtifactResponse,
                await
            ),
            Some("StageContent") => {
                struct Handler(Arc<ContentStore>);
                impl tonic::server::ClientStreamingService<a::StageContentRequest> for Handler {
                    type Response = a::StageContentResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;
                    fn call(
                        &mut self,
                        r: tonic::Request<tonic::Streaming<a::StageContentRequest>>,
                    ) -> Self::Future {
                        let owner = self.0.clone();
                        Box::pin(
                            async move { owner.stage(r.into_inner()).await.map(Response::new) },
                        )
                    }
                }
                let handler = Handler(self.content.clone());
                return Box::pin(async move {
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .client_streaming(handler, req)
                        .await)
                });
            }
            Some("WatchApplianceOperation") => {}
            _ => {
                return Box::pin(async {
                    Ok(Status::unimplemented("unknown appliance method").into_http())
                });
            }
        }
        struct Watch(ApplianceOperations);
        impl tonic::server::ServerStreamingService<a::WatchApplianceOperationRequest> for Watch {
            type Response = a::WatchApplianceOperationResponse;
            type ResponseStream =
                tokio_stream::wrappers::ReceiverStream<Result<Self::Response, Status>>;
            type Future = BoxFuture<Response<Self::ResponseStream>, Status>;
            fn call(
                &mut self,
                req: tonic::Request<a::WatchApplianceOperationRequest>,
            ) -> Self::Future {
                let owner = self.0.clone();
                Box::pin(async move {
                    let r = req.into_inner();
                    let id = r.operation_id.clone();
                    let result = if r.installation_id == "agent-vmm-mcp" {
                        owner.mcp.watch(r)?
                    } else {
                        let response = owner.content.watch(&id)?;
                        if response
                            .operation
                            .as_ref()
                            .is_none_or(|o| o.installation_id != r.installation_id)
                        {
                            return Err(Status::not_found("appliance operation"));
                        }
                        response
                    };
                    let running = result
                        .operation
                        .as_ref()
                        .is_some_and(|op| op.state() == a::ApplianceOperationState::Running);
                    let (tx, rx) = tokio::sync::mpsc::channel(2);
                    tx.send(Ok(result)).await.expect("stream receiver exists");
                    if running {
                        tokio::spawn(async move {
                            tokio::select! {
                                _=tx.closed()=>{},
                                terminal=owner.content.terminal(&id)=>{let _=tx.send(terminal).await;}
                            }
                        });
                    }
                    Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
                        rx,
                    )))
                })
            }
        }
        let handler = Watch(self.clone());
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .server_streaming(handler, req)
                .await)
        })
    }
}
