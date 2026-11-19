//! Operator-only appliance administration.
use super::{McpManagement, content::ContentStore};
use agent_vmm_proto::appliance::v1 as a;
use std::{path::Path, sync::Arc};
use vmm_rpc::{Response, Status};
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
#[vmm_rpc::async_trait]
impl vmm_rpc::Service for ApplianceOperations {
    fn name(&self) -> &'static str {
        "agentvmm.appliance.v1.ApplianceAdminService"
    }
    async fn call(
        &self,
        method: &str,
        request: vmm_rpc::Request<vmm_rpc::Body>,
    ) -> Result<vmm_rpc::Body, Status> {
        match method {
            "PlanInstall" => Ok(Response::new(
                self.content
                    .plan_install(request.unary::<a::PlanInstallRequest>().await?.into_inner())?,
            )
            .unary_body()),
            "Install" => Ok(Response::new(
                self.content
                    .install(request.unary::<a::InstallRequest>().await?.into_inner())
                    .await?,
            )
            .unary_body()),
            "ListAppliances" => Ok(Response::new(
                self.list_appliances(
                    request
                        .unary::<a::ListAppliancesRequest>()
                        .await?
                        .into_inner(),
                )?,
            )
            .unary_body()),
            "GetApplianceStatus" => Ok(Response::new(
                self.get_status(
                    request
                        .unary::<a::GetApplianceStatusRequest>()
                        .await?
                        .into_inner(),
                )?,
            )
            .unary_body()),
            "GetContentStatus" => Ok(Response::new(
                self.content.status(
                    request
                        .unary::<a::GetContentStatusRequest>()
                        .await?
                        .into_inner(),
                )?,
            )
            .unary_body()),
            "StageArtifact" => Ok(Response::new(
                self.content
                    .stage_artifact(
                        request
                            .unary::<a::StageArtifactRequest>()
                            .await?
                            .into_inner(),
                    )
                    .await?,
            )
            .unary_body()),
            "StageContent" => Ok(Response::new(
                self.content.stage(request.streaming().into_inner()).await?,
            )
            .unary_body()),
            "WatchApplianceOperation" => {
                let owner = self.clone();
                let r = request
                    .unary::<a::WatchApplianceOperationRequest>()
                    .await?
                    .into_inner();
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
                Ok(vmm_rpc::Body::encode(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                ))
            }
            _ => Err(Status::unimplemented("unknown appliance method")),
        }
    }
}
