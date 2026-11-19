use crate::{
    RuntimeContext,
    container_spec::definition,
    context::{core_status, resource_id},
    error::host_status,
    mutation::mutate,
};
use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use tonic::{Code, Status};
use vmm_core::{BootIdentity, HostShareMount, Sandbox, SandboxKey};
use vmm_guest_proto::rpc::HealthReply;

#[async_trait]
pub trait RuntimeDriver: Send + Sync {
    /// Verify ownership and the effective read/write grant for every host share.
    async fn authorize_shares(&self, scope: &str, shares: &[HostShareMount]) -> Result<(), Status>;
    /// Query the specific VM incarnation and verify the guest's boot proof.
    async fn health(&self, boot: &BootIdentity) -> Result<HealthReply, Status>;
}

#[derive(Clone)]
pub struct Containers {
    pub(crate) driver: Arc<dyn RuntimeDriver>,
    facts: Arc<Mutex<Connection>>,
    pub(crate) definitions: Arc<tokio::sync::Mutex<()>>,
}
pub(crate) struct Facts {
    pub hash: Vec<u8>,
    pub created: i64,
    pub instance: String,
    pub started: i64,
}
pub(crate) fn db_error(_: rusqlite::Error) -> Status {
    Status::internal("container protocol database unavailable")
}
pub(crate) fn fingerprint(mut r: h::CreateContainerRequest) -> [u8; 32] {
    r.request_id.clear();
    Sha256::digest(r.encode_to_vec()).into()
}

impl Containers {
    pub fn open(path: impl AsRef<Path>, driver: Arc<dyn RuntimeDriver>) -> Result<Self, Status> {
        let db = Connection::open(path).map_err(db_error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS container_facts(scope TEXT NOT NULL,sandbox TEXT NOT NULL,generation TEXT NOT NULL,configuration BLOB NOT NULL,created INTEGER NOT NULL,instance TEXT NOT NULL DEFAULT '',started INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,sandbox,generation));").map_err(db_error)?;
        crate::image::grant::initialize(&db)?;
        crate::execution::initialize(&db)?;
        Ok(Self {
            driver,
            facts: Arc::new(Mutex::new(db)),
            definitions: Default::default(),
        })
    }
    pub(crate) fn db(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Status> {
        self.facts
            .lock()
            .map_err(|_| Status::internal("container facts unavailable"))
    }
    pub(crate) fn facts(&self, k: &SandboxKey) -> Result<Option<Facts>, Status> {
        self.db()?.query_row("SELECT configuration,created,instance,started FROM container_facts WHERE scope=? AND sandbox=? AND generation=?",params![k.scope_id,k.sandbox_id,k.generation_id],|r|Ok(Facts{hash:r.get(0)?,created:r.get(1)?,instance:r.get(2)?,started:r.get(3)?})).optional().map_err(db_error)
    }
    pub async fn create(
        &self,
        ctx: &RuntimeContext,
        r: h::CreateContainerRequest,
    ) -> Result<h::CreateContainerResponse, Status> {
        let host = self.clone();
        let owner = ctx.clone();
        mutate(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-create-container",
            r.request_id.clone(),
            r.encode_to_vec(),
            async move {
                let def = definition(&owner, &r)?;
                if !def.host_shares.is_empty() {
                    host.driver
                        .authorize_shares(&owner.scope.id, &def.host_shares)
                        .await?;
                }
                // Core permits historical generations sharing a sandbox ID.
                // The public runtime permits one current definition across all sessions.
                let guard = host.definitions.lock().await;
                if find(&owner, &r.container_id)?.is_some() {
                    return Err(Status::already_exists("container already exists"));
                }
                let key = &def.key;
                host.db()?.execute(
                    "INSERT INTO container_facts(scope,sandbox,generation,configuration,created)
                     VALUES(?,?,?,?,unixepoch())",
                    params![key.scope_id,key.sandbox_id,key.generation_id,fingerprint(r)],
                ).map_err(db_error)?;
                if let Err(error) = owner.core.define_sandbox(&def).await {
                    host.db()?.execute(
                        "DELETE FROM container_facts WHERE scope=? AND sandbox=? AND generation=?",
                        params![key.scope_id,key.sandbox_id,key.generation_id],
                    ).map_err(db_error)?;
                    return Err(core_status(error));
                }
                let sb = owner.core.store.sandbox(key).map_err(core_status)?;
                drop(guard);
                Ok(h::CreateContainerResponse {
                    container: Some(host.observe(&owner.core.store, &sb, None).await?),
                })
            },
        )
        .await
    }
    pub async fn start(
        &self,
        ctx: &RuntimeContext,
        r: h::StartContainerRequest,
    ) -> Result<h::StartContainerResponse, Status> {
        let host = self.clone();
        let owner = ctx.clone();
        mutate(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-start-container",
            r.request_id.clone(),
            r.encode_to_vec(),
            async move {
                let sb = current(&owner, &r.container_id)?;
                let key = &sb.definition.key;
                check_generation(key, &r.expected_generation_id)?;
                if !sb.definition.host_shares.is_empty() {
                    host.driver
                        .authorize_shares(&owner.scope.id, &sb.definition.host_shares)
                        .await?;
                }
                let vm = owner.core.start(key).await.map_err(core_status)?;
                let started = i64::try_from(vm.started_at)
                    .map_err(|_| Status::internal("VM timestamp exceeds host range"))?;
                host.db()?.execute(
                    "UPDATE container_facts SET instance=?,started=? WHERE scope=? AND sandbox=? AND generation=?",
                    params![vm.boot.instance_id, started, key.scope_id, key.sandbox_id, key.generation_id],
                ).map_err(db_error)?;
                let sb = owner.core.store.sandbox(key).map_err(core_status)?;
                Ok(h::StartContainerResponse {
                    container: Some(host.observe(&owner.core.store, &sb, None).await?),
                })
            },
        )
        .await
    }
    pub async fn delete(
        &self,
        ctx: &RuntimeContext,
        r: h::DeleteContainerRequest,
    ) -> Result<h::DeleteContainerResponse, Status> {
        let owner = ctx.clone();
        mutate(
            ctx.core.store.clone(),
            ctx.scope.id.clone(),
            "runtime-delete-container",
            r.request_id.clone(),
            r.encode_to_vec(),
            async move {
                let sb = current(&owner, &r.container_id)?;
                check_generation(&sb.definition.key, &r.expected_generation_id)?;
                owner
                    .core
                    .delete_sandbox(&sb.definition.key)
                    .await
                    .map_err(core_status)?;
                Ok(h::DeleteContainerResponse {})
            },
        )
        .await
    }
    pub async fn get(
        &self,
        ctx: &RuntimeContext,
        r: h::GetContainerRequest,
    ) -> Result<h::GetContainerResponse, Status> {
        let sb = current(ctx, &r.container_id)?;
        Ok(h::GetContainerResponse {
            container: Some(
                self.observe(&ctx.core.store, &sb, r.expected_configuration)
                    .await?,
            ),
        })
    }
    pub async fn list(&self, ctx: &RuntimeContext) -> Result<h::ListContainersResponse, Status> {
        let mut containers = vec![];
        for sb in ctx
            .core
            .store
            .list_sandboxes(&ctx.scope.id, None, usize::MAX)
            .map_err(core_status)?
        {
            containers.push(self.observe(&ctx.core.store, &sb, None).await?);
        }
        Ok(h::ListContainersResponse { containers })
    }
}

fn check_generation(key: &SandboxKey, expected: &str) -> Result<(), Status> {
    if expected.is_empty() {
        return Err(Status::invalid_argument(
            "expected_generation_id is required",
        ));
    }
    if key.generation_id != expected {
        return Err(host_status(
            Code::Aborted,
            h::ErrorReason::ContainerGenerationConflict,
            "container generation changed",
        ));
    }
    Ok(())
}
pub(crate) fn current(ctx: &RuntimeContext, id: &str) -> Result<Sandbox, Status> {
    find(ctx, id)?.ok_or_else(|| Status::not_found("container not found"))
}
pub(crate) fn find(ctx: &RuntimeContext, id: &str) -> Result<Option<Sandbox>, Status> {
    find_in_scope(&ctx.core.store, &ctx.scope.id, id)
}

pub(crate) fn find_in_scope(
    store: &vmm_core::Store,
    scope: &str,
    id: &str,
) -> Result<Option<Sandbox>, Status> {
    resource_id(id)?;
    Ok(store
        .list_sandboxes(scope, None, usize::MAX)
        .map_err(core_status)?
        .into_iter()
        .find(|s| s.definition.key.sandbox_id == id))
}
