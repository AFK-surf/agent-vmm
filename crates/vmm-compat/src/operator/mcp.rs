//! Durable key rotation for the built-in MCP service and its appliance watch RPC.
use agent_vmm_proto::appliance::v1 as a;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tonic::{Response, Status};
use vmm_mcp::TokenAuthority;

const INSTALLATION: &str = "agent-vmm-mcp";
pub struct McpManagement {
    authority: TokenAuthority,
    db: Mutex<Connection>,
}
fn storage(_: rusqlite::Error) -> Status {
    Status::unavailable("MCP operation store unavailable")
}
fn key(_: std::io::Error) -> Status {
    Status::unavailable("MCP signing key unavailable")
}

impl McpManagement {
    pub fn open(path: &Path, authority: TokenAuthority) -> Result<Self, Status> {
        let db = Connection::open(path).map_err(storage)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS mcp_rotations(id TEXT PRIMARY KEY, prior BLOB, operation BLOB NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS mcp_rotation_pending ON mcp_rotations((1)) WHERE prior IS NOT NULL;").map_err(storage)?;
        let owner = Self {
            db: Mutex::new(db),
            authority,
        };
        {
            let db = owner.lock()?;
            owner.finish_pending(&db)?;
        }
        Ok(owner)
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Status> {
        self.db
            .lock()
            .map_err(|_| Status::unavailable("MCP operation store unavailable"))
    }
    fn finish_pending(&self, db: &Connection) -> Result<(), Status> {
        let pending = db
            .query_row(
                "SELECT id,prior,operation FROM mcp_rotations WHERE prior IS NOT NULL",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(storage)?;
        if let Some((id, prior, bytes)) = pending {
            let mut operation = decode(&bytes)?;
            if prior.len() != 32
                || operation.operation_id != id
                || operation.installation_id != INSTALLATION
            {
                return Err(Status::data_loss("invalid pending rotation receipt"));
            }
            if prior.as_slice() == self.authority.key_id().map_err(key)? {
                self.authority.rotate().map_err(key)?;
            }
            operation.state = a::ApplianceOperationState::Succeeded as i32;
            operation.phase = a::ApplianceOperationPhase::Terminal as i32;
            operation.terminal_reason = a::ApplianceTerminalReason::Succeeded as i32;
            operation.updated_at = Some(SystemTime::now().into());
            db.execute(
                "UPDATE mcp_rotations SET prior=NULL,operation=? WHERE id=?",
                params![operation.encode_to_vec(), id],
            )
            .map_err(storage)?;
        }
        Ok(())
    }
    pub fn issue(&self) -> Result<String, Status> {
        let db = self.lock()?;
        self.finish_pending(&db)?;
        self.authority.issue().map_err(key)
    }
    pub fn rotate(&self) -> Result<String, Status> {
        let db = self.lock()?;
        self.finish_pending(&db)?;
        let prior = self.authority.key_id().map_err(key)?;
        let operation_id = uuid::Uuid::new_v4().to_string();
        let now = Some(SystemTime::now().into());
        let operation = a::ApplianceOperation {
            operation_id: operation_id.clone(),
            installation_id: INSTALLATION.into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            kind: a::ApplianceOperationKind::Update as i32,
            phase: a::ApplianceOperationPhase::Switching as i32,
            state: a::ApplianceOperationState::Running as i32,
            started_at: now,
            updated_at: now,
            ..Default::default()
        };
        // The key store and SQLite cannot share a transaction. The old key ID distinguishes
        // a crash before replacement from a crash after replacement but before completion.
        db.execute(
            "INSERT INTO mcp_rotations(id,prior,operation) VALUES(?,?,?)",
            params![operation_id, prior.as_slice(), operation.encode_to_vec()],
        )
        .map_err(storage)?;
        self.finish_pending(&db)?;
        Ok(operation_id)
    }
    fn watch(
        &self,
        req: a::WatchApplianceOperationRequest,
    ) -> Result<a::WatchApplianceOperationResponse, Status> {
        if req.installation_id != INSTALLATION {
            return Err(Status::not_found("appliance operation"));
        }
        let db = self.lock()?;
        self.finish_pending(&db)?;
        let bytes = db
            .query_row(
                "SELECT operation FROM mcp_rotations WHERE id=?",
                [req.operation_id],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(storage)?
            .ok_or_else(|| Status::not_found("appliance operation"))?;
        Ok(a::WatchApplianceOperationResponse {
            operation: Some(decode(&bytes)?),
            artifact_id: String::new(),
        })
    }
}
fn decode(bytes: &[u8]) -> Result<a::ApplianceOperation, Status> {
    a::ApplianceOperation::decode(bytes).map_err(|_| Status::data_loss("invalid rotation receipt"))
}

#[derive(Clone)]
pub struct ApplianceOperations(pub Arc<McpManagement>);
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
        if req.uri().path()
            != "/agentvmm.appliance.v1.ApplianceAdminService/WatchApplianceOperation"
        {
            return Box::pin(async {
                Ok(Status::unimplemented("unknown appliance method").into_http())
            });
        }
        struct Watch(Arc<McpManagement>);
        impl tonic::server::ServerStreamingService<a::WatchApplianceOperationRequest> for Watch {
            type Response = a::WatchApplianceOperationResponse;
            type ResponseStream = tokio_stream::Once<Result<Self::Response, Status>>;
            type Future = BoxFuture<Response<Self::ResponseStream>, Status>;
            fn call(
                &mut self,
                req: tonic::Request<a::WatchApplianceOperationRequest>,
            ) -> Self::Future {
                let owner = self.0.clone();
                Box::pin(async move {
                    let result = tokio::task::spawn_blocking(move || owner.watch(req.into_inner()))
                        .await
                        .map_err(|_| Status::unavailable("operation store unavailable"))??;
                    Ok(Response::new(tokio_stream::once(Ok(result))))
                })
            }
        }
        let handler = Watch(self.0.clone());
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .server_streaming(handler, req)
                .await)
        })
    }
}
