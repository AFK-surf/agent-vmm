//! Durable key rotation for the built-in MCP service and its appliance watch RPC.
use agent_vmm_proto::appliance::v1 as a;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{path::Path, sync::Mutex, time::SystemTime};
use vmm_rpc::Status;
use vmm_mcp::TokenAuthority;

const INSTALLATION: &str = "agent-vmm-mcp";
pub struct McpManagement {
    authority: TokenAuthority,
    frontend: std::sync::OnceLock<(std::net::SocketAddr, String)>,
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
            frontend: Default::default(),
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
    pub fn publish_frontend(
        &self,
        bound: std::net::SocketAddr,
        registration: &str,
    ) -> Result<(), Status> {
        if !bound.ip().is_loopback() || bound.port() == 0 || registration.is_empty() {
            return Err(Status::internal("invalid bound MCP frontend"));
        }
        self.frontend
            .set((bound, registration.to_owned()))
            .map_err(|_| Status::already_exists("MCP frontend is bound"))
    }
    fn builtin_status(&self) -> a::ApplianceStatus {
        let live = self.frontend.get();
        let running = live.is_some();
        let boot = if running {
            super::BOOT_ID.to_string()
        } else {
            String::new()
        };
        a::ApplianceStatus {
            installation_id: INSTALLATION.into(),
            product_id: INSTALLATION.into(),
            revision: 1,
            enabled: true,
            runtime_stopped: Some(!running),
            registration_id: live.map(|(_, id)| id.clone()).unwrap_or_default(),
            boot_id: boot.clone(),
            appliance_state: if running {
                a::ApplianceState::Running
            } else {
                a::ApplianceState::Stopped
            } as i32,
            channel_state: if running {
                a::ControllerChannelState::Active
            } else {
                a::ControllerChannelState::Disconnected
            } as i32,
            health: Some(a::ApplianceHealth {
                installation_id: INSTALLATION.into(),
                boot_id: boot,
                healthy: running,
                config_revision: 1,
                ..Default::default()
            }),
            frontends: vec![a::ApplianceFrontendStatus {
                name: "mcp-http".into(),
                transport: a::FrontendTransport::Http as i32,
                exposure: a::FrontendExposure::HostLoopback as i32,
                endpoint: live.map(|(addr, _)| addr.to_string()).unwrap_or_default(),
                available: running,
                ..Default::default()
            }],
            ..Default::default()
        }
    }
    pub(super) fn list_appliances(
        &self,
        _: a::ListAppliancesRequest,
    ) -> Result<a::ListAppliancesResponse, Status> {
        Ok(a::ListAppliancesResponse {
            appliances: vec![self.builtin_status()],
        })
    }
    pub(super) fn get_status(
        &self,
        r: a::GetApplianceStatusRequest,
    ) -> Result<a::GetApplianceStatusResponse, Status> {
        if r.installation_id != INSTALLATION {
            return Err(Status::not_found("appliance installation"));
        }
        Ok(a::GetApplianceStatusResponse {
            status: Some(self.builtin_status()),
        })
    }
    pub fn token_authority(&self) -> TokenAuthority {
        self.authority.clone()
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
    pub(super) fn watch(
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
