use crate::{
    Connector, Error, Result,
    remote::{OpenSession, SessionReady},
    unix_millis,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;
use vmm_core::{Channel, RemoteIdentity, Store};
use vmm_rpc::{Body, Metadata, Request, Router, Service, Status};
use zeroize::Zeroizing;

const LIMIT: usize = 32;
const TTL: Duration = Duration::from_secs(8 * 60 * 60);

struct Active {
    store: Arc<Store>,
    identity: RemoteIdentity,
    capability: Zeroizing<[u8; 16]>,
    command_id: String,
    ready: SessionReady,
    revoked: CancellationToken,
}

impl Drop for Active {
    fn drop(&mut self) {
        self.revoked.cancel();
        let _ = self.store.revoke_session(&self.identity.session_id);
    }
}

type Table = Arc<Mutex<HashMap<String, Active>>>;

pub(crate) struct Sessions<'a> {
    connector: &'a Connector,
    channel: &'a Channel,
    table: Table,
}

impl Drop for Sessions<'_> {
    fn drop(&mut self) {
        self.table.lock().unwrap().clear();
    }
}

impl<'a> Sessions<'a> {
    pub(crate) fn new(connector: &'a Connector, channel: &'a Channel) -> Self {
        Self {
            connector,
            channel,
            table: Table::default(),
        }
    }
    pub(crate) fn router(&self) -> Router {
        match self.connector.config.runtime.clone() {
            Some(runtime) => Router::new(Runtime {
                table: self.table.clone(),
                runtime,
            }),
            None => Router::default(),
        }
    }
    pub(crate) fn close(&mut self, allocation: &str) -> Result<()> {
        if let Some(entry) = self.table.lock().unwrap().remove(allocation) {
            entry.store.revoke_session(&entry.identity.session_id)?;
        }
        Ok(())
    }
    pub(crate) fn replay(&self, command: &str, allocation: &str) -> Option<SessionReady> {
        let table = self.table.lock().unwrap();
        let entry = table.get(allocation)?;
        (entry.command_id == command && entry.ready.expires_unix_millis > unix_millis())
            .then(|| entry.ready.clone())
    }
    pub(crate) fn open(&mut self, command: &str, request: &OpenSession) -> Result<SessionReady> {
        if self.connector.config.runtime.is_none() {
            return Err(Error::Runtime);
        }
        let mut table = self.table.lock().unwrap();
        table.retain(|_, entry| entry.ready.expires_unix_millis > unix_millis());
        if table.len() == LIMIT && !table.contains_key(&request.allocation_id) {
            return Err(
                vmm_core::Error::Capacity(vmm_core::CapacityDimension::ComputeSlots).into(),
            );
        }
        let mut random = Zeroizing::new([0u8; 16]);
        getrandom::fill(random.as_mut()).map_err(|_| Error::Random)?;
        let store = &self.connector.core.store;
        let (session, capability) = store.issue_remote_session(
            self.channel,
            &request.allocation_id,
            request.allocation_generation,
        )?;
        let ready = SessionReady {
            allocation_id: request.allocation_id.clone(),
            allocation_generation: request.allocation_generation,
            tunnel_nonce: hex::encode(*random),
            expires_unix_millis: unix_millis().saturating_add(TTL.as_millis() as i64),
        };
        table.insert(
            request.allocation_id.clone(),
            Active {
                store: store.clone(),
                identity: RemoteIdentity {
                    session_id: session.id,
                    registration_id: self.channel.registration_id.clone(),
                    allocation_id: request.allocation_id.clone(),
                    generation: request.allocation_generation,
                    execution_owner_id: request.execution_owner_id.clone(),
                },
                capability: Zeroizing::new(capability),
                command_id: command.into(),
                ready: ready.clone(),
                revoked: CancellationToken::new(),
            },
        );
        Ok(ready)
    }
}

struct Runtime {
    table: Table,
    runtime: Arc<dyn Service>,
}

fn denied() -> Status {
    Status::unauthenticated("invalid runtime session")
}
fn one<'a>(metadata: &'a Metadata, key: &'static str) -> std::result::Result<&'a str, Status> {
    let mut values = metadata.get_all(key);
    let value = values.next().ok_or_else(denied)?;
    if values.next().is_some() {
        return Err(denied());
    }
    Ok(value)
}

#[vmm_rpc::async_trait]
impl Service for Runtime {
    fn name(&self) -> &'static str {
        "agentvmm.host.v1.AgentRuntimeService"
    }
    async fn call(
        &self,
        method: &str,
        mut request: Request<Body>,
    ) -> std::result::Result<Body, Status> {
        let (identity, session, revoked, deadline) = {
            let meta = request.metadata();
            let allocation = one(meta, "x-agentvmm-allocation-id")?;
            let generation = one(meta, "x-agentvmm-allocation-generation")?;
            let nonce = one(meta, "authorization")?
                .strip_prefix("Bearer ")
                .ok_or_else(denied)?;
            let table = self.table.lock().unwrap();
            let active = table.get(allocation).ok_or_else(denied)?;
            let remaining = active
                .ready
                .expires_unix_millis
                .saturating_sub(unix_millis());
            if remaining <= 0
                || generation != active.ready.allocation_generation.to_string()
                || !bool::from(active.ready.tunnel_nonce.as_bytes().ct_eq(nonce.as_bytes()))
            {
                return Err(denied());
            }
            let session = active
                .store
                .authenticate_session(&active.identity.session_id, active.capability.as_slice())
                .map_err(|_| denied())?;
            (
                active.identity.clone(),
                session,
                active.revoked.clone(),
                tokio::time::Instant::now() + Duration::from_millis(remaining as u64),
            )
        };
        request.extensions_mut().insert(identity);
        request.extensions_mut().insert(session);
        let cancel = move || {
            let revoked = revoked.clone();
            async move {
                tokio::select! { _ = revoked.cancelled() => {}, _ = tokio::time::sleep_until(deadline) => {} }
                denied()
            }
        };
        let request = request.map(|body| body.until_error(cancel()));
        let response = tokio::select! {
            biased;
            status = cancel() => return Err(status),
            result = self.runtime.call(method, request) => result?,
        };
        Ok(response.until_error(cancel()))
    }
}
