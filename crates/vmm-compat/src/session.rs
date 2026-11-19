use crate::error::detailed_status;
use base64::{Engine, engine::general_purpose::STANDARD};
use std::sync::Arc;
use tonic::{Code, Request, Status, service::Interceptor};
use vmm_core::{Error, Store};
use zeroize::Zeroizing;

/// Metadata supplied by the authenticated control tunnel when it opens a session.
#[derive(Clone)]
pub struct RemoteIdentity {
    pub session_id: String,
    pub registration_id: String,
    pub allocation_id: String,
    pub generation: u64,
    pub execution_owner_id: String,
}

#[derive(Clone)]
pub struct RuntimeAuth {
    store: Arc<Store>,
    session_id: String,
    remote: Option<RemoteIdentity>,
    transport_token: Option<Arc<Zeroizing<[u8; 16]>>>,
}

impl RuntimeAuth {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The caller owns a private, per-session listener or an authenticated
    /// ServeSession tunnel. Legacy inner RPCs carry no Authorization header.
    pub fn bound(store: Arc<Store>, session_id: String, capability: Zeroizing<[u8; 16]>) -> Self {
        Self {
            store,
            remote: None,
            session_id,
            transport_token: Some(Arc::new(capability)),
        }
    }

    pub fn for_remote(mut self, remote: RemoteIdentity) -> Self {
        self.remote = Some(remote);
        self
    }

    /// A listener requiring an explicit bearer never accepts a missing header.
    pub fn bearer(store: Arc<Store>, session_id: String) -> Self {
        Self {
            store,
            remote: None,
            session_id,
            transport_token: None,
        }
    }
}

fn invalid_token() -> Status {
    detailed_status(
        Code::Unauthenticated,
        "SESSION_TOKEN_INVALID",
        "invalid runtime session capability",
        Default::default(),
    )
}

impl Interceptor for RuntimeAuth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let mut headers = req.metadata().get_all("authorization").iter();
        let explicit;
        let token = if let Some(header) = headers.next() {
            let encoded = header
                .to_str()
                .ok()
                .and_then(|v| v.strip_prefix("Bearer "))
                .ok_or_else(invalid_token)?;
            if encoded.len() != 24 || headers.next().is_some() {
                return Err(invalid_token());
            }
            explicit = Zeroizing::new(STANDARD.decode(encoded).map_err(|_| invalid_token())?);
            explicit.as_slice()
        } else {
            self.transport_token
                .as_ref()
                .ok_or_else(invalid_token)?
                .as_slice()
        };
        let session = self
            .store
            .authenticate_session(&self.session_id, token)
            .map_err(|error| match error {
                Error::Fenced => invalid_token(),
                _ => Status::unavailable("runtime authority store unavailable"),
            })?;
        if let Some(remote) = &self.remote {
            if remote.session_id != session.id || remote.allocation_id.is_empty() {
                return Err(invalid_token());
            }
            req.extensions_mut().insert(remote.clone());
        }
        req.extensions_mut().insert(session);
        Ok(req)
    }
}
