use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{io, sync::Arc};
use zeroize::Zeroizing;

/// Selected by the daemon according to the installation's existing secret backend.
pub trait SigningKeys: Send + Sync {
    fn current(&self) -> io::Result<Zeroizing<[u8; 32]>>;
    fn replace(&self, key: &[u8; 32]) -> io::Result<()>;
}

#[derive(Clone)]
pub struct TokenAuthority(Arc<dyn SigningKeys>);

/// The allocation owned by one authenticated MCP token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub allocation_id: String,
}

impl TokenAuthority {
    pub fn new(keys: Arc<dyn SigningKeys>) -> Self {
        Self(keys)
    }

    pub fn issue(&self) -> io::Result<String> {
        let mut raw = Zeroizing::new([0u8; 32]);
        getrandom::fill(&mut raw[..16]).map_err(io::Error::other)?;
        let key = self.0.current()?;
        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_slice()).expect("valid HMAC key");
        mac.update(&raw[..16]);
        raw[16..].copy_from_slice(&mac.finalize().into_bytes()[..16]);
        Ok(hex::encode(raw.as_slice()))
    }

    /// Fingerprint used to recover an interrupted key rotation without storing a signing key.
    pub fn key_id(&self) -> io::Result<[u8; 32]> {
        let key = self.0.current()?;
        Ok(Sha256::digest(key.as_slice()).into())
    }

    pub fn rotate(&self) -> io::Result<()> {
        let mut key = Zeroizing::new([0u8; 32]);
        getrandom::fill(key.as_mut_slice()).map_err(io::Error::other)?;
        self.0.replace(&key)
    }

    pub fn verify(&self, token: &str) -> io::Result<Option<Binding>> {
        let mut raw = Zeroizing::new([0u8; 32]);
        if token.bytes().any(|b| b.is_ascii_uppercase())
            || hex::decode_to_slice(token, raw.as_mut_slice()).is_err()
        {
            return Ok(None);
        }
        let key = self.0.current()?;
        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_slice()).expect("valid HMAC key");
        mac.update(&raw[..16]);
        if mac.verify_truncated_left(&raw[16..]).is_err() {
            return Ok(None);
        }
        Ok(Some(Binding {
            allocation_id: format!("mcp-{}", hex::encode(Sha256::digest(token.as_bytes()))),
        }))
    }

    pub fn protect(&self, router: Router) -> Router {
        router.route_layer(middleware::from_fn_with_state(self.clone(), authenticate))
    }
}

async fn authenticate(
    State(authority): State<TokenAuthority>,
    mut request: Request,
    next: Next,
) -> Response {
    let mut headers = request.headers().get_all(header::AUTHORIZATION).iter();
    let token = headers
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
        .filter(|_| headers.next().is_none())
        .map(|(_, token)| token.to_owned());

    if let Some(token) = token {
        // The selected backend can be a keychain or a regular file.
        let checked = tokio::task::spawn_blocking(move || authority.verify(&token)).await;
        match checked {
            Ok(Ok(Some(binding))) => {
                request.extensions_mut().insert(binding);
                return next.run(request).await;
            }
            Ok(Ok(None)) => (),
            _ => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
    )
        .into_response()
}
