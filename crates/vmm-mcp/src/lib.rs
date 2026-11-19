//! Host-side HTTP authentication for the built-in MCP service.
mod auth;
mod file_keys;

pub use auth::{Binding, SigningKeys, TokenAuthority};
pub use file_keys::FileSigningKeys;

mod http;
pub use http::{ToolFailure, ToolRuntime, router};
