//! Golden contract suite for the Comma/Salix consumers of agent-vmm.
//!
//! Each module is an executable model of one consumer in `AFK-surf/Cue`
//! (pinned at `de4b430da7`). The models read the same JSON that the real
//! consumer reads and return the consumer's verdict, so a Rust rewrite of
//! agent-vmm can prove its outputs against them without a running Comma.
//!
//! * [`consumer`]: Electron Main's parsers of `agent-vmm-lifecycle` output.
//! * [`descriptor`]: descriptors and HTTP bodies exchanged with Salix.
//! * [`signing`]: the `agent-vmm/signature/v1` vectors Salix verifies.
//! * [`protolock`]: the pinned protobuf surface and the names Cue uses.
//!
//! Each vector in `golden_vectors` cites the Cue `file:line` of its rule.

pub mod consumer;
pub mod descriptor;
pub mod protolock;
pub mod signing;

use std::path::PathBuf;

/// Directory of the golden vectors: `$VMM_COMMA_GOLDEN_DIR`, else `golden_vectors` in this crate.
pub fn golden_dir() -> PathBuf {
    match std::env::var_os("VMM_COMMA_GOLDEN_DIR") {
        Some(value) => PathBuf::from(value),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden_vectors"),
    }
}

/// Reads and parses one golden JSON file.
pub fn golden(relative: &str) -> serde_json::Value {
    let path = golden_dir().join(relative);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
