//! `preflight --json` checks the host volume before a new installation. The
//! state pool is sparse, so a new Host needs the emergency reserve and room for
//! the base images, not the virtual size of the pool.

use crate::error::{Result, err};
use serde::Serialize;
use std::path::Path;

/// Stays free on the host volume of the state pool.
pub const EMERGENCY_BYTES: u64 = 10 << 30;
/// Room for the base images that the first workloads install.
pub const BASE_IMAGE_BYTES: u64 = 4 << 30;
/// The pool of this Host generation, and the preallocated disk of the Go Host.
pub const STATE_DISKS: [&str; 2] = ["instance/pool.raw", "instance/state.raw"];

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Issue {
    pub code: &'static str,
    pub required_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    pub ok: bool,
    pub issues: Vec<Issue>,
}

/// An installation that has a state disk already owns its space.
pub fn required_bytes(data: &Path) -> u64 {
    if STATE_DISKS.iter().any(|disk| data.join(disk).symlink_metadata().is_ok()) { 0 } else { EMERGENCY_BYTES + BASE_IMAGE_BYTES }
}

/// Measures the volume of `directory`, or of its nearest existing parent before the first install.
pub fn check(directory: &Path, required: u64) -> Result<Report> {
    let mut existing = directory;
    let stats = loop {
        match rustix::fs::statvfs(existing) {
            Ok(stats) => break stats,
            Err(rustix::io::Errno::NOENT) => existing = existing.parent().ok_or_else(|| err("preflight found no existing directory"))?,
            Err(error) => return Err(error.into()),
        }
    };
    // Electron reads the numbers as JSON numbers, so they stay below 2^53.
    let available = stats.f_bavail.saturating_mul(stats.f_frsize).min((1 << 53) - 1);
    let issues = if available >= required { Vec::new() } else { vec![Issue { code: "insufficient_disk", required_bytes: required, available_bytes: available }] };
    Ok(Report { ok: issues.is_empty(), issues })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_installation_needs_the_reserve_and_not_the_pool_size() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("missing").join("AgentVMM");
        assert_eq!(required_bytes(&data), 14 << 30);
        assert_eq!(check(&data, 1).unwrap(), Report { ok: true, issues: Vec::new() });
        let report = check(&data, u64::MAX).unwrap();
        assert!(!report.ok);
        let encoded = serde_json::to_value(&report).unwrap();
        assert_eq!(encoded["issues"][0]["code"], "insufficient_disk");
        assert!(encoded["issues"][0]["requiredBytes"].is_u64() && encoded["issues"][0]["availableBytes"].is_u64());

        std::fs::create_dir_all(data.join("instance")).unwrap();
        std::fs::write(data.join("instance/pool.raw"), b"").unwrap();
        assert_eq!(required_bytes(&data), 0);
    }
}
