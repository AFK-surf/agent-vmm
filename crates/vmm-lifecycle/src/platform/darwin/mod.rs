//! macOS: launchd jobs of one service user, and maintenance of the shared Host bundle.

mod actions;
mod carrier;
mod commands;
mod disk;
mod launchd;
mod maintenance;
mod maintenance_journal;
mod paths;
mod plist;
mod publish;
mod service;

pub use actions::host_release_id as release_identity;
pub use commands::execute_platform_command;
pub use launchd::{host_installed, host_job, observe_host_service};
pub use paths::{lifecycle_paths, observation_paths, preflight_data_directory};
pub use service::{drain_service, reconcile_service, remove_service};

use crate::error::{Result, err};
use crate::{Context, Paths};
use std::path::Path;

/// A lifecycle mutation changes the state of the service user. Root runs only the fixed service executor.
pub fn check_mutation_user(euid: u32) -> Result<()> {
    if euid == 0 {
        return Err(err("lifecycle business mutations must run as the configured non-root service user; use agent-vmm-service-executor only for fixed system jobs"));
    }
    Ok(())
}

pub fn validate_prepared_update(paths: &Paths, source: &Path) -> Result<()> {
    if source.parent() != paths.app.parent() || source == paths.app {
        return Err(err("prepared update must be a hidden sibling of the installed Host app"));
    }
    Ok(())
}

pub fn update_application(context: &Context, paths: &Paths, source: &Path, release_id: &str, request_id: &str) -> Result<()> {
    let mut actions = actions::Update { context, paths, release_id, request_id };
    crate::update::run(context, paths, source, release_id, request_id, &mut actions)
}
