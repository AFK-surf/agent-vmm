//! The production effects of maintenance and update: `ditto`, `codesign`,
//! launchd, and an atomic exchange of the application bundle.

use super::carrier;
use super::launchd::Job;
use super::maintenance::{Actions, MaintenanceRequest};
use super::service::{self, Scope};
use crate::error::{Deadline, Result, err};
use crate::schema::{self, DataSchema};
use crate::update::UpdateActions;
use crate::{Context, HOST_STOP_TIMEOUT, Paths, cutover, status};
use rustix::fs::{CWD, RenameFlags};
use std::path::{Path, PathBuf};
use std::time::Duration;

const COPY_TIMEOUT: Duration = Duration::from_secs(600);

/// Reads the release identity from the inventory of a Host bundle.
pub fn host_release_id(app: &Path) -> Result<String> {
    let invalid = || err("host release inventory is invalid");
    let inventory: serde_json::Value = serde_json::from_slice(&std::fs::read(app.join("Contents/Resources/release-inventory.json"))?).map_err(|_| invalid())?;
    let id = inventory["release_id"].as_str().ok_or_else(invalid)?;
    let valid = (1..=128).contains(&id.len()) && id.as_bytes()[0].is_ascii_alphanumeric() && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid { Ok(id.to_string()) } else { Err(invalid()) }
}

fn tool(context: &Context, program: &str, arguments: &[&str], timeout: Duration) -> Result<()> {
    let output = context.system.run(program, arguments, Deadline::after(timeout))?;
    if output.success() { Ok(()) } else { Err(err(format!("{program} failed: {}", output.combined()))) }
}

pub fn copy_bundle(context: &Context, source: &Path, target: &Path) -> Result<()> {
    tool(context, "/usr/bin/ditto", &[&source.display().to_string(), &target.display().to_string()], COPY_TIMEOUT)
}

pub fn verify_signature(context: &Context, app: &Path) -> Result<()> {
    tool(context, "/usr/bin/codesign", &["--verify", "--deep", "--strict", &app.display().to_string()], Duration::from_secs(120))
}

/// A bundle path is canonical when no ancestor is a symbolic link.
pub fn canonical(app: &Path) -> bool {
    app.canonicalize().is_ok_and(|canonical| canonical == app)
}

/// Exchanges the staged bundle with the installed one, or installs it when no bundle exists.
fn exchange_bundle(stage: &Path, target: &Path) -> Result<()> {
    if !stage.is_dir() {
        return Err(err("staged application bundle is unavailable"));
    }
    if target.is_dir() {
        return rustix::fs::renameat_with(CWD, stage, CWD, target, RenameFlags::EXCHANGE).map_err(|error| err(format!("application bundle could not be atomically replaced: {error}")));
    }
    std::fs::rename(stage, target).map_err(|error| err(format!("application bundle could not be installed: {error}")))
}

/// Copies the source release beside the installed Host, verifies the copy, and publishes it in one rename.
pub fn replace_bundle(context: &Context, paths: &Paths, source: &Path, release_id: &str, request_id: &str) -> Result<()> {
    let parent = paths.app.parent().ok_or_else(|| err("shared Host location is invalid"))?;
    let stage: PathBuf = parent.join(format!(".Agent VMM Host.maintenance-{request_id}.app"));
    service::remove_tree(&stage)?;
    std::fs::create_dir_all(parent)?;
    copy_bundle(context, source, &stage)?;
    if !host_release_id(&stage).is_ok_and(|id| id == release_id) {
        return Err(err("prepared replacement does not match selected release"));
    }
    verify_signature(context, &stage)?;
    exchange_bundle(&stage, &paths.app)?;
    service::remove_tree(&stage)
}

/// Stops the Host. The carrier and its VM keep running when the release in `source` starts the same carrier.
fn stop_for_release(context: &Context, paths: &Paths, source: Option<&Path>) -> Result<()> {
    let deadline = Deadline::after(HOST_STOP_TIMEOUT);
    let keep = source.is_some_and(|source| carrier::survives(context, paths, source, deadline));
    service::stop(context, paths, if keep { Scope::Host } else { Scope::Runtime }, deadline)
}

fn repair(context: &Context, paths: &Paths) -> Result<()> {
    service::reconcile_service(context, paths, false, Deadline::after(HOST_STOP_TIMEOUT))
}

pub struct Production<'a> {
    pub context: &'a Context,
}

impl Actions for Production<'_> {
    fn validate_source(&self, request: &MaintenanceRequest) -> Result<()> {
        if canonical(Path::new(&request.source_app)) { Ok(()) } else { Err(err("maintenance source must be a canonical bundle path without symlinks")) }
    }

    fn target_schema(&self, request: &MaintenanceRequest) -> Result<(u32, u32)> {
        let helper = Path::new(&request.source_app).join("Contents/Helpers/agent-vmm-lifecycle").display().to_string();
        let unreadable = || err("target Host version is unreadable");
        let output = self.context.system.run(&helper, &["version", "--json"], Deadline::after(Duration::from_secs(10))).map_err(|error| error.context("target Host version is unreadable"))?;
        let version: serde_json::Value = serde_json::from_str(&output.stdout).map_err(|_| unreadable())?;
        let number = |key: &str| version[key].as_u64().and_then(|value| u32::try_from(value).ok()).ok_or_else(unreadable);
        Ok((number("minimumSchema")?, number("currentSchema")?))
    }

    fn data_schema(&self, paths: &Paths) -> Result<DataSchema> {
        schema::read(&paths.application_data.join("host.sqlite"))
    }

    fn stop(&mut self, paths: &Paths, request: &MaintenanceRequest) -> Result<()> {
        // A reset and an uninstall end the VM. Any other maintenance replaces the control plane only.
        let source = (!request.reset() && !request.uninstall()).then(|| Path::new(&request.source_app));
        stop_for_release(self.context, paths, source)
    }

    fn remove_software(&mut self, paths: &Paths) -> Result<()> {
        service::remove_software(paths)
    }

    fn reset_data(&mut self, paths: &Paths) -> Result<()> {
        service::reset_data(self.context, paths)
    }

    fn replace(&mut self, paths: &Paths, request: &MaintenanceRequest) -> Result<()> {
        replace_bundle(self.context, paths, Path::new(&request.source_app), &request.target_release_id, &request.request_id)
    }

    fn restore(&mut self, paths: &Paths, request_id: &str) -> Result<()> {
        cutover::restore_host(self.context, paths, request_id, &mut || repair(self.context, paths))
    }

    fn verify(&mut self, paths: &Paths, request: &MaintenanceRequest) -> Result<()> {
        let absent = |path: &Path| matches!(path.symlink_metadata(), Err(error) if error.kind() == std::io::ErrorKind::NotFound);
        if request.uninstall() {
            let definitions = [Job::Host, Job::Carrier].into_iter().flat_map(|job| [job.plist(paths), job.alternate_plist(paths)]);
            if !absent(&paths.app) || !definitions.into_iter().all(|definition| absent(&definition)) {
                return Err(err("Host software removal is not confirmed"));
            }
            if request.reset() && !absent(&paths.logs) {
                return Err(err("Host data deletion is not confirmed"));
            }
            return Ok(());
        }
        let facts = status::read(self.context, paths, None, Deadline::after(Duration::from_secs(4)));
        if facts.host_running && facts.host_healthy { Ok(()) } else { Err(err("installed Host readiness is not confirmed")) }
    }
}

/// The effects of the `update` command for one prepared release.
pub struct Update<'a> {
    pub context: &'a Context,
    pub paths: &'a Paths,
    pub release_id: &'a str,
    pub request_id: &'a str,
}

impl UpdateActions for Update<'_> {
    fn installed_release(&self) -> Result<String> {
        host_release_id(&self.paths.app)
    }

    fn drain(&mut self, source: &Path) -> Result<()> {
        stop_for_release(self.context, self.paths, Some(source))
    }

    fn replace(&mut self, source: &Path) -> Result<()> {
        replace_bundle(self.context, self.paths, source, self.release_id, self.request_id)
    }

    fn repair(&mut self) -> Result<()> {
        repair(self.context, self.paths)
    }

    fn remove(&mut self, source: &Path) -> Result<()> {
        service::remove_tree(source)
    }
}
