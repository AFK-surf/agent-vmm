//! The fixed actions: `configure --service-user USER`, and
//! `install|start|stop|remove|status [carrier]` for the Host job or the carrier job.

use crate::config::Owner;
use crate::plist;
use crate::system::{Launch, System};
use std::path::{Path, PathBuf};

pub const OWNER_FILE: &str = "/Library/Application Support/AgentVMM/service-owner.json";
use crate::HOST_BUNDLE_IDENTIFIER as HOST_LABEL;
const CARRIER_LABEL: &str = "com.afk-surf.agent-vmm-carrier";
const APP: &str = "Library/Application Support/Agent VMM Host/current/Agent VMM Host.app";
const STATE_DISKS: [&str; 2] = ["Library/Application Support/AgentVMM/instance/pool.raw", "Library/Application Support/AgentVMM/instance/state.raw"];
const USAGE: &str = "usage: agent-vmm-service-executor configure --service-user USER | install|start|stop|remove|status [carrier]";
const STOP_POLLS: u32 = 600;
/// The virtualization service of the system closes the disks soon after the carrier exits.
const RELEASE_POLLS: u32 = 100;

type Outcome = Result<(), String>;

/// One of the two fixed system jobs.
struct Job {
    carrier: bool,
    label: &'static str,
    program: PathBuf,
    definition: String,
}

impl Job {
    fn new(owner: &Owner, carrier: bool) -> Self {
        let app = owner.home.join(APP);
        let label = if carrier { CARRIER_LABEL } else { HOST_LABEL };
        let params = plist::Params { label, home: &owner.home, app: &app, user: &owner.user, group: &owner.group, daemon: true, disable_pairing: true };
        if carrier {
            return Self { carrier, label, program: app.join("Contents/Helpers/agent-vmm-carrier"), definition: plist::carrier(&params) };
        }
        Self { carrier, label, program: app.join("Contents/MacOS/agent-vmm-host"), definition: plist::host(&params) }
    }

    fn target(&self) -> String {
        format!("system/{}", self.label)
    }

    fn plist(&self) -> PathBuf {
        PathBuf::from(format!("/Library/LaunchDaemons/{}.plist", self.label))
    }
}

/// Root must own the path and each parent, and nobody else may replace them.
fn protected(system: &dyn System, path: &Path, executable: bool) -> Outcome {
    let mut current = Some(path);
    while let Some(component) = current.filter(|component| *component != Path::new("/")) {
        let stat = system.lstat(component).map_err(|error| format!("{}: {error}", component.display()))?;
        if stat.uid != 0 || stat.mode & 0o022 != 0 || stat.symlink {
            return Err(format!("root-owned path {} is replaceable", component.display()));
        }
        if executable && component == path && stat.mode & 0o111 == 0 {
            return Err("executor is not executable".into());
        }
        current = component.parent();
    }
    Ok(())
}

/// The service user or root owns each component from `from` down to `path`, and nobody else may replace one.
fn owned(system: &dyn System, owner: &Owner, from: &Path, path: &Path) -> Outcome {
    let mut current = Some(path);
    while let Some(component) = current {
        let stat = system.lstat(component).map_err(|_| format!("service path {} is unavailable", component.display()))?;
        if stat.uid != 0 && stat.uid != owner.uid || stat.mode & 0o022 != 0 || stat.symlink {
            return Err(format!("service path {} is controlled by another identity", component.display()));
        }
        if component == from {
            return Ok(());
        }
        current = component.parent();
    }
    Err("service path is outside the configured home".into())
}

/// The fixed program is an executable of the service user below the configured home.
fn validate_program(system: &dyn System, owner: &Owner, program: &Path) -> Outcome {
    owned(system, owner, &owner.home, program)?;
    let stat = system.lstat(program).map_err(|error| error.to_string())?;
    if !stat.regular || stat.mode & 0o111 == 0 || stat.uid != owner.uid {
        return Err("fixed service program is not an executable owned by the configured service user".into());
    }
    Ok(())
}

fn configure(system: &dyn System, arguments: &[String]) -> Outcome {
    let [flag, name] = arguments else { return Err("configure requires --service-user USER".into()) };
    if flag != "--service-user" || name.is_empty() {
        return Err("configure requires --service-user USER".into());
    }
    let owner = system.account(name).ok_or("service user does not exist")?;
    if owner.uid == 0 || owner.gid == 0 || !owner.home.is_absolute() || owner.group.is_empty() {
        return Err("service user must have non-root numeric identity, a primary group and an absolute home".into());
    }
    owned(system, &owner, Path::new("/"), &owner.home)?;
    let directory = Path::new(OWNER_FILE).parent().expect("the owner file has a parent");
    protected(system, directory.parent().expect("the owner directory has a parent"), false).map_err(|_| "service owner config parent is not protected")?;
    system.create_dir(directory, 0o755).map_err(|error| error.to_string())?;
    protected(system, directory, false).map_err(|_| "service owner config directory is not protected")?;
    system.replace_file(Path::new(OWNER_FILE), owner.encode().as_bytes(), 0o600).map_err(|error| error.to_string())?;
    protected(system, Path::new(OWNER_FILE), false)
}

/// Reads the owner file and requires that it still describes the OS account.
fn read_owner(system: &dyn System) -> Result<Owner, String> {
    protected(system, Path::new(OWNER_FILE), false)?;
    let text = system.read(Path::new(OWNER_FILE)).map_err(|error| error.to_string())?;
    let owner = Owner::decode(&text).filter(|owner| owner.uid != 0 && owner.gid != 0 && owner.home.is_absolute()).ok_or("service owner config is invalid")?;
    if system.account(&owner.user).as_ref() != Some(&owner) {
        return Err("service owner config no longer matches the OS account".into());
    }
    Ok(owner)
}

fn loaded(system: &dyn System, job: &Job) -> Result<Option<String>, String> {
    match system.launchctl(&["print", &job.target()]) {
        Launch::Done(output) => Ok(Some(output)),
        Launch::NotFound => Ok(None),
        Launch::Failed(_) => Err("fixed job state could not be read".into()),
    }
}

fn launch(system: &dyn System, arguments: &[&str]) -> Outcome {
    match system.launchctl(arguments) {
        Launch::Done(_) => Ok(()),
        Launch::NotFound => Err(format!("launchctl {}: the job is not loaded", arguments[0])),
        Launch::Failed(message) => Err(format!("launchctl {}: {message}", arguments[0])),
    }
}

/// The process identifier in `launchctl print` output, or 0 when the job has no process.
fn pid(output: &str) -> i32 {
    let value = output.lines().filter_map(|line| line.trim().split_once('=')).find(|(key, _)| key.trim() == "pid");
    value.and_then(|(_, pid)| pid.trim().parse().ok()).unwrap_or(0)
}

fn install(system: &dyn System, owner: &Owner, job: &Job) -> Outcome {
    validate_program(system, owner, &job.program)?;
    protected(system, Path::new("/Library/LaunchDaemons"), false).map_err(|_| "launchd definition directory is not protected")?;
    system.prepare_logs(owner.gid).map_err(|error| error.to_string())?;
    if loaded(system, job)?.is_some() {
        return Err("fixed job must be stopped before its definition is replaced".into());
    }
    system.replace_file(&job.plist(), job.definition.as_bytes(), 0o644).map_err(|error| error.to_string())?;
    // launchd keeps a disabled override and refuses to bootstrap a disabled service.
    launch(system, &["enable", &job.target()])?;
    launch(system, &["bootstrap", "system", &job.plist().to_string_lossy()])
}

fn start(system: &dyn System, owner: &Owner, job: &Job) -> Outcome {
    validate_program(system, owner, &job.program)?;
    let output = loaded(system, job)?.ok_or("fixed job is not loaded; install it before start")?;
    let running = pid(&output);
    if running > 0 && system.process_running(running) {
        return Ok(());
    }
    launch(system, &["kickstart", &job.target()])
}

/// The processes that hold a VM state disk open. The virtualization service of the
/// system holds the disks of a running VM, so the holder is not the carrier itself.
fn disk_holders(system: &dyn System, owner: &Owner) -> Result<Vec<i32>, String> {
    let mut holders = Vec::new();
    for disk in STATE_DISKS {
        holders.extend(system.open_owners(&owner.home.join(disk)).map_err(|_| "state disk attachment could not be checked")?);
    }
    Ok(holders)
}

/// Stops the job. The Host job holds no VM disk: it stops while the carrier keeps the VM.
/// The carrier stops only after the service user stopped the VM, and the stop ends when the disks are closed.
fn stop(system: &dyn System, owner: &Owner, job: &Job) -> Outcome {
    let Some(output) = loaded(system, job)? else { return Ok(()) };
    let running = pid(&output);
    if job.carrier && !disk_holders(system, owner)?.is_empty() {
        return Err("VM state disk is still attached; complete the service-user VM stop first".into());
    }
    launch(system, &["bootout", &job.target()])?;
    let mut polls = 0;
    while running > 0 && system.process_running(running) {
        polls += 1;
        if polls > STOP_POLLS {
            return Err("fixed service process did not exit".into());
        }
        system.pause();
    }
    if !job.carrier {
        return Ok(());
    }
    for _ in 0..RELEASE_POLLS {
        if disk_holders(system, owner)?.is_empty() {
            return Ok(());
        }
        system.pause();
    }
    Err(format!("VM state disk is still open by process IDs {:?}", disk_holders(system, owner)?))
}

pub fn run(system: &dyn System, arguments: &[String]) -> Outcome {
    if system.euid() != 0 {
        return Err("administrator access is required".into());
    }
    let (action, rest) = arguments.split_first().ok_or(USAGE)?;
    protected(system, &system.executable().map_err(|error| error.to_string())?, true)?;
    if action == "configure" {
        return configure(system, rest);
    }
    let carrier = match rest {
        [] => false,
        [job] if job == "carrier" => true,
        _ => return Err(USAGE.into()),
    };
    if !matches!(action.as_str(), "install" | "start" | "stop" | "remove" | "status") {
        return Err(USAGE.into());
    }
    let owner = read_owner(system)?;
    let job = Job::new(&owner, carrier);
    match action.as_str() {
        "install" => install(system, &owner, &job),
        "start" => start(system, &owner, &job),
        "stop" => stop(system, &owner, &job),
        "remove" => {
            stop(system, &owner, &job)?;
            system.remove_file(&job.plist()).map_err(|error| error.to_string())
        }
        _ => launch(system, &["print", &job.target()]),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
