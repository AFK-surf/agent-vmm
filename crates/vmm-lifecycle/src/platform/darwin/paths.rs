//! The macOS layout: one Host per service user, below that user's home.

use super::launchd;
use crate::account::{self, Account};
use crate::error::{Deadline, Result, err};
use crate::{Context, HOST_APP_NAME, Paths, ServiceConfig, ServiceMode, ServiceRequest, release_id};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The launchd label of the Host job.
#[cfg(not(feature = "e2e-isolation"))]
pub fn host_label() -> String {
    crate::HOST_BUNDLE_IDENTIFIER.to_string()
}

/// A test build moves the label and the service home away from the production Host.
#[cfg(feature = "e2e-isolation")]
pub fn host_label() -> String {
    let label = std::env::var("AGENT_VMM_E2E_HOST_LABEL").unwrap_or_default();
    let valid = (1..=128).contains(&label.len()) && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-');
    assert!(valid && label != crate::HOST_BUNDLE_IDENTIFIER, "an e2e lifecycle build requires AGENT_VMM_E2E_HOST_LABEL other than the production label");
    label
}

/// The carrier owns the VM. Its job outlives a replacement of the Host control plane.
pub fn carrier_label() -> String {
    let host = host_label();
    format!("{}-carrier", host.strip_suffix("-host").unwrap_or(&host))
}

#[cfg(not(feature = "e2e-isolation"))]
fn service_account(account: Account) -> Account {
    account
}

#[cfg(feature = "e2e-isolation")]
fn service_account(mut account: Account) -> Account {
    let home = PathBuf::from(std::env::var_os("AGENT_VMM_E2E_HOME").unwrap_or_default());
    let canonical = home.canonicalize().is_ok_and(|canonical| canonical == home);
    assert!(home.is_absolute() && canonical && home != account.home, "an e2e lifecycle build requires a canonical AGENT_VMM_E2E_HOME other than the user's home");
    account.home = home;
    account
}

pub fn lookup_account(name: Option<&str>) -> Result<Account> {
    account::lookup(name).map(service_account)
}

pub fn application_data(home: &Path) -> PathBuf {
    home.join("Library/Application Support/AgentVMM")
}

/// The standard location of the Host that Comma and the installer share.
pub fn shared_host_app(home: &Path) -> PathBuf {
    home.join("Library/Application Support/Agent VMM Host/current").join(HOST_APP_NAME)
}

pub fn preflight_data_directory() -> Result<PathBuf> {
    Ok(application_data(&lookup_account(None).map_err(|_| err("preflight cannot resolve the home directory"))?.home))
}

/// An existing service is observed or removed through its recorded owner. Only
/// install and repair reinterpret `auto` and can switch the launchd domain.
pub fn select_mode(requested: ServiceMode, configured: Option<ServiceMode>, action: &str, euid: u32, gui_available: impl FnOnce() -> bool) -> Result<ServiceMode> {
    if requested != ServiceMode::Auto {
        return Ok(requested);
    }
    if let Some(configured) = configured
        && action != "install"
        && action != "repair"
    {
        return Ok(configured);
    }
    if euid == 0 {
        return Ok(ServiceMode::Daemon);
    }
    if let Some(configured) = configured {
        return Ok(configured);
    }
    if gui_available() {
        return Ok(ServiceMode::Agent);
    }
    Err(err("automatic service selection requires administrator access or an active GUI login for the service user"))
}

pub fn resolve_service_config(context: &Context, request: &ServiceRequest) -> Result<(ServiceConfig, Account)> {
    let account = lookup_account(request.user.as_deref())?;
    if account.uid == 0 || !account.home.is_absolute() || account.home == Path::new("/") {
        return Err(err("service user must be a non-root macOS user with an absolute home directory"));
    }
    if account.group.is_empty() {
        return Err(err("service user's primary group is unavailable"));
    }
    let euid = context.system.euid();
    if euid != 0 && account.uid != euid {
        return Err(err("a non-root installer can only manage its own Agent VMM service"));
    }
    let configured = match ServiceConfig::read(&application_data(&account.home).join("service.json")) {
        Ok(configured) => Some(configured),
        Err(error) if error.not_found() => None,
        Err(error) => return Err(error),
    };
    let requested = request.mode.or(configured.as_ref().map(|configured| configured.mode)).unwrap_or(ServiceMode::Agent);
    let gui = || launchd::run(context, &["print", &format!("gui/{}", account.uid)], Deadline::after(Duration::from_secs(10))).is_ok();
    let mode = select_mode(requested, configured.as_ref().map(|configured| configured.mode), &request.action, euid, gui)?;
    let config = ServiceConfig {
        version: 1,
        mode,
        user: account.name.clone(),
        group: account.group.clone(),
        uid: account.uid,
        gid: account.gid,
        release_id: release_id().to_string(),
        disable_pairing: configured.is_some_and(|configured| configured.disable_pairing),
    };
    Ok((config, account))
}

fn current_helper() -> Result<PathBuf> {
    std::env::current_exe().map_err(|_| err("helper location is unavailable"))?.canonicalize().map_err(|_| err("helper location is invalid"))
}

/// Derives the layout from the helper inside its bundle. `target` names an installed Host other than that bundle.
pub fn paths_for(home: &Path, helper: &Path, service: ServiceConfig, target: Option<&Path>, mutation: bool) -> Result<Paths> {
    if !home.is_absolute() || home == Path::new("/") {
        return Err(err("user home directory is invalid"));
    }
    let named = |path: Option<&Path>, name: &str| path.is_some_and(|path| path.file_name().is_some_and(|file| file == name));
    let helpers = helper.parent();
    let contents = helpers.and_then(Path::parent);
    if !named(helpers, "Helpers") || !named(contents, "Contents") || mutation && !named(Some(helper), "agent-vmm-lifecycle") {
        return Err(err("helper is not inside an Agent VMM application bundle"));
    }
    let source = contents.and_then(Path::parent).filter(|app| named(Some(app), HOST_APP_NAME)).ok_or_else(|| err(format!("application bundle must be \"{HOST_APP_NAME}\"")))?;
    let app = match target {
        Some(target) if target.is_absolute() && named(Some(target), HOST_APP_NAME) => target,
        Some(_) => return Err(err("service app must be an absolute Agent VMM Host.app path")),
        None => source,
    };
    let label = host_label();
    let agent_plist = home.join("Library/LaunchAgents").join(format!("{label}.plist"));
    let daemon_plist = PathBuf::from("/Library/LaunchDaemons").join(format!("{label}.plist"));
    let daemon = service.mode == ServiceMode::Daemon;
    let data = application_data(home);
    Ok(Paths {
        correlation_request_id: None,
        home: home.to_path_buf(),
        app: app.to_path_buf(),
        source: source.to_path_buf(),
        logs: home.join("Library/Logs/AgentVMM"),
        service_logs: if daemon { PathBuf::from("/Library/Logs/AgentVMM") } else { home.join("Library/Logs/AgentVMM") },
        service_config_file: data.join("service.json"),
        application_data: data,
        host_plist: if daemon { daemon_plist.clone() } else { agent_plist.clone() },
        agent_plist,
        daemon_plist,
        service,
        install_root: PathBuf::new(),
        unit: PathBuf::new(),
    })
}

/// Refuses a symbolic link on the way from the service home to `target`. A missing descendant is valid.
pub fn validate_maintenance_path(home: &Path, target: &Path) -> Result<()> {
    let relative = target.strip_prefix(home).map_err(|_| err("maintenance target is outside the service home"))?;
    let mut current = home.to_path_buf();
    for part in relative.iter() {
        current.push(part);
        match current.symlink_metadata() {
            Ok(metadata) if metadata.file_type().is_symlink() => return Err(err("maintenance target contains a symlink")),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn request_paths(context: &Context, request: &ServiceRequest, mutation: bool) -> Result<Paths> {
    let helper = current_helper()?;
    let (config, account) = resolve_service_config(context, request)?;
    if request.app.is_some() && context.system.euid() != 0 {
        return Err(err("--service-app is restricted to an administrator installer"));
    }
    let shared = request.shared_host.then(|| shared_host_app(&account.home));
    if let (Some(shared), true) = (&shared, mutation) {
        validate_maintenance_path(&account.home, shared)?;
    }
    paths_for(&account.home, &helper, config, shared.as_deref().or(request.app.as_deref()), mutation)
}

pub fn lifecycle_paths(context: &Context, request: &ServiceRequest) -> Result<Paths> {
    request_paths(context, request, true)
}

pub fn observation_paths(context: &Context, request: &ServiceRequest) -> Result<Paths> {
    request_paths(context, request, false)
}
