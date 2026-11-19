//! The commands that only macOS has: `host-maintain`, `maintenance-status` and `publish-host`.

use super::maintenance::{self, MaintenanceRequest};
use super::paths::{application_data, lookup_account, paths_for, resolve_service_config, shared_host_app};
use super::{actions, maintenance_journal, publish};
use crate::cli::{Reply, read_input};
use crate::error::{Kind, Result, err};
use crate::{Context, Paths, ServiceMode, ServiceRequest};
use serde_json::json;
use std::io::Read;
use std::path::Path;

const REQUEST_LIMIT: usize = 16384;

fn status_request(input: &[u8]) -> Result<String> {
    let invalid = || err("invalid maintenance status request");
    if input.iter().all(u8::is_ascii_whitespace) {
        return Ok(String::new());
    }
    let request: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(input).map_err(|_| invalid())?;
    if request.keys().any(|key| key != "requestId") {
        return Err(invalid());
    }
    match request.get("requestId") {
        None => Ok(String::new()),
        Some(id) => id.as_str().map(str::to_string).ok_or_else(invalid),
    }
}

fn maintain(context: &Context, command: &str, mut request: ServiceRequest, arguments: &[String], input: &mut dyn Read) -> Result<Reply> {
    if !arguments.is_empty() || request.app.is_some() {
        return Err(err("maintenance commands accept a JSON request on stdin and service identity options only"));
    }
    let account = lookup_account(request.user.as_deref())?;
    let euid = context.system.euid();
    if account.uid == 0 || euid != 0 && account.uid != euid {
        return Err(err("maintenance requires the local service user"));
    }
    let input = read_input(input, REQUEST_LIMIT).map_err(|_| err("invalid maintenance request"))?;
    // The journal is read before the service is resolved: a reset can have removed service.json.
    let journal_paths = Paths { home: account.home.clone(), application_data: application_data(&account.home), ..Paths::default() };
    if command == "maintenance-status" {
        let receipt = maintenance_journal::status(context, &journal_paths, &status_request(&input)?)?;
        return Ok(Reply::ok(maintenance::report(receipt.as_ref())));
    }
    let maintenance_request = MaintenanceRequest::decode(&input)?;
    let saved = maintenance_journal::service_receipt(context, &journal_paths, &maintenance_request)?;
    if let Some(saved) = &saved
        && matches!(request.mode, None | Some(ServiceMode::Auto))
    {
        request.mode = match saved.service_mode.as_str() {
            "agent" => Some(ServiceMode::Agent),
            "daemon" => Some(ServiceMode::Daemon),
            _ => request.mode,
        };
    }
    let (mut config, account) = resolve_service_config(context, &request)?;
    if let Some(saved) = &saved {
        config.disable_pairing = saved.disable_pairing;
    }
    if config.uid != euid {
        return Err(err("maintenance must run as the configured non-root service user"));
    }
    // The maintenance runs in this process. The caller stops it by the path of this executable.
    let helper = std::env::current_exe()?.canonicalize()?;
    let paths = paths_for(&account.home, &helper, config, Some(&shared_host_app(&account.home)), false)?;
    let (receipt, result) = maintenance::run(context, &paths, &maintenance_request, &mut actions::Production { context });
    let failure = result.err();
    let output = match (&receipt, &failure) {
        (Some(receipt), _) => maintenance::report(Some(receipt)),
        (None, Some(error)) if error.kind == Kind::OwnerBusy => json!({"error": {"code": "owner_busy", "message": error.message}}),
        (None, _) => json!({"error": {"code": "maintenance_rejected", "reason": "request_rejected"}}),
    };
    Ok(Reply { output: Some(output), failure: failure.map(|error| format!("Host maintenance: {error}")) })
}

fn publish_host(context: &Context, request: ServiceRequest, arguments: &[String]) -> Result<Reply> {
    let [flag, source] = arguments else { return Err(err("publish-host requires --source-app and uses only the standard shared target")) };
    if flag != "--source-app" || request.app.is_some() {
        return Err(err("publish-host requires --source-app and uses only the standard shared target"));
    }
    let (config, account) = resolve_service_config(context, &request)?;
    if config.uid != context.system.euid() {
        return Err(err("publication must run as the configured non-root service user"));
    }
    let paths = Paths { home: account.home.clone(), app: shared_host_app(&account.home), application_data: application_data(&account.home), service: config, ..Paths::default() };
    let (result, outcome) = publish::publish(context, &paths, Path::new(source), &publish::Production(context));
    Ok(match outcome {
        Ok(()) => Reply::ok(serde_json::to_value(&result)?),
        // Only the owner's uninstall is a result that the caller acts on.
        Err(error) => Reply { output: result.error.is_some().then(|| serde_json::to_value(&result)).transpose()?, failure: Some(error.message) },
    })
}

/// Runs a macOS command. `None` means that `command` is not one of them.
pub fn execute_platform_command(context: &Context, command: &str, request: &ServiceRequest, arguments: &[String], input: &mut dyn Read) -> Option<Result<Reply>> {
    match command {
        "host-maintain" | "maintenance" | "maintenance-status" => Some(maintain(context, command, request.clone(), arguments, input)),
        "publish-host" => Some(publish_host(context, request.clone(), arguments)),
        _ => None,
    }
}
