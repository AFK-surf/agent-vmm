//! The command line of the lifecycle helper. Standard output carries one JSON
//! value; every diagnostic goes to standard error.

use crate::args::{self, InstallArgs};
use crate::error::{Deadline, Error, Kind, Result, err};
use crate::journal::LifecycleOperation;
use crate::operation::{self, Request};
use crate::registration::{self, Authorization, Registrar};
use crate::registration_plan::{CLAIM_DESCRIPTOR_LIMIT, ClaimDescriptor, RECOVERY_DESCRIPTOR_LIMIT, RecoveryDescriptor};
use crate::{Context, HOST_STOP_TIMEOUT, INIT_TIMEOUT, Paths, ServiceRequest, local, new_operation_id, operator, platform, preflight, report, schema, status, system};
use agent_vmm_proto::host::v1::RemoteRegistrationState;
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::time::Duration;

const USAGE: &str = "usage: agent-vmm-lifecycle version --json|preflight --json|status|operation|host-maintain|maintenance-status|publish-host|install|repair|registration-claim|registration-recover|registration-enable|registration-drain|registration-remove|update|uninstall [--service-type agent|daemon|auto] [--service-user USER] [...]";
const STATUS_BUDGET: Duration = Duration::from_secs(4);

/// What a command prints, and the failure that makes the process exit with 1.
#[derive(Debug, Default)]
pub struct Reply {
    pub output: Option<Value>,
    pub failure: Option<String>,
}

impl Reply {
    pub fn ok(output: Value) -> Self {
        Self { output: Some(output), failure: None }
    }

    /// A receipt is printed before the failure that it records ends the command.
    fn receipt((operation, result): (LifecycleOperation, Result<()>)) -> Result<Self> {
        Ok(Self { output: Some(serde_json::to_value(operation)?), failure: result.err().map(|error| error.message) })
    }
}

/// Reads standard input up to `limit` bytes. A longer input is an error.
pub fn read_input(input: &mut dyn Read, limit: usize) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    input.take(limit as u64 + 1).read_to_end(&mut data)?;
    if data.len() > limit {
        return Err(err("standard input exceeds its bound"));
    }
    Ok(data)
}

fn known(command: &str) -> bool {
    matches!(command, "version" | "preflight" | "status" | "operation" | "maintenance-status") || mutation(command) || local::is_local_command(command)
}

fn mutation(command: &str) -> bool {
    matches!(
        command,
        "publish-host" | "host-maintain" | "maintenance" | "install" | "install-operation-ack" | "host-repair" | "repair" | "registration-claim" | "registration-recover"
            | "registration-enable" | "registration-drain" | "registration-remove" | "registration-state" | "registration-revoke" | "drain" | "host-update" | "update" | "uninstall"
    )
}

/// Runs the helper with `arguments` after the program name and returns the exit code.
pub fn run(context: &Context, arguments: &[String], input: &mut dyn Read, output: &mut dyn Write, diagnostics: &mut dyn Write) -> i32 {
    let reply = match arguments.split_first() {
        None => Err(err(USAGE)),
        // Comma reads both signals: the text on standard error and the code on standard output.
        Some((command, _)) if !known(command) => Ok(Reply {
            output: Some(json!({"error": {"code": "upgrade_required", "reason": "unknown_command"}})),
            failure: Some(format!("upgrade_required: unknown command {command}")),
        }),
        Some((command, rest)) => dispatch(context, command, rest, input),
    };
    let reply = reply.unwrap_or_else(|error| Reply { output: error_output(&error), failure: Some(error.message) });
    if let Some(value) = &reply.output {
        let _ = writeln!(output, "{value}");
    }
    match reply.failure {
        Some(failure) => {
            let _ = writeln!(diagnostics, "lifecycle failed: {failure}");
            1
        }
        None => 0,
    }
}

/// The structured form of the failures that a caller acts on.
fn error_output(error: &Error) -> Option<Value> {
    match &error.kind {
        Kind::RequestBlocked(request_id) => Some(json!({"error": {"code": "request_blocked", "reason": "local_owner_fence", "requestId": request_id}})),
        Kind::HostUninstalled => Some(json!({"error": {"code": "host_uninstalled", "reason": "owner_uninstalled"}})),
        _ => None,
    }
}

fn dispatch(context: &Context, command: &str, rest: &[String], input: &mut dyn Read) -> Result<Reply> {
    match command {
        "version" if rest == ["--json"] => return Ok(Reply::ok(schema::version_report())),
        "version" => return Err(err("version requires --json")),
        "preflight" if rest == ["--json"] => {
            let directory = platform::preflight_data_directory()?;
            let checked = preflight::check(&directory, preflight::required_bytes(&directory)).map_err(|error| error.context("preflight cannot read free disk space"))?;
            return Ok(Reply::ok(serde_json::to_value(checked)?));
        }
        "preflight" => return Err(err("preflight requires --json")),
        _ => {}
    }
    if mutation(command) {
        platform::check_mutation_user(context.system.euid())?;
    }
    let (request, arguments) = ServiceRequest::extract(command, rest)?;
    if let Some(reply) = platform::execute_platform_command(context, command, &request, &arguments, input) {
        return reply;
    }
    if local::is_local_command(command) {
        return local_operator(context, command, &request, &arguments, input);
    }
    match command {
        "operation" => {
            let paths = platform::observation_paths(context, &request)?;
            return Ok(Reply::ok(report::operation(context, &paths, &arguments)?));
        }
        "status" => {
            let paths = platform::observation_paths(context, &request)?;
            let registration = args::parse_status(&arguments)?;
            let observed = status::read(context, &paths, registration.as_deref(), Deadline::after(STATUS_BUDGET));
            return Ok(Reply::ok(serde_json::to_value(observed)?));
        }
        _ => {}
    }
    let mut paths = platform::lifecycle_paths(context, &request)?;
    match command {
        "install-operation-ack" => Err(err("upgrade_required: installation handoff was replaced by registration claim")),
        "registration-claim" | "registration-recover" => register(context, &mut paths, command, args::parse_install("registration-claim", &arguments)?, input),
        "install" | "host-repair" | "repair" => {
            let kind = if command == "install" { "install" } else { "repair" };
            let parsed = args::parse_install(kind, &arguments)?;
            if parsed.descriptor_stdin {
                // The Comma installer gives the registration with the installation.
                return register(context, &mut paths, "registration-claim", parsed, input);
            }
            install(context, &paths, kind, parsed)
        }
        "drain" => {
            let request = Request::new("drain", &args::parse_request_id(&arguments)?);
            Reply::receipt(operation::run(context, &paths, request, &mut || platform::drain_service(context, &paths, Deadline::after(HOST_STOP_TIMEOUT))))
        }
        "host-update" | "update" => update(context, &paths, &arguments, input),
        "registration-remove" | "registration-revoke" => {
            let parsed = args::parse_registration_state(&arguments, Some(RemoteRegistrationState::Draining))?;
            let request_id = or_new(parsed.request_id);
            let request = Request::retryable("registration-remove", &request_id, &parsed.registration_id);
            Reply::receipt(operation::run(context, &paths, request, &mut || {
                operator::revoke_registration(context, &paths, &parsed.registration_id, &request_id, Deadline::after(Duration::from_secs(120)))
            }))
        }
        "registration-enable" | "registration-drain" | "registration-state" => {
            let fixed = match command {
                "registration-enable" => Some(RemoteRegistrationState::Enabled),
                "registration-drain" => Some(RemoteRegistrationState::Draining),
                _ => None,
            };
            let parsed = args::parse_registration_state(&arguments, fixed)?;
            let request_id = or_new(parsed.request_id);
            let kind = if fixed.is_some() { command.to_string() } else { format!("registration-{}", parsed.state.as_str_name()) };
            let request = Request::retryable(&kind, &request_id, &parsed.registration_id);
            Reply::receipt(operation::run(context, &paths, request, &mut || {
                operator::set_registration_state(context, &paths, &parsed.registration_id, parsed.state, &request_id, Deadline::after(Duration::from_secs(60)))
            }))
        }
        "uninstall" => {
            let (purge, request_id) = args::parse_uninstall(&arguments)?;
            let request = Request::new("remove", &request_id);
            Reply::receipt(operation::run(context, &paths, request, &mut || platform::remove_service(context, &paths, purge, Deadline::after(HOST_STOP_TIMEOUT))))
        }
        _ => Err(err("unknown lifecycle command")),
    }
}

fn or_new(request_id: String) -> String {
    if request_id.is_empty() { new_operation_id("req") } else { request_id }
}

/// The requests are exact protobuf messages. These commands write no lifecycle journal and contact no cloud service.
fn local_operator(context: &Context, command: &str, request: &ServiceRequest, arguments: &[String], input: &mut dyn Read) -> Result<Reply> {
    let result = (|| {
        if !arguments.is_empty() {
            return Err(err("local operator requests must use bounded JSON stdin"));
        }
        let paths = platform::observation_paths(context, request)?;
        let mut data = Vec::new();
        input.take(8193).read_to_end(&mut data)?;
        local::run(context, &paths, command, &data)
    })();
    Ok(match result {
        Ok(line) => Reply::ok(serde_json::from_str(&line)?),
        Err(error) => Reply { output: Some(local::error_envelope(&error)), failure: Some(error.message) },
    })
}

fn register(context: &Context, paths: &mut Paths, command: &str, parsed: InstallArgs, input: &mut dyn Read) -> Result<Reply> {
    if parsed.request_id.is_empty() {
        return Err(err("registration-claim requires --request-id"));
    }
    let authorization = match (command, parsed.descriptor_stdin) {
        ("registration-recover", false) => return Err(err("registration-recover requires fresh authorization on stdin")),
        ("registration-recover", true) => Authorization::Recovery(RecoveryDescriptor::decode(&read_input(input, RECOVERY_DESCRIPTOR_LIMIT)?)?),
        (_, true) => Authorization::Claim(ClaimDescriptor::decode(&read_input(input, CLAIM_DESCRIPTOR_LIMIT)?)?),
        (_, false) => Authorization::saved(context, paths, &parsed.request_id)?,
    };
    let registration_id = authorization.registration_id().to_string();
    // A recovery of a historical request continues that request and names this one as its correlation.
    let request_id = registration::original_request_id(context, paths, &registration_id, &parsed.request_id, command)?;
    if request_id != parsed.request_id {
        paths.correlation_request_id = Some(parsed.request_id.clone());
    }
    let paths = &*paths;
    let mut registrar = Registrar::new(context, paths, authorization);
    let request = Request::retryable(command, &request_id, &registration_id);
    Reply::receipt(operation::run(context, paths, request, &mut || registrar.run(command, &request_id, parsed.disable_pairing)))
}

fn install(context: &Context, paths: &Paths, kind: &str, parsed: InstallArgs) -> Result<Reply> {
    let request = Request::retryable(kind, &or_new(parsed.request_id), &parsed.registration_id);
    let stage = |name: &str| if kind == "repair" { operation::persist_stage(context, paths, &request, name) } else { Ok(()) };
    Reply::receipt(operation::run(context, paths, request.clone(), &mut || {
        stage("service")?;
        platform::reconcile_service(context, paths, parsed.disable_pairing, Deadline::after(HOST_STOP_TIMEOUT))?;
        stage("vm")?;
        operator::ensure_vm_started(context, paths, &request.request_id, Deadline::after(INIT_TIMEOUT))?;
        if !parsed.registration_id.is_empty() {
            stage("registration")?;
            operator::wait_registration_connected(context, paths, &parsed.registration_id, Deadline::after(Duration::from_secs(60)))?;
        }
        stage("complete")
    }))
}

fn update(context: &Context, paths: &Paths, arguments: &[String], input: &mut dyn Read) -> Result<Reply> {
    let parsed = args::parse_update(arguments)?;
    let preparation = parsed.preparation_failure_stdin.then(|| args::decode_preparation_failure(&read_input(input, 4096)?)).transpose()?;
    let request = Request {
        release_id: parsed.target_release_id.clone(),
        artifact_sha256: parsed.artifact_sha256.clone(),
        artifact_size: parsed.artifact_size,
        ..Request::retryable("update", &parsed.request_id, "")
    };
    Reply::receipt(operation::run(context, paths, request.clone(), &mut || match (&preparation, &parsed.source_app) {
        (Some(failure), _) => {
            operation::persist_stage(context, paths, &request, &failure.stage)?;
            Err(Error::new(Kind::Preparation(failure.clone()), failure.message.clone()))
        }
        (None, Some(source)) => platform::update_application(context, paths, source, &parsed.target_release_id, &parsed.request_id),
        (None, None) => Err(err("update requires a prepared release")),
    }))
}

/// The entry of `agent-vmm-lifecycle` and of `agent-vmm lifecycle`.
pub fn main(arguments: Vec<String>) -> ! {
    let context = system::production();
    let code = run(&context, &arguments, &mut std::io::stdin().lock(), &mut std::io::stdout().lock(), &mut std::io::stderr().lock());
    std::process::exit(code)
}

#[cfg(test)]
#[path = "cli_tests.rs"]
mod tests;
