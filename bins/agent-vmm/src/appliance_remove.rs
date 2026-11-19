//! `agent-vmm appliance remove`: a reviewed plan, then the removal of exactly the confirmed resources.
//! The confirmed command is saved under its request ID, so a retry repeats the original command.

use crate::appliance::{Client, Outcome, bounded, block_on, message_text, owner_socket, request_id, unix_millis, watch_operation};
use crate::flags::{Flags, Kind, format_duration};
use crate::helper;
use agent_vmm_proto::appliance::v1 as appliancev1;
use appliancev1::ExecuteRemoveRequest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;

pub fn remove(arguments: &[String]) -> Result<Outcome<()>, String> {
    use Kind::{Boolean, Value};
    let flags = Flags::parse(arguments, &[("operator-socket", Value), ("installation", Value), ("request-id", Value), ("expected-revision", Value), ("execute", Boolean), ("timeout", Value), ("confirm-resource", Value)])?;
    let confirmed = flags.all("confirm-resource").to_vec();
    if confirmed.iter().any(String::is_empty) {
        return Err("invalid value \"\" for flag -confirm-resource: confirmed resource ID must not be empty".into());
    }
    let (installation, request_id, execute) = (flags.text("installation"), flags.text("request-id"), flags.enabled("execute"));
    let (revision, timeout) = (flags.number("expected-revision")?.unwrap_or(0), flags.duration("timeout", Duration::from_secs(600))?);
    let socket = flags.get("operator-socket").map_or_else(owner_socket, str::to_string);
    if installation.is_empty() || timeout.is_zero() {
        return Ok(Err("remove requires --installation and a positive timeout".into()));
    }
    if execute && (request_id.is_empty() || revision == 0 || confirmed.is_empty()) {
        return Ok(Err("execute requires the plan's --request-id, --expected-revision and explicit --confirm-resource IDs".into()));
    }
    if !execute && (revision != 0 || !confirmed.is_empty()) {
        return Ok(Err("resource confirmation and expected revision require --execute".into()));
    }
    let deadline = unix_millis(SystemTime::now() + timeout);
    if !execute {
        return Ok(block_on(&socket, timeout, |client, end| plan(client, end, installation, request_id, deadline)));
    }
    let request = ExecuteRemoveRequest { installation_id: installation, request_id, expected_revision: revision, deadline_unix_millis: deadline, confirmed_resource_ids: confirmed };
    let request = match freeze(&socket, request, timeout, &helper::platform_data_directory(&helper::home()).join("appliance-requests")) {
        Ok(request) => request,
        Err(fault) => return Ok(Err(fault)),
    };
    let command = retry_command(&socket, &request, timeout);
    Ok(block_on(&socket, timeout, |client, end| execute_frozen(client, end, request, command)))
}

async fn plan(mut client: Client, end: Instant, installation: String, request: String, deadline: i64) -> Outcome<()> {
    let status = bounded(end, client.get_appliance_status(appliancev1::GetApplianceStatusRequest { installation_id: installation.clone() })).await?;
    let request_id = if request.is_empty() { request_id() } else { request };
    let plan = appliancev1::PlanRemoveRequest { installation_id: installation, request_id: request_id.clone(), expected_revision: status.status.map_or(0, |status| status.revision), deadline_unix_millis: deadline };
    let plan = bounded(end, client.plan_remove(plan)).await?;
    println!("request_id={request_id} expected_revision={}\n{}", plan.plan.as_ref().map_or(0, |plan| plan.revision), message_text("PlanRemoveResponse", &plan));
    Ok(())
}

async fn execute_frozen(mut client: Client, end: Instant, request: ExecuteRemoveRequest, command: String) -> Outcome<()> {
    let installation = request.installation_id.clone();
    let response = bounded(end, client.execute_remove(request.clone())).await.map_err(|fault| {
        let reason = fault.status.as_ref().and_then(vmm_lifecycle::grpc::error_info).map(|info| info.reason).unwrap_or_default();
        if reason != "APPLIANCE_OPERATION_NOT_ADMITTED" {
            return fault;
        }
        fault.context(&format!("original removal intent was not accepted; review a new plan with appliance remove --installation {installation}, then approve it with a new --request-id"))
    })?;
    let operation = response.operation.unwrap_or_default();
    match watch_operation(&mut client, end, &installation, &operation).await {
        Err(fault) if fault.status.as_ref().is_some_and(|status| status.code() == tonic::Code::DeadlineExceeded) => Err(format!(
            "remove watch deadline reached. This deadline stops observation only.\ninstallation={installation} operation={} request_id={}\nContinue with the original removal request:\n{command}\n{}",
            operation.operation_id, request.request_id, fault.message
        )
        .into()),
        result => result,
    }
}

/// The command line that repeats a confirmed removal, with each argument quoted for a shell.
fn retry_command(socket: &str, request: &ExecuteRemoveRequest, timeout: Duration) -> String {
    let program = std::env::args().next().unwrap_or_else(|| "agent-vmm".into());
    let mut arguments = vec![program, "appliance".into(), "remove".into(), "--operator-socket".into(), socket.into(), "--installation".into(), request.installation_id.clone()];
    arguments.extend(["--request-id".into(), request.request_id.clone(), "--execute".into(), "--expected-revision".into(), request.expected_revision.to_string(), "--timeout".into(), format_duration(timeout)]);
    for resource in &request.confirmed_resource_ids {
        arguments.extend(["--confirm-resource".to_string(), resource.clone()]);
    }
    arguments.iter().map(|argument| format!("'{}'", argument.replace('\'', "'\"'\"'"))).collect::<Vec<_>>().join(" ")
}

fn is_zero<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// The saved request, with the keys and the omissions of the Go Host's request file.
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct SavedRequest {
    #[serde(skip_serializing_if = "is_zero")]
    installation_id: String,
    #[serde(skip_serializing_if = "is_zero")]
    request_id: String,
    #[serde(skip_serializing_if = "is_zero")]
    expected_revision: u64,
    #[serde(skip_serializing_if = "is_zero")]
    deadline_unix_millis: i64,
    #[serde(skip_serializing_if = "is_zero")]
    confirmed_resource_ids: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct SavedCommand {
    timeout_nanos: u64,
    request: Option<SavedRequest>,
}

/// Saves the first confirmed command of a request ID and returns it. A later
/// command with the same request ID must have the same parameters; it runs with the saved deadline.
fn freeze(socket: &str, mut request: ExecuteRemoveRequest, timeout: Duration, directory: &Path) -> Outcome<ExecuteRemoveRequest> {
    use std::os::unix::fs::DirBuilderExt;
    request.confirmed_resource_ids.sort();
    request.confirmed_resource_ids.dedup();
    let canonical = std::fs::canonicalize(socket)?;
    let mut key = Sha256::new();
    key.update(canonical.as_os_str().as_encoded_bytes());
    key.update([0]);
    key.update(request.request_id.as_bytes());
    let name: String = key.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    let path = directory.join(format!("remove-{name}.json"));
    if !path.exists() {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(directory)?;
        let saved = SavedRequest {
            installation_id: request.installation_id.clone(),
            request_id: request.request_id.clone(),
            expected_revision: request.expected_revision,
            deadline_unix_millis: request.deadline_unix_millis,
            confirmed_resource_ids: request.confirmed_resource_ids.clone(),
        };
        let mut temporary = tempfile::Builder::new().prefix(".remove-").tempfile_in(directory)?;
        let encoded = serde_json::to_string(&SavedCommand { timeout_nanos: timeout.as_nanos() as u64, request: Some(saved) }).map_err(|error| error.to_string())?;
        writeln!(temporary, "{encoded}")?;
        temporary.as_file().sync_all()?;
        // The first command wins: a link never replaces a saved command.
        match std::fs::hard_link(temporary.path(), &path) {
            Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => return Err(error.into()),
            _ => {}
        }
    }
    let saved: SavedCommand = serde_json::from_slice(&std::fs::read(&path)?).map_err(|error| format!("read original appliance removal command: {error}"))?;
    let original = saved.request.unwrap_or_default();
    request.deadline_unix_millis = original.deadline_unix_millis;
    let same = original.installation_id == request.installation_id && original.request_id == request.request_id && original.expected_revision == request.expected_revision && original.confirmed_resource_ids == request.confirmed_resource_ids;
    if saved.timeout_nanos != timeout.as_nanos() as u64 || !same {
        return Err("removal request ID belongs to a different confirmed command; repeat its original parameters".into());
    }
    std::fs::File::open(directory)?.sync_all()?;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(revision: u64, deadline: i64, confirmed: &[&str]) -> ExecuteRemoveRequest {
        ExecuteRemoveRequest { installation_id: "mcp-1".into(), request_id: "remove-1".into(), expected_revision: revision, deadline_unix_millis: deadline, confirmed_resource_ids: confirmed.iter().map(|id| id.to_string()).collect() }
    }

    #[test]
    fn a_retry_repeats_the_first_confirmed_command_with_its_deadline() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("operator.sock");
        std::fs::write(&socket, "").unwrap();
        let (socket, directory) = (socket.display().to_string(), root.path().join("appliance-requests"));
        let timeout = Duration::from_secs(600);

        let first = freeze(&socket, request(7, 1_000, &["volume-b", "volume-a", "volume-b"]), timeout, &directory).unwrap();
        assert_eq!(first, request(7, 1_000, &["volume-a", "volume-b"]));
        let file = std::fs::read_dir(&directory).unwrap().next().unwrap().unwrap().path();
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "{\"timeout_nanos\":600000000000,\"request\":{\"installation_id\":\"mcp-1\",\"request_id\":\"remove-1\",\"expected_revision\":7,\"deadline_unix_millis\":1000,\"confirmed_resource_ids\":[\"volume-a\",\"volume-b\"]}}\n"
        );

        // A retry at a later time runs with the saved deadline.
        assert_eq!(freeze(&socket, request(7, 9_000, &["volume-a", "volume-b"]), timeout, &directory).unwrap(), first);
        for changed in [request(8, 9_000, &["volume-a", "volume-b"]), request(7, 9_000, &["volume-a"])] {
            assert!(freeze(&socket, changed, timeout, &directory).is_err(), "a request ID is bound to one confirmed command");
        }
        assert!(freeze(&socket, request(7, 9_000, &["volume-a", "volume-b"]), Duration::from_secs(60), &directory).is_err());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1, "no temporary file remains");
        assert!(freeze(&root.path().join("absent.sock").display().to_string(), request(7, 1, &["volume-a"]), timeout, &directory).is_err());
    }

    #[test]
    fn the_retry_command_quotes_each_argument() {
        let command = retry_command("/data/operator's.sock", &request(7, 1_000, &["volume-a"]), Duration::from_secs(600));
        assert!(command.ends_with("'appliance' 'remove' '--operator-socket' '/data/operator'\"'\"'s.sock' '--installation' 'mcp-1' '--request-id' 'remove-1' '--execute' '--expected-revision' '7' '--timeout' '10m0s' '--confirm-resource' 'volume-a'"), "{command}");
    }
}
