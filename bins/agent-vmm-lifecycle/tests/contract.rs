//! Runs the built helper against a Host operator on a Unix socket and checks
//! what Comma reads from standard output with the consumer models of the Comma
//! contract suite. With `AGENT_VMM_CONTRACT_OUTPUT_DIR` the outputs are also
//! saved there for the live probes of that suite.

use agent_vmm_proto::host::v1 as hostv1;
use bytes::{Buf, BufMut, Bytes};
use prost::Message;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::{Request, Response, Status};
use vmm_comma_contract_tests::consumer;

const SERVICE: &str = "agentvmm.host.v1.OperatorService";

/// Passes message bytes through, so the test serves any method without the generated server trait.
#[derive(Clone, Copy, Default)]
struct Raw;

impl Codec for Raw {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Raw;
    type Decoder = Raw;
    fn encoder(&mut self) -> Raw {
        Raw
    }
    fn decoder(&mut self) -> Raw {
        Raw
    }
}

impl Encoder for Raw {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, buffer: &mut EncodeBuf<'_>) -> Result<(), Status> {
        buffer.put(item);
        Ok(())
    }
}

impl Decoder for Raw {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, buffer: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(buffer.copy_to_bytes(buffer.remaining())))
    }
}

fn timestamp() -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds: 1_795_000_000, nanos: 0 })
}

fn operation() -> Option<hostv1::Operation> {
    let resource = hostv1::ResourceReference { kind: "environment".into(), environment_id: "env-1".into(), ..Default::default() };
    Some(hostv1::Operation {
        id: "operation-1".into(),
        kind: "local-disposal".into(),
        resource: Some(resource),
        state: hostv1::OperationState::Succeeded as i32,
        request_id: "request-1".into(),
        started_at: timestamp(),
        updated_at: timestamp(),
        ..Default::default()
    })
}

/// The answers of a Host with one local environment and one registration.
fn answer(method: &str) -> Result<Vec<u8>, Status> {
    Ok(match method {
        "ListLocalEnvironmentPage" => {
            let environment = hostv1::LocalEnvironmentSummary { id: "env-1".into(), origin: "local".into(), state: "observed_active".into(), revision: 1, private_disk_bytes: Some(0), sampled_at: timestamp(), ..Default::default() };
            hostv1::ListLocalEnvironmentPageResponse { environments: vec![environment], collected_at: timestamp(), boot_id: "boot-1".into(), ..Default::default() }.encode_to_vec()
        }
        "GetLocalDiskSummary" => {
            // A sparse pool reports its virtual capacity. The three counters are set even when zero.
            let disk = hostv1::DiskAccounting { used_bytes: Some(0), capacity_bytes: Some(68_719_476_736), import_reservation_bytes: Some(0), ..Default::default() };
            hostv1::GetLocalDiskSummaryResponse { collected_at: timestamp(), boot_id: "boot-1".into(), vm_state: hostv1::VmState::Running as i32, guest_state_fs: Some(disk), ..Default::default() }.encode_to_vec()
        }
        "ListLocalRegistrationPage" => {
            let registration = hostv1::LocalRegistrationMetadata { id: "registration-1".into(), revision: 3, state: hostv1::RemoteRegistrationState::Enabled as i32 };
            hostv1::ListLocalRegistrationPageResponse { registrations: vec![registration], ..Default::default() }.encode_to_vec()
        }
        "PlanLocalDisposal" => {
            let plan = hostv1::LocalDisposalPlan { target_kind: hostv1::LocalDisposalTargetKind::Environment as i32, target_id: "env-1".into(), revision: 1, ..Default::default() };
            hostv1::PlanLocalDisposalResponse { plan: Some(plan) }.encode_to_vec()
        }
        "ExecuteLocalDisposal" => hostv1::ExecuteLocalDisposalResponse { operation: operation() }.encode_to_vec(),
        "GetOperation" => hostv1::GetOperationResponse { operation: operation() }.encode_to_vec(),
        _ => return Err(Status::unimplemented("the test Host does not serve this method")),
    })
}

struct Method(String);

impl tonic::server::UnaryService<Bytes> for Method {
    type Response = Bytes;
    type Future = std::future::Ready<Result<Response<Bytes>, Status>>;
    fn call(&mut self, _: Request<Bytes>) -> Self::Future {
        std::future::ready(answer(&self.0).map(|message| Response::new(Bytes::from(message))))
    }
}

#[derive(Clone)]
struct Operator;

impl tonic::server::NamedService for Operator {
    const NAME: &'static str = SERVICE;
}

impl tower::Service<http::Request<tonic::body::Body>> for Operator {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Infallible>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let method = request.uri().path().rsplit('/').next().unwrap_or_default().to_string();
        Box::pin(async move { Ok(tonic::server::Grpc::new(Raw).unary(Method(method), request).await) })
    }
}

struct Installation {
    _directory: tempfile::TempDir,
    helper: PathBuf,
    home: PathBuf,
    _runtime: tokio::runtime::Runtime,
}

impl Installation {
    fn new() -> Self {
        // A short path keeps the operator socket below the address limit of a Unix socket.
        let directory = tempfile::Builder::new().prefix("avl").tempdir_in("/tmp").unwrap();
        let home = directory.path().canonicalize().unwrap();
        let (helpers, data) = if cfg!(target_os = "macos") {
            (home.join("staged/Agent VMM Host.app/Contents/Helpers"), home.join("Library/Application Support/AgentVMM"))
        } else {
            (home.join("release"), home.join("var/lib/agent-vmm"))
        };
        std::fs::create_dir_all(&helpers).unwrap();
        std::fs::create_dir_all(&data).unwrap();
        let helper = helpers.join("agent-vmm-lifecycle");
        std::fs::copy(env!("CARGO_BIN_EXE_agent-vmm-lifecycle"), &helper).unwrap();

        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
        let listener = {
            let _guard = runtime.enter();
            tokio::net::UnixListener::bind(data.join("operator.sock")).unwrap()
        };
        runtime.spawn(tonic::transport::Server::builder().add_service(Operator).serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener)));
        Self { _directory: directory, helper, home, _runtime: runtime }
    }

    /// Runs the helper and returns its exit status, standard output and standard error.
    fn run(&self, arguments: &[&str], input: &str) -> (bool, String, String) {
        let mut child = Command::new(&self.helper)
            .args(arguments)
            .env("AGENT_VMM_E2E_HOME", &self.home)
            .env("AGENT_VMM_E2E_ROOT", &self.home)
            .env("AGENT_VMM_E2E_HOST_LABEL", format!("com.afk-surf.agent-vmm-host.contract-{}", std::process::id()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap();
        (output.status.success(), String::from_utf8(output.stdout).unwrap(), String::from_utf8(output.stderr).unwrap())
    }

    /// Runs a command that must succeed and print exactly one JSON value.
    fn json(&self, arguments: &[&str], input: &str) -> Value {
        let (success, output, diagnostics) = self.run(arguments, input);
        assert!(success, "{arguments:?} failed: {diagnostics}");
        assert_eq!(output.matches('\n').count(), 1, "{arguments:?} must print one line: {output}");
        serde_json::from_str(&output).unwrap_or_else(|error| panic!("{arguments:?} printed {output}: {error}"))
    }
}

fn save(name: &str, value: &Value) {
    if let Some(directory) = std::env::var_os("AGENT_VMM_CONTRACT_OUTPUT_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(Path::new(&directory).join(name), value.to_string()).unwrap();
    }
}

/// The service options that Electron passes. Linux has one fixed system service and accepts none.
fn service(kind: &'static str) -> Vec<&'static str> {
    if cfg!(target_os = "macos") { vec!["--service-type", kind] } else { Vec::new() }
}

fn command(name: &'static str, kind: &'static str) -> Vec<&'static str> {
    [vec![name], service(kind)].concat()
}

#[test]
fn the_helper_prints_what_comma_reads() {
    let installation = Installation::new();

    let version = installation.json(&["version", "--json"], "");
    assert_eq!((version["contractVersion"].as_u64(), version["component"].as_str()), (Some(1), Some("agent-vmm-host")));
    assert!(version["release_id"].is_string() && version["currentSchema"].is_u64() && version["minimumSchema"].is_u64());
    assert_eq!(consumer::version_report(&version), json!({"contract": "ok", "installed_release_id": version["release_id"]}));
    save("version.json", &version);

    let (success, output, diagnostics) = installation.run(&["comma-contract-unknown-command"], "");
    assert!(!success && diagnostics.contains("unknown command"), "{diagnostics}");
    assert_eq!(consumer::native_command_error(&output), Some(json!({"code": "upgrade_required", "reason": "unknown_command"})));

    let status = installation.json(&[command("status", "agent"), vec!["--registration-id", "registration-1"]].concat(), "");
    assert_eq!(status["contractVersion"], json!(1));
    for key in ["hostInstalled", "hostLoaded", "hostReadable", "hostHealthy", "connectorInstalled", "connectorLoaded"] {
        assert!(status[key].is_boolean(), "status.{key}: {status}");
    }
    assert_eq!(status["registrationRead"], json!("unreadable"), "a Host that the service manager does not run is not asked");
    assert_eq!(consumer::helper_contract_failure(&status), None);
    save("status.json", &status);

    assert_eq!(installation.json(&[command("operation", "agent"), vec!["--request-id", "request-1"]].concat(), ""), Value::Null);

    let environments = installation.json(&command("local-environments", "agent"), "{}");
    assert_eq!(environments["environments"][0], json!({"id": "env-1", "origin": "local", "state": "observed_active", "revision": "1", "privateDiskBytes": "0", "sampledAt": "2026-11-18T11:06:40Z"}));
    assert_eq!((environments["bootId"].as_str(), environments["collectedAt"].as_str()), (Some("boot-1"), Some("2026-11-18T11:06:40Z")));
    consumer::local_environments(&environments).unwrap();
    save("local-environments.json", &environments);

    let disk = installation.json(&command("local-disk", "agent"), "{}");
    assert_eq!(disk["guestStateFs"], json!({"usedBytes": "0", "capacityBytes": "68719476736", "importReservationBytes": "0"}));
    assert_eq!(disk["bootId"], environments["bootId"], "the disk summary and the environment page name the same boot");
    assert_eq!(consumer::local_disk(&disk), Ok(Some(disk["guestStateFs"].clone())));
    save("local-disk.json", &disk);

    let registrations = installation.json(&command("local-registrations", "agent"), "{\"afterId\":\"\"}");
    assert_eq!(registrations["registrations"][0], json!({"id": "registration-1", "revision": "3", "state": "REMOTE_REGISTRATION_STATE_ENABLED"}));
    consumer::local_registrations(&registrations).unwrap();
    save("local-registrations.json", &registrations);

    let plan = installation.json(&command("local-plan", "agent"), "{\"targetKind\":\"LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT\",\"targetId\":\"env-1\"}");
    assert_eq!(plan["plan"], json!({"targetKind": "LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT", "targetId": "env-1", "revision": "1"}));
    consumer::local_plan(&plan, "LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT", "env-1").unwrap();
    let execute = json!({"requestId": "request-1", "plan": plan["plan"], "irreversibleConfirmation": true}).to_string();
    let executed = installation.json(&command("local-execute", "agent"), &execute);
    let operation = &executed["operation"];
    assert_eq!((operation["kind"].as_str(), operation["state"].as_str(), operation["resource"]["environmentId"].as_str()), (Some("local-disposal"), Some("OPERATION_STATE_SUCCEEDED"), Some("env-1")));
    assert_eq!(consumer::local_operation(&executed, "env-1"), Ok("completed"));
    assert_eq!(installation.json(&command("local-operation", "agent"), "{\"operationId\":\"operation-1\"}"), executed);

    // A failure names the gRPC code on standard output, and the command exits non-zero.
    let (success, output, _) = installation.run(&command("registration-recovery-proof", "agent"), "{}");
    assert!(!success);
    assert_eq!(consumer::native_command_error(&output), Some(json!({"code": "Unimplemented", "reason": "LOCAL_OPERATOR_FAILED"})));
    let (success, output, _) = installation.run(&command("local-disk", "agent"), "{\"unknown\":1}");
    assert!(!success);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap()["error"]["code"], json!("Unknown"));

    if cfg!(target_os = "macos") {
        let maintenance = installation.json(&command("maintenance-status", "auto"), "{}");
        assert_eq!(consumer::maintenance_status(&maintenance, None), Ok(None));
        save("maintenance-status.json", &maintenance);
    }
}
