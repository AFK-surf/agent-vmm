//! `agent-vmm appliance`: the Host owner installs, updates and operates a controller appliance.

use crate::flags::{Flags, Kind};
use crate::helper;
use agent_vmm_proto::appliance::v1 as appliancev1;
use agent_vmm_proto::host::v1 as hostv1;
use appliancev1::appliance_admin_service_client::ApplianceAdminServiceClient;
use appliancev1::{ApplianceOperation, ApplianceOperationState, ApprovedGrant, ControllerApplianceManifest};
use prost::Message;
use std::future::Future;
use std::io::Read;
use std::net::SocketAddrV4;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;


pub const USAGE: &str = "usage: agent-vmm appliance install|update|status|recover|stop|start|activate|restart|revoke|remove ...";
pub type Client = ApplianceAdminServiceClient;

/// A failed appliance command. `status` is kept for a failure that the Host returned.
#[derive(Debug)]
pub struct Fault {
    pub message: String,
    pub status: Option<vmm_rpc::Status>,
}

impl Fault {
    pub fn context(mut self, prefix: &str) -> Self {
        self.message = format!("{prefix}: {}", self.message);
        self
    }
}

impl From<vmm_rpc::Status> for Fault {
    fn from(status: vmm_rpc::Status) -> Self {
        Self { message: vmm_lifecycle::rpc::status_error(&status).message, status: Some(status) }
    }
}

impl From<&str> for Fault {
    fn from(message: &str) -> Self {
        Self { message: message.into(), status: None }
    }
}

impl From<String> for Fault {
    fn from(message: String) -> Self {
        Self { message, status: None }
    }
}

impl From<std::io::Error> for Fault {
    fn from(error: std::io::Error) -> Self {
        error.to_string().into()
    }
}

pub type Outcome<T> = Result<T, Fault>;

/// Makes one call that ends with the command deadline.
pub async fn bounded<T>(end: Instant, call: impl Future<Output = Result<vmm_rpc::Response<T>, vmm_rpc::Status>>) -> Outcome<T> {
    match tokio::time::timeout_at(end, call).await {
        Ok(result) => Ok(result?.into_inner()),
        Err(_) => Err(vmm_rpc::Status::deadline_exceeded("context deadline exceeded").into()),
    }
}

pub fn request_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the system provides random bytes");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn unix_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis() as i64)
}

/// A message of the appliance package as indented protobuf JSON.
pub fn message_text(name: &str, message: &impl Message) -> String {
    let value = vmm_lifecycle::protojson(&format!("agentvmm.appliance.v1.{name}"), &message.encode_to_vec()).unwrap_or_default();
    serde_json::to_string_pretty(&value).unwrap_or_default()
}

/// Runs an appliance command to its end within `timeout`.
pub fn block_on<F: Future<Output = Outcome<()>>>(socket: &str, timeout: Duration, command: impl FnOnce(Client, Instant) -> F) -> Outcome<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let channel = vmm_rpc::Client::unix(socket);
        command(ApplianceAdminServiceClient::new(channel), Instant::now() + timeout).await
    })
}

/// Runs `agent-vmm appliance ...`. Returns `None` for a subcommand that this command does not have.
pub fn run(arguments: &[String]) -> Option<i32> {
    let result = match arguments.first().map(String::as_str)? {
        "--help" | "-h" => {
            println!("{USAGE}");
            return Some(0);
        }
        "status" => status(&arguments[1..]),
        "remove" => crate::appliance_remove::remove(&arguments[1..]),
        command @ ("install" | "update" | "recover" | "stop" | "start" | "activate" | "restart" | "revoke") => lifecycle(command, &arguments[1..]),
        _ => return None,
    };
    Some(report(result))
}

/// A flag error ends with status 2, and a failed command with status 1.
pub fn report(result: Result<Outcome<()>, String>) -> i32 {
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(fault)) => {
            eprintln!("agent-vmm: {}", fault.message);
            1
        }
        Err(usage) => {
            eprintln!("{usage}\n{USAGE}");
            2
        }
    }
}

fn status(arguments: &[String]) -> Result<Outcome<()>, String> {
    let flags = Flags::parse(arguments, &[("operator-socket", Kind::Value)])?;
    let socket = flags.get("operator-socket").map_or_else(helper::default_operator_socket, str::to_string);
    Ok(block_on(&socket, Duration::from_secs(10), |mut client, end| async move {
        let listed = bounded(end, client.list_appliances(appliancev1::ListAppliancesRequest {})).await?;
        for appliance in &listed.appliances {
            let health = appliance.health.clone().unwrap_or_default();
            println!(
                "id={} state={} channel={} enabled={} healthy={} status={} boot={} container={}",
                appliance.installation_id,
                appliance.appliance_state().as_str_name(),
                appliance.channel_state().as_str_name(),
                appliance.enabled,
                health.healthy,
                health.status,
                appliance.boot_id,
                appliance.container_instance
            );
            for frontend in &appliance.frontends {
                println!("  frontend={} endpoint={} available={} error_reason={}", frontend.name, frontend.endpoint, frontend.available, frontend.error_reason);
            }
        }
        Ok(())
    }))
}

/// The resource limits that the operator approves in place of the limits of the manifest.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Resources {
    pub system_pids: Option<u64>,
    pub system_disk_gib: Option<u64>,
    pub environment_pids: Option<u64>,
    pub environment_disk_gib: Option<u64>,
}

impl Resources {
    fn from_flags(flags: &Flags) -> Result<Self, String> {
        Ok(Self { system_pids: flags.number("system-pids")?, system_disk_gib: flags.number("system-disk-gib")?, environment_pids: flags.number("environment-pids")?, environment_disk_gib: flags.number("environment-disk-gib")? })
    }

    /// Lowers the limits of the grant. A value above the limit of the manifest is refused.
    pub fn apply(&self, grant: &mut ApprovedGrant, manifest: &ControllerApplianceManifest) -> Outcome<()> {
        approve("system", self.system_pids, self.system_disk_gib, grant.system_limits.as_mut(), manifest.system_limits.as_ref())?;
        let target = grant.capacity.as_mut().and_then(|capacity| capacity.per_environment_limits.as_mut());
        let maximum = manifest.requested_capacity.as_ref().and_then(|capacity| capacity.per_environment_limits.as_ref());
        approve("environment", self.environment_pids, self.environment_disk_gib, target, maximum)
    }
}

fn approve(scope: &str, pids: Option<u64>, disk_gib: Option<u64>, target: Option<&mut hostv1::ResourceLimits>, maximum: Option<&hostv1::ResourceLimits>) -> Outcome<()> {
    let unit = match (pids, disk_gib) {
        (None, None) => return Ok(()),
        (Some(_), _) => "pids",
        (None, Some(_)) => "disk-gib",
    };
    let (Some(target), Some(maximum)) = (target, maximum) else {
        return Err(format!("--{scope}-{unit} requires manifest resource limits").into());
    };
    if let Some(value) = pids {
        let limit = maximum.pids.unwrap_or(0);
        target.pids = Some(u32::try_from(value).ok().filter(|value| (1..=limit).contains(value)).ok_or_else(|| format!("--{scope}-pids must be positive and at most manifest limit {limit}"))?);
    }
    if let Some(value) = disk_gib {
        let bytes = value.checked_mul(1 << 30).filter(|bytes| *bytes != 0).ok_or_else(|| format!("--{scope}-disk-gib must be a positive GiB value without overflow"))?;
        let limit = maximum.disk_bytes.unwrap_or(0);
        if bytes > limit {
            return Err(format!("--{scope}-disk-gib exceeds manifest disk limit {limit} bytes").into());
        }
        target.disk_bytes = Some(bytes);
    }
    Ok(())
}

/// The grant that the operator approves for a release. An update keeps the previous grant unless the operator approves the changes.
pub fn lifecycle_grant(manifest: &ControllerApplianceManifest, previous: Option<&ApprovedGrant>, endpoints: Option<&str>, approve_changes: bool) -> Outcome<ApprovedGrant> {
    let mut grant = ApprovedGrant {
        system_limits: manifest.system_limits,
        capacity: manifest.requested_capacity,
        egress_mode: manifest.requested_egress_mode,
        frontends: manifest.requested_frontends.clone(),
        lan_egress: manifest.requested_lan_egress,
        ..ApprovedGrant::default()
    };
    match previous {
        Some(previous) if !approve_changes => grant = previous.clone(),
        Some(previous) => grant.host_tcp_endpoints = previous.host_tcp_endpoints.clone(),
        None => {}
    }
    if let Some(endpoints) = endpoints {
        grant.host_tcp_endpoints.clear();
        for value in endpoints.split(',').filter(|value| !value.is_empty()) {
            let endpoint = value.parse::<SocketAddrV4>().ok().filter(|endpoint| endpoint.port() != 0).ok_or("--host-tcp-endpoints requires exact IPv4:TCP-port values")?;
            grant.host_tcp_endpoints.push(appliancev1::HostTcpEndpoint { ip: endpoint.ip().to_string(), port: u32::from(endpoint.port()) });
        }
    }
    Ok(grant)
}

pub fn select_installation<'a>(statuses: &'a [appliancev1::ApplianceStatus], product: &str, installation: &str) -> Outcome<&'a appliancev1::ApplianceStatus> {
    let mut matches = statuses.iter().filter(|candidate| (installation.is_empty() || candidate.installation_id == installation) && (product.is_empty() || candidate.product_id == product));
    let selected = matches.next().ok_or("no installation matches")?;
    if matches.next().is_some() {
        return Err("more than one installation matches; specify --installation".into());
    }
    Ok(selected)
}

/// Waits for the next message of a stream. The end of the stream is an error.
pub async fn next_message<T: Send + 'static>(end: Instant, stream: &mut vmm_rpc::Streaming<T>) -> Outcome<T> {
    match tokio::time::timeout_at(end, stream.message()).await {
        Ok(message) => Ok(message?.ok_or("EOF")?),
        Err(_) => Err(vmm_rpc::Status::deadline_exceeded("context deadline exceeded").into()),
    }
}

/// Prints each phase of an operation until the operation ends.
pub async fn watch_operation(client: &mut Client, end: Instant, installation: &str, operation: &ApplianceOperation) -> Outcome<()> {
    println!("installation={installation} operation={} phase={}", operation.operation_id, operation.phase().as_str_name());
    let request = appliancev1::WatchApplianceOperationRequest { installation_id: installation.to_string(), operation_id: operation.operation_id.clone() };
    let mut watch = bounded(end, client.watch_appliance_operation(request)).await?;
    let mut last_phase = appliancev1::ApplianceOperationPhase::Unspecified;
    loop {
        let operation = next_message(end, &mut watch).await?.operation.unwrap_or_default();
        if operation.phase() != last_phase {
            println!("phase={} state={}", operation.phase().as_str_name(), operation.state().as_str_name());
            last_phase = operation.phase();
        }
        match operation.state() {
            ApplianceOperationState::Succeeded => return Ok(()),
            ApplianceOperationState::Running => {}
            _ => {
                let reason = operation.error.as_ref().map(|error| error.reason.as_str()).unwrap_or_default();
                return Err(format!("operation {} retained phase {}: {reason}", operation.operation_id, operation.phase().as_str_name()).into());
            }
        }
    }
}

/// The arguments of one lifecycle command.
pub(crate) struct Lifecycle {
    pub command: String,
    pub product: String,
    pub installation: String,
    pub package: String,
    pub operation: String,
    pub timeout: Duration,
    pub bootstrap_stdin: bool,
    /// The approved Host endpoints, when the operator named them.
    pub endpoints: Option<String>,
    pub approve_changes: bool,
    pub resources: Resources,
}

fn lifecycle(command: &str, arguments: &[String]) -> Result<Outcome<()>, String> {
    use Kind::{Boolean, Value};
    let accepted = [
        ("operator-socket", Value),
        ("product", Value),
        ("installation", Value),
        ("package", Value),
        ("operation", Value),
        ("timeout", Value),
        ("bootstrap-stdin", Boolean),
        ("host-tcp-endpoints", Value),
        ("approve-grant-changes", Boolean),
        ("system-pids", Value),
        ("system-disk-gib", Value),
        ("environment-pids", Value),
        ("environment-disk-gib", Value),
    ];
    let flags = Flags::parse(arguments, &accepted)?;
    let socket = flags.get("operator-socket").map_or_else(owner_socket, str::to_string);
    let request = Lifecycle {
        command: command.to_string(),
        product: flags.text("product"),
        installation: flags.text("installation"),
        package: flags.text("package"),
        operation: flags.text("operation"),
        timeout: flags.duration("timeout", Duration::from_secs(600))?,
        bootstrap_stdin: flags.enabled("bootstrap-stdin"),
        endpoints: flags.get("host-tcp-endpoints").map(str::to_string),
        approve_changes: flags.enabled("approve-grant-changes"),
        resources: Resources::from_flags(&flags)?,
    };
    if request.timeout.is_zero() {
        return Ok(Err("timeout must be positive".into()));
    }
    let timeout = request.timeout;
    Ok(block_on(&socket, timeout, |client, end| crate::appliance_lifecycle::run(client, end, request)))
}

/// The operator socket of a Host that the current user owns.
pub fn owner_socket() -> String {
    helper::platform_data_directory(&helper::home()).join("operator.sock").display().to_string()
}

/// Reads the bootstrap seed of an installation from standard input.
pub(crate) fn read_seed(input: &mut dyn Read) -> Outcome<Vec<u8>> {
    let mut seed = Vec::new();
    input.take((1 << 20) + 1).read_to_end(&mut seed).map_err(|_| "cannot read bootstrap seed from stdin")?;
    if seed.is_empty() || seed.len() > 1 << 20 {
        return Err("bootstrap seed must contain 1 to 1048576 bytes".into());
    }
    Ok(seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn manifest() -> ControllerApplianceManifest {
        let limits = Some(hostv1::ResourceLimits { pids: Some(1024), disk_bytes: Some(16 * GIB) });
        ControllerApplianceManifest { system_limits: limits, requested_capacity: Some(appliancev1::CapacityGrant { per_environment_limits: limits, ..Default::default() }), requested_lan_egress: true, ..Default::default() }
    }

    #[test]
    fn approved_limits_replace_only_the_named_limits_within_the_manifest() {
        let mut previous = lifecycle_grant(&manifest(), None, None, true).unwrap();
        previous.capacity.as_mut().unwrap().per_environment_limits.as_mut().unwrap().disk_bytes = Some(8 * GIB);
        let approve = |resources: Resources| {
            let mut grant = lifecycle_grant(&manifest(), Some(&previous), None, false).unwrap();
            resources.apply(&mut grant, &manifest()).map(|()| grant)
        };
        let grant = approve(Resources { environment_disk_gib: Some(4), system_pids: Some(256), ..Resources::default() }).unwrap();
        let (system, environment) = (grant.system_limits.unwrap(), grant.capacity.unwrap().per_environment_limits.unwrap());
        assert_eq!((environment.disk_bytes, environment.pids, system.pids, system.disk_bytes), (Some(4 * GIB), Some(1024), Some(256), Some(16 * GIB)));
        for refused in [
            Resources { environment_disk_gib: Some(0), ..Resources::default() },
            Resources { environment_disk_gib: Some(u64::MAX), ..Resources::default() },
            Resources { environment_disk_gib: Some(17), ..Resources::default() },
            Resources { system_pids: Some(1025), ..Resources::default() },
            Resources { system_pids: Some(0), ..Resources::default() },
        ] {
            assert!(approve(refused.clone()).is_err(), "{refused:?}");
        }
        let mut unlimited = ApprovedGrant::default();
        assert_eq!(Resources { system_pids: Some(1), ..Resources::default() }.apply(&mut unlimited, &ControllerApplianceManifest::default()).unwrap_err().message, "--system-pids requires manifest resource limits");
        assert!(Resources::default().apply(&mut unlimited, &ControllerApplianceManifest::default()).is_ok());
    }

    #[test]
    fn an_update_keeps_the_grant_unless_the_operator_approves_a_change() {
        let endpoint = |ip: &str, port| appliancev1::HostTcpEndpoint { ip: ip.into(), port };
        let previous = ApprovedGrant { revision: 7, host_tcp_endpoints: vec![endpoint("192.168.1.10", 3000)], ..Default::default() };
        let grant = |endpoints, approve| lifecycle_grant(&manifest(), Some(&previous), endpoints, approve);
        assert_eq!(grant(None, false).unwrap(), previous, "an update of the content changes no authority");
        let approved = grant(None, true).unwrap();
        assert_eq!((approved.lan_egress, approved.revision, &approved.host_tcp_endpoints), (true, 0, &previous.host_tcp_endpoints));
        let replaced = grant(Some("192.168.1.20:4000,"), false).unwrap();
        assert_eq!((replaced.lan_egress, replaced.revision, replaced.host_tcp_endpoints), (false, 7, vec![endpoint("192.168.1.20", 4000)]));
        assert!(grant(Some(""), false).unwrap().host_tcp_endpoints.is_empty());
        for invalid in ["localhost:3000", "192.168.1.20", "192.168.1.20:0", "[::1]:3000"] {
            assert!(grant(Some(invalid), false).is_err(), "{invalid}");
        }
    }

    #[test]
    fn a_command_selects_exactly_one_installation() {
        let status = |installation: &str, product: &str| appliancev1::ApplianceStatus { installation_id: installation.into(), product_id: product.into(), ..Default::default() };
        let installed = [status("mcp-1", "mcp"), status("mcp-2", "mcp"), status("build-1", "build")];
        assert_eq!(select_installation(&installed, "build", "").unwrap().installation_id, "build-1");
        assert_eq!(select_installation(&installed, "", "mcp-2").unwrap().installation_id, "mcp-2");
        assert_eq!(select_installation(&installed, "mcp", "").unwrap_err().message, "more than one installation matches; specify --installation");
        assert_eq!(select_installation(&installed, "build", "mcp-1").unwrap_err().message, "no installation matches");
        assert_eq!(read_seed(&mut &b"seed"[..]).unwrap(), b"seed");
        assert!(read_seed(&mut std::io::empty()).is_err() && read_seed(&mut std::io::repeat(1)).is_err());
        assert_eq!(request_id().len(), 32);
    }
}
