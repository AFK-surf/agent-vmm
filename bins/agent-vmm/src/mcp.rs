//! `agent-vmm mcp token`: the Host owner issues an MCP client token, or rotates the key that signs all tokens.

use crate::appliance::{Outcome, bounded, watch_operation};
use crate::flags::{Flags, Kind};
use crate::helper;
use agent_vmm_proto::appliance::v1::ApplianceOperation;
use agent_vmm_proto::appliance::v1::appliance_admin_service_client::ApplianceAdminServiceClient;
use agent_vmm_proto::host::v1 as hostv1;
use hostv1::operator_service_client::OperatorServiceClient;
use std::path::Path;
use std::time::Duration;
use tokio::time::Instant;

const USAGE: &str = "usage: agent-vmm mcp token create|rotate [--operator-socket PATH]";
const TIMEOUT: Duration = Duration::from_secs(600);

pub fn run(arguments: &[String]) -> i32 {
    let action = match arguments {
        [token, action, ..] if token == "token" && matches!(action.as_str(), "create" | "rotate") => action.as_str(),
        _ => {
            eprintln!("{USAGE}");
            return 2;
        }
    };
    let flags = match Flags::parse(&arguments[2..], &[("operator-socket", Kind::Value)]) {
        Ok(flags) if flags.rest.is_empty() => flags,
        Ok(_) => return 2,
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            return 2;
        }
    };
    let socket = flags.get("operator-socket").map_or_else(helper::default_operator_socket, str::to_string);
    match token(action, &socket) {
        Ok(()) => 0,
        Err(fault) => {
            eprintln!("{}", fault.message);
            1
        }
    }
}

fn token(action: &str, socket: &str) -> Outcome<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let end = Instant::now() + TIMEOUT;
        let channel = vmm_lifecycle::grpc::channel(Path::new(socket)).map_err(|error| error.message)?;
        let mut operator = OperatorServiceClient::new(channel.clone());
        if action == "create" {
            println!("{}", bounded(end, operator.create_mcp_token(hostv1::CreateMcpTokenRequest {})).await?.token);
            return Ok(());
        }
        // A rotation is an operation of the MCP appliance: every issued token stops working when it ends.
        let operation_id = bounded(end, operator.rotate_mcp_token_key(hostv1::RotateMcpTokenKeyRequest {})).await?.operation_id;
        let operation = ApplianceOperation { operation_id, ..ApplianceOperation::default() };
        let mut appliances = ApplianceAdminServiceClient::new(channel);
        watch_operation(&mut appliances, end, "agent-vmm-mcp", &operation).await
    })
}
