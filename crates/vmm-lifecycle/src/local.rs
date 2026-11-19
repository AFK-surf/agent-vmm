//! The `local-*` commands relay one `host.v1.OperatorService` request. Standard
//! input and output are protobuf JSON with the default options: lowerCamel
//! keys, enum names, 64-bit integers as decimal strings, and no default values.

use crate::error::{Deadline, Error, Kind, Result, err};
use crate::{Context, Paths};
use base64::Engine;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};
use std::sync::OnceLock;
use std::time::Duration;

const INPUT_LIMIT: usize = 8192;
const BUDGET: Duration = Duration::from_secs(15);

/// The request and response message of each command, in `agentvmm.host.v1`.
fn messages(command: &str) -> Option<(&'static str, &'static str)> {
    Some(match command {
        "local-disk" => ("GetLocalDiskSummaryRequest", "GetLocalDiskSummaryResponse"),
        "local-environments" => ("ListLocalEnvironmentPageRequest", "ListLocalEnvironmentPageResponse"),
        "local-registrations" => ("ListLocalRegistrationPageRequest", "ListLocalRegistrationPageResponse"),
        "local-plan" => ("PlanLocalDisposalRequest", "PlanLocalDisposalResponse"),
        "local-execute" => ("ExecuteLocalDisposalRequest", "ExecuteLocalDisposalResponse"),
        "local-operation" => ("GetOperationRequest", "GetOperationResponse"),
        "registration-recovery-proof" => ("SignRegistrationRecoveryRequest", "SignRegistrationRecoveryResponse"),
        _ => return None,
    })
}

pub fn is_local_command(command: &str) -> bool {
    messages(command).is_some()
}

fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| DescriptorPool::decode(agent_vmm_proto::FILE_DESCRIPTOR_SET).expect("the generated descriptor set decodes"))
}

pub(crate) fn dynamic(name: &str, encoded: &[u8]) -> Result<DynamicMessage> {
    let descriptor = pool().get_message_by_name(&format!("agentvmm.host.v1.{name}")).ok_or_else(|| err(format!("unknown message {name}")))?;
    DynamicMessage::decode(descriptor, encoded).map_err(|error| err(error.to_string()))
}

/// Encodes a message of the generated packages as protobuf JSON. `name` is the full message name.
pub fn protojson(name: &str, encoded: &[u8]) -> Result<serde_json::Value> {
    let descriptor = pool().get_message_by_name(name).ok_or_else(|| err(format!("unknown message {name}")))?;
    let message = DynamicMessage::decode(descriptor, encoded).map_err(|error| err(error.to_string()))?;
    Ok(serde_json::to_value(&message)?)
}

fn decode_request(name: &str, raw: &[u8]) -> Result<Vec<u8>> {
    let descriptor = pool().get_message_by_name(&format!("agentvmm.host.v1.{name}")).ok_or_else(|| err(format!("unknown message {name}")))?;
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let message = DynamicMessage::deserialize(descriptor, &mut deserializer)?;
    deserializer.end()?;
    Ok(message.encode_to_vec())
}

/// Runs one command and returns the line for standard output.
pub fn run(context: &Context, paths: &Paths, command: &str, input: &[u8]) -> Result<String> {
    let (request, response) = messages(command).ok_or_else(|| err("unknown local operator command"))?;
    if input.is_empty() || input.len() > INPUT_LIMIT {
        return Err(err("local operator request exceeds its input budget"));
    }
    let encoded = if command == "registration-recovery-proof" {
        // The Host parses the fixed-purpose challenge. This command signs no other bytes.
        agent_vmm_proto::host::v1::SignRegistrationRecoveryRequest { challenge_json: input.to_vec() }.encode_to_vec()
    } else {
        decode_request(request, input)?
    };
    let mut operator = context.operator(paths)?;
    let result = operator.local(command, &encoded, Deadline::after(BUDGET))?;
    Ok(serde_json::to_string(&dynamic(response, &result)?)?)
}

/// The error object of a failed command. Electron reads `code` and `reason`.
pub fn error_envelope(error: &Error) -> serde_json::Value {
    let code = match &error.kind {
        Kind::Rpc { code, .. } => code.name(),
        _ => "Unknown",
    };
    serde_json::json!({"error": {"code": code, "reason": error.rpc_reason().unwrap_or("LOCAL_OPERATOR_FAILED")}})
}

/// Encodes a message as Go `encoding/json` encodes the generated struct: proto
/// field names, numbers for enums and integers, and set fields only. The
/// `status` report has carried Host resources in this form since the Go Host.
pub(crate) fn go_json(message: &DynamicMessage) -> serde_json::Value {
    serde_json::Value::Object(message.fields().map(|(field, value)| (field.name().to_string(), go_value(value))).collect())
}

fn go_value(value: &Value) -> serde_json::Value {
    use serde_json::json;
    match value {
        Value::Bool(value) => json!(value),
        Value::I32(value) => json!(value),
        Value::I64(value) => json!(value),
        Value::U32(value) => json!(value),
        Value::U64(value) => json!(value),
        Value::F32(value) => json!(value),
        Value::F64(value) => json!(value),
        Value::String(value) => json!(value),
        Value::Bytes(value) => json!(base64::engine::general_purpose::STANDARD.encode(value)),
        Value::EnumNumber(value) => json!(value),
        Value::Message(value) => go_json(value),
        Value::List(values) => values.iter().map(go_value).collect(),
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| {
                    let key = match key {
                        MapKey::Bool(key) => key.to_string(),
                        MapKey::I32(key) => key.to_string(),
                        MapKey::I64(key) => key.to_string(),
                        MapKey::U32(key) => key.to_string(),
                        MapKey::U64(key) => key.to_string(),
                        MapKey::String(key) => key.clone(),
                    };
                    (key, go_value(value))
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_vmm_proto::host::v1 as hostv1;
    use serde_json::json;

    fn protojson(name: &str, message: &impl Message) -> serde_json::Value {
        serde_json::to_value(dynamic(name, &message.encode_to_vec()).unwrap()).unwrap()
    }

    #[test]
    fn disk_accounting_prints_set_zero_values_as_decimal_strings() {
        let disk = hostv1::DiskAccounting { used_bytes: Some(0), capacity_bytes: Some(68_719_476_736), import_reservation_bytes: Some(0), ..Default::default() };
        let response = hostv1::GetLocalDiskSummaryResponse { guest_state_fs: Some(disk), boot_id: "boot-1".into(), ..Default::default() };
        let value = protojson("GetLocalDiskSummaryResponse", &response);
        assert_eq!(value["guestStateFs"], json!({"usedBytes": "0", "capacityBytes": "68719476736", "importReservationBytes": "0"}));
        assert_eq!(value["bootId"], json!("boot-1"));
    }

    #[test]
    fn environment_pages_use_names_strings_and_rfc3339() {
        let environment = hostv1::LocalEnvironmentSummary { id: "env-1".into(), origin: "local".into(), state: "observed_active".into(), revision: 1, ..Default::default() };
        let response = hostv1::ListLocalEnvironmentPageResponse {
            environments: vec![environment],
            collected_at: Some(prost_types::Timestamp { seconds: 1_795_000_000, nanos: 0 }),
            boot_id: "boot-1".into(),
            ..Default::default()
        };
        let value = protojson("ListLocalEnvironmentPageResponse", &response);
        assert_eq!(value["environments"][0]["revision"], json!("1"));
        assert_eq!(value["collectedAt"], json!("2026-11-18T11:06:40Z"));
        assert!(value.get("nextCursor").is_none(), "a default value is omitted: {value}");
    }

    #[test]
    fn requests_accept_protojson_and_reject_unknown_keys() {
        let encoded = decode_request("PlanLocalDisposalRequest", br#"{"targetKind":"LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT","targetId":"env-1"}"#).unwrap();
        let request = hostv1::PlanLocalDisposalRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!((request.target_kind(), request.target_id.as_str()), (hostv1::LocalDisposalTargetKind::Environment, "env-1"));
        assert!(decode_request("GetLocalDiskSummaryRequest", b"{}").unwrap().is_empty());
        assert!(decode_request("GetLocalDiskSummaryRequest", br#"{"other":1}"#).is_err());
        assert!(decode_request("GetLocalDiskSummaryRequest", b"{} {}").is_err());
    }

    #[test]
    fn failures_name_the_grpc_code_and_the_host_reason() {
        let unimplemented = Error::new(Kind::Rpc { code: crate::RpcCode::Unimplemented, reason: None }, "old Host");
        assert_eq!(error_envelope(&unimplemented), json!({"error": {"code": "Unimplemented", "reason": "LOCAL_OPERATOR_FAILED"}}));
        let disposed = Error::new(Kind::Rpc { code: crate::RpcCode::FailedPrecondition, reason: Some("LOCAL_DISPOSED".into()) }, "gone");
        assert_eq!(error_envelope(&disposed), json!({"error": {"code": "FailedPrecondition", "reason": "LOCAL_DISPOSED"}}));
        assert_eq!(error_envelope(&err("bad input"))["error"]["code"], json!("Unknown"));
    }

    #[test]
    fn go_json_keeps_proto_names_and_numbers() {
        let resources = hostv1::LayerResources { memory_bytes: Some(0), pids: Some(7), unavailable_reason: String::new(), ..Default::default() };
        let value = go_json(&dynamic("LayerResources", &resources.encode_to_vec()).unwrap());
        assert_eq!(value, json!({"memory_bytes": 0, "pids": 7}));
    }
}
