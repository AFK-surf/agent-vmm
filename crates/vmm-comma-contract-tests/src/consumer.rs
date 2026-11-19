//! Electron Main's readers of `agent-vmm-lifecycle` output, as executable models.
//!
//! Source of every rule: `clients/apps/electron/src/main/modules/compute-node/`
//! in `AFK-surf/Cue`. Line references are in each function's comment.

use serde_json::{json, Map, Value};

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// JavaScript truthiness of a JSON value.
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => number.as_f64().is_some_and(|n| n != 0.0),
        Some(Value::String(text)) => !text.is_empty(),
        Some(_) => true,
    }
}

/// `Number.isSafeInteger`.
fn safe_integer(value: Option<&Value>) -> Option<f64> {
    let number = value?.as_f64()?;
    (value?.is_number() && number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER).then_some(number)
}

/// zod `z.string().optional()`: absent or a string. `null` is rejected.
fn optional_string(object: &Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    match object.get(key) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(format!("{key} must be a string when present")),
    }
}

fn required_string(object: &Map<String, Value>, key: &str) -> Result<String, String> {
    match object.get(key) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(format!("{key} must be a string")),
    }
}

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>, String> {
    value.as_object().ok_or_else(|| format!("{what} must be an object"))
}

/// The `decimal` refinement: a canonical uint64 as a JSON string
/// (local-operator.ts:12-18).
pub fn decimal(value: &Value) -> bool {
    let Some(text) = value.as_str() else { return false };
    let canonical = text == "0"
        || (!text.is_empty()
            && !text.starts_with('0')
            && text.bytes().all(|byte| byte.is_ascii_digit()));
    canonical && (text.len() < 20 || (text.len() == 20 && text <= "18446744073709551615"))
}

fn optional_decimal(object: &Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    match object.get(key) {
        None => Ok(None),
        Some(value) if decimal(value) => Ok(value.as_str().map(str::to_owned)),
        Some(_) => Err(format!("{key} must be a decimal uint64 string")),
    }
}

fn required_decimal(object: &Map<String, Value>, key: &str) -> Result<String, String> {
    optional_decimal(object, key)?.ok_or_else(|| format!("{key} is required"))
}

/// zod `z.string().datetime({ offset: true })`.
pub fn datetime_with_offset(text: &str) -> bool {
    let bytes = text.as_bytes();
    let digits = |range: std::ops::Range<usize>| {
        bytes.get(range).is_some_and(|part| part.iter().all(u8::is_ascii_digit))
    };
    if !(digits(0..4) && bytes.get(4) == Some(&b'-') && digits(5..7) && bytes.get(7) == Some(&b'-')
        && digits(8..10) && bytes.get(10) == Some(&b'T') && digits(11..13) && bytes.get(13) == Some(&b':')
        && digits(14..16) && bytes.get(16) == Some(&b':') && digits(17..19))
    {
        return false;
    }
    let mut rest = &text[19..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let length = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if length == 0 {
            return false;
        }
        rest = &fraction[length..];
    }
    if rest == "Z" {
        return true;
    }
    let zone = rest.as_bytes();
    matches!(zone.first(), Some(b'+') | Some(b'-'))
        && zone.len() == 6
        && zone[1..3].iter().all(u8::is_ascii_digit)
        && zone[3] == b':'
        && zone[4..6].iter().all(u8::is_ascii_digit)
}

/// `helperContractFailure` (host-maintenance-protocol.ts:8-18).
pub fn helper_contract_failure(value: &Value) -> Option<&'static str> {
    let version = value.as_object().and_then(|object| object.get("contractVersion"));
    match safe_integer(version) {
        Some(version) if version >= 1.0 => (version > 1.0).then_some("too_new"),
        _ => Some("too_old"),
    }
}

/// `version --json` as read by `bundleVersion` (host-preparation.ts:277-286)
/// and `state()` (host-maintenance.ts:171-182). Returns
/// `{"contract": "ok"|"too_old"|"too_new", "installed_release_id": string|null}`.
pub fn version_report(value: &Value) -> Value {
    let release = value.get("release_id").and_then(Value::as_str);
    json!({
        "contract": helper_contract_failure(value).unwrap_or("ok"),
        "installed_release_id": release,
    })
}

/// `AgentVMMCommandAdapter.observe` (runtime-adapter.ts:138-203): the
/// observation Main derives from `status [--registration-id ID]`.
pub fn project_status(value: &Value, registration_id: Option<&str>) -> Value {
    let empty = Map::new();
    let status = value.as_object().unwrap_or(&empty);
    let field = |key: &str| status.get(key);
    let complete = status.contains_key("hostReadable") && status.contains_key("hostHealthy");
    let host_ready = truthy(field("hostLoaded")) && complete && truthy(field("hostReadable")) && truthy(field("hostHealthy"));
    let read = field("registrationRead").and_then(Value::as_str);
    let registration = registration_id.map(|_| match read {
        Some("present") | Some("absent") | Some("unreadable") => read.unwrap_or_default(),
        _ => "unreadable",
    });
    let registration_state = field("registrationState")
        .and_then(Value::as_str)
        .filter(|state| matches!(*state, "enabled" | "draining" | "revoked"));

    let mut observed = Map::new();
    if let Some(side) = helper_contract_failure(value) {
        observed.insert("helperContractFailure".into(), json!(side));
    }
    if registration_id.is_some() {
        if let Some(protocol) = remote_protocol(field("remoteProtocol")) {
            observed.insert("remoteProtocol".into(), protocol);
        }
    }
    if let Some(failure) = host_start_failure(field("hostStartFailure")) {
        observed.insert("hostStartFailure".into(), failure);
    }
    if let Some(registration) = registration {
        observed.insert("registration".into(), json!(registration));
    }
    if let (Some(_), Some(state)) = (registration_id, registration_state) {
        observed.insert("registrationState".into(), json!(state));
    }
    if registration_id.is_some() && !truthy(field("registrationRead")) {
        observed.insert("reason".into(), json!("capability_missing"));
    }
    let connector = match registration {
        Some("absent") => "absent",
        Some("unreadable") => "degraded",
        _ if truthy(field("connectorLoaded")) => "ready",
        _ if truthy(field("connectorInstalled")) => "stopped",
        _ => "absent",
    };
    let host = if host_ready {
        "ready"
    } else if complete && truthy(field("hostLoaded")) {
        "degraded"
    } else if truthy(field("hostInstalled")) {
        "stopped"
    } else {
        "absent"
    };
    observed.insert("connector".into(), json!(connector));
    observed.insert("host".into(), json!(host));
    observed.insert(
        "readability".into(),
        json!(if registration == Some("unreadable") { "unreadable" } else { "readable" }),
    );
    Value::Object(observed)
}

/// `readRemoteProtocol` (runtime-adapter.ts:417-441).
fn remote_protocol(value: Option<&Value>) -> Option<Value> {
    let refusal = value?.as_object()?;
    let host_protocol = refusal.get("hostProtocol")?.as_str().filter(|text| text.len() <= 128)?;
    let minimum = safe_integer(refusal.get("minProtocol")).filter(|number| *number >= 1.0)?;
    let maximum = safe_integer(refusal.get("maxProtocol")).filter(|number| *number >= minimum)?;
    let side = refusal.get("hostSide")?.as_str().filter(|side| matches!(*side, "too_old" | "too_new"))?;
    Some(json!({"hostProtocol": host_protocol, "minProtocol": minimum as u64, "maxProtocol": maximum as u64, "hostSide": side}))
}

/// `readHostStartFailure` (runtime-adapter.ts:377-395).
fn host_start_failure(value: Option<&Value>) -> Option<Value> {
    let failure = value?.as_object()?;
    let code = failure.get("code")?.as_str()?;
    if !matches!(code, "STATE_SCHEMA_INCOMPATIBLE" | "STATE_MIGRATION_FAILED") {
        return None;
    }
    let mut result = Map::new();
    result.insert("code".into(), json!(code));
    if let Some(schema) = safe_integer(failure.get("dataSchema")) {
        result.insert("dataSchema".into(), json!(schema as i64));
    }
    Some(Value::Object(result))
}

/// `phaseOfStage` (host-maintenance.ts:531-543).
pub fn phase_of_stage(stage: &str) -> &'static str {
    match stage {
        "stop" => "stopping",
        "reset" => "removing",
        "replace" | "restore" => "installing",
        "settled" => "checking",
        _ => "preparing",
    }
}

/// `maintenance-status` as read by `LocalHostMaintenance.receipt` and projected
/// by `project` (host-maintenance.ts:141-158,225-249; host-maintenance-protocol.ts:26-48).
///
/// `Ok(None)`: no software-maintenance receipt. `Ok(Some(operation))`: the
/// projected operation. `Err(code)`: `another_request`, `contract_too_old`,
/// `contract_too_new` or `schema`.
pub fn maintenance_status(value: &Value, intent_request_id: Option<&str>) -> Result<Option<Value>, &'static str> {
    if value.is_null() {
        return Ok(None);
    }
    // `output.action` on a non-object is undefined; the receipt is then ignored.
    let action = value.get("action").and_then(Value::as_str);
    if !matches!(action, Some("update") | Some("reinstall") | Some("uninstall")) {
        return Ok(None);
    }
    match helper_contract_failure(value) {
        Some("too_new") => return Err("contract_too_new"),
        Some(_) => return Err("contract_too_old"),
        None => {}
    }
    let receipt = native_receipt(value).map_err(|_| "schema")?;
    if intent_request_id.is_some_and(|expected| expected != receipt.request_id) {
        return Err("another_request");
    }
    let phase = phase_of_stage(&receipt.stage);
    let mut operation = Map::new();
    operation.insert(
        "phase".into(),
        json!(match receipt.outcome.as_str() {
            "succeeded" => "completed",
            "failed" => "failed",
            _ => phase,
        }),
    );
    operation.insert(
        "outcome".into(),
        json!(if receipt.outcome == "superseded" { "failed" } else { receipt.outcome.as_str() }),
    );
    if receipt.outcome == "failed" {
        operation.insert("failedPhase".into(), json!(phase));
    }
    if let Some(code) = receipt.failure_code.filter(|code| matches!(code.as_str(), "host_start_failed" | "target_older_than_data")) {
        operation.insert("failureCode".into(), json!(code));
    }
    Ok(Some(Value::Object(operation)))
}

struct NativeReceipt {
    request_id: String,
    stage: String,
    outcome: String,
    failure_code: Option<String>,
}

/// `nativeReceiptSchema` (host-maintenance-protocol.ts:26-41).
fn native_receipt(value: &Value) -> Result<NativeReceipt, String> {
    let receipt = object(value, "receipt")?;
    if let Some(version) = receipt.get("contractVersion") {
        safe_integer(Some(version)).ok_or("contractVersion must be an integer")?;
    }
    let version = receipt.get("version").and_then(|version| safe_integer(Some(version)));
    if !version.is_some_and(|version| version >= 1.0) {
        return Err("version must be an integer >= 1".into());
    }
    let request_id = required_string(receipt, "requestId")?;
    let action = required_string(receipt, "action")?;
    if !matches!(action.as_str(), "update" | "reinstall" | "uninstall") {
        return Err("action".into());
    }
    let policy = required_string(receipt, "dataPolicy")?;
    if !matches!(policy.as_str(), "preserve" | "reset") {
        return Err("dataPolicy".into());
    }
    for key in ["targetApp", "sourceApp", "targetReleaseId", "failureMessage"] {
        optional_string(receipt, key)?;
    }
    let stage = required_string(receipt, "stage")?;
    let outcome = required_string(receipt, "outcome")?;
    if !matches!(outcome.as_str(), "pending" | "failed" | "succeeded" | "superseded") {
        return Err("outcome".into());
    }
    let failure_code = optional_string(receipt, "failureCode")?;
    if failure_code.as_ref().is_some_and(|code| code.chars().count() > 128) {
        return Err("failureCode exceeds 128 characters".into());
    }
    match receipt.get("retryable") {
        None | Some(Value::Bool(_)) => {}
        Some(_) => return Err("retryable must be a boolean when present".into()),
    }
    Ok(NativeReceipt { request_id, stage, outcome, failure_code })
}

/// The receipt of a tracked mutation (`registration-claim`, `registration-recover`,
/// `registration-enable`) as accepted by `tracked` and `parseRequestReceipt`
/// (runtime-adapter.ts:264-295,342-375).
///
/// `Err(code)`: `unreadable`, `another_registration`, `another_action`,
/// `not_acknowledged`.
pub fn request_receipt(value: &Value, command: &str, request_id: &str, registration_id: &str) -> Result<Value, &'static str> {
    let receipt = parse_request_receipt(value)?.ok_or("another_registration")?;
    let text = |key: &str| receipt.get(key).and_then(Value::as_str);
    if text("targetRegistrationId") != Some(registration_id) {
        return Err("another_registration");
    }
    if text("kind") != Some(command) {
        return Err("another_action");
    }
    if text("requestId") != Some(request_id)
        && (command != "registration-recover" || text("correlationRequestId") != Some(request_id))
    {
        return Err("not_acknowledged");
    }
    let mut result = Map::new();
    result.insert("outcome".into(), receipt["outcome"].clone());
    result.insert("retryable".into(), receipt["retryable"].clone());
    if let Some(code) = text("failureCode") {
        result.insert("failureCode".into(), json!(code));
        // `rejectedClaimTicket` (index.ts:1147-1154).
        if command == "registration-claim" && text("outcome") == Some("failed") && matches!(code, "invalid_ticket" | "ticket_expired") {
            result.insert("rejectedClaimTicket".into(), json!(true));
        }
    }
    Ok(Value::Object(result))
}

/// `parseRequestReceipt` (runtime-adapter.ts:342-375). `Ok(None)` for `null`.
pub fn parse_request_receipt(value: &Value) -> Result<Option<Map<String, Value>>, &'static str> {
    if value.is_null() {
        return Ok(None);
    }
    let receipt = value.as_object().ok_or("unreadable")?;
    // `receipt.requestId ?? receipt.operationId`: only null/absent falls through.
    let canonical = match receipt.get("requestId") {
        None | Some(Value::Null) => receipt.get("operationId"),
        other => other,
    };
    let canonical = canonical.and_then(Value::as_str).filter(|id| !id.is_empty()).ok_or("unreadable")?;
    let kind = receipt.get("kind").and_then(Value::as_str).ok_or("unreadable")?;
    let outcome = receipt.get("outcome").and_then(Value::as_str).ok_or("unreadable")?;
    if !matches!(outcome, "pending" | "failed" | "succeeded" | "superseded") {
        return Err("unreadable");
    }
    let mut result = Map::new();
    result.insert("requestId".into(), json!(canonical));
    result.insert("kind".into(), json!(kind));
    result.insert("outcome".into(), json!(outcome));
    result.insert("retryable".into(), json!(receipt.get("retryable") == Some(&Value::Bool(true))));
    for key in ["correlationRequestId", "targetRegistrationId", "stage", "failureCode"] {
        if let Some(text) = receipt.get(key).and_then(Value::as_str) {
            result.insert(key.into(), json!(text));
        }
    }
    Ok(Some(result))
}

/// `readNativeCommandError` (command.ts:123-160): the structured error Main
/// reads from stdout of a command that exited non-zero.
pub fn native_command_error(stdout: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    let error = value.get("error");
    let code = error.and_then(|error| error.get("code")).and_then(Value::as_str);
    let reason = error.and_then(|error| error.get("reason")).and_then(Value::as_str);
    if let (Some(code), Some(reason)) = (code, reason) {
        let mut result = Map::new();
        result.insert("code".into(), json!(code));
        result.insert("reason".into(), json!(reason));
        if let Some(request) = error.and_then(|error| error.get("requestId")).and_then(Value::as_str) {
            result.insert("requestId".into(), json!(request));
        }
        return Some(Value::Object(result));
    }
    if let Some(code) = code {
        return Some(json!({"code": code, "reason": "operation_failed"}));
    }
    let code = value.get("failureCode").and_then(Value::as_str)?;
    let stage = value.get("failureStage").and_then(Value::as_str).unwrap_or("operation_failed");
    Some(json!({"code": code, "reason": stage}))
}

/// `environmentPage` (local-operator.ts:19-39).
pub fn local_environments(value: &Value) -> Result<(), String> {
    let page = object(value, "page")?;
    optional_string(page, "bootId")?;
    required_string(page, "collectedAt")?;
    optional_string(page, "nextCursor")?;
    let Some(environments) = page.get("environments") else { return Ok(()) };
    let environments = environments.as_array().ok_or("environments must be an array")?;
    if environments.len() > 32 {
        return Err("environments exceeds 32 entries".into());
    }
    for environment in environments {
        let row = object(environment, "environment")?;
        let id = required_string(row, "id")?;
        if id.is_empty() || id.chars().count() > 200 {
            return Err("id must have 1..200 characters".into());
        }
        if !matches!(required_string(row, "origin")?.as_str(), "local" | "remote") {
            return Err("origin must be local or remote".into());
        }
        if !matches!(required_string(row, "state")?.as_str(), "unknown" | "observed_active" | "retired" | "vm_stopped") {
            return Err("state is not a known local environment state".into());
        }
        optional_string(row, "registrationId")?;
        optional_string(row, "allocationId")?;
        optional_decimal(row, "allocationGeneration")?;
        required_decimal(row, "revision")?;
        optional_decimal(row, "privateDiskBytes")?;
        optional_string(row, "sampledAt")?;
    }
    Ok(())
}

/// `diskSummary` (local-operator.ts:40-68). Returns the `disk` Main keeps,
/// or `None` when `guestStateFs` is absent.
pub fn local_disk(value: &Value) -> Result<Option<Value>, String> {
    let summary = object(value, "summary")?;
    required_string(summary, "collectedAt")?;
    optional_string(summary, "bootId")?;
    let Some(state) = summary.get("guestStateFs") else { return Ok(None) };
    let state = object(state, "guestStateFs")?;
    let mut disk = Map::new();
    for key in ["usedBytes", "capacityBytes", "importReservationBytes"] {
        disk.insert(key.into(), json!(required_decimal(state, key)?));
    }
    let breakdown = ["imageRuntimeBytes", "buildCacheBytes", "privateBytes", "otherBytes"];
    let sampled = state.get("breakdownSampledAt").and_then(Value::as_str).filter(|text| datetime_with_offset(text));
    if let (true, Some(sampled)) = (breakdown.iter().all(|key| state.get(*key).is_some_and(decimal)), sampled) {
        for key in breakdown {
            disk.insert(key.into(), state[key].clone());
        }
        disk.insert("breakdownSampledAt".into(), json!(sampled));
    }
    Ok(Some(Value::Object(disk)))
}

/// `registrationPage` (local-operator.ts:154-174).
pub fn local_registrations(value: &Value) -> Result<(), String> {
    let page = object(value, "page")?;
    optional_string(page, "nextCursor")?;
    let Some(registrations) = page.get("registrations") else { return Ok(()) };
    let registrations = registrations.as_array().ok_or("registrations must be an array")?;
    if registrations.len() > 32 {
        return Err("registrations exceeds 32 entries".into());
    }
    for registration in registrations {
        let row = object(registration, "registration")?;
        let id = required_string(row, "id")?;
        if id.is_empty() || id.chars().count() > 200 {
            return Err("id must have 1..200 characters".into());
        }
        optional_decimal(row, "revision")?;
        for key in ["displayName", "endpoint", "state"] {
            optional_string(row, key)?;
        }
    }
    Ok(())
}

/// `planSchema` and the target check of `execute` (local-operator.ts:69-83,407-415).
pub fn local_plan(value: &Value, target_kind: &str, target_id: &str) -> Result<(), String> {
    let plan = object(value.get("plan").ok_or("plan is required")?, "plan")?;
    let kind = required_string(plan, "targetKind")?;
    if !matches!(kind.as_str(), "LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT" | "LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION") {
        return Err("targetKind".into());
    }
    let id = required_string(plan, "targetId")?;
    if id.is_empty() || id.chars().count() > 200 {
        return Err("targetId must have 1..200 characters".into());
    }
    let revision = required_decimal(plan, "revision")?;
    optional_string(plan, "namespaceId")?;
    optional_decimal(plan, "environmentCount")?;
    match plan.get("activityUnknown") {
        None | Some(Value::Bool(_)) => {}
        Some(_) => return Err("activityUnknown must be a boolean when present".into()),
    }
    if kind != target_kind || id != target_id || revision == "0" {
        return Err("Local deletion target changed.".into());
    }
    Ok(())
}

/// `receiptSchema` and `requireReceiptTarget` (local-operator.ts:84-97,513-519).
/// Returns the disposal outcome Main reports: `completed` or `pending`.
pub fn local_operation(value: &Value, plan_target_id: &str) -> Result<&'static str, String> {
    let operation = object(value.get("operation").ok_or("operation is required")?, "operation")?;
    for key in ["id", "requestId"] {
        if required_string(operation, key)?.is_empty() {
            return Err(format!("{key} must not be empty"));
        }
    }
    if required_string(operation, "kind")? != "local-disposal" {
        return Err("kind must be local-disposal".into());
    }
    let resource = object(operation.get("resource").ok_or("resource is required")?, "resource")?;
    if required_string(resource, "kind")? != "environment" {
        return Err("resource.kind must be environment".into());
    }
    let environment = required_string(resource, "environmentId")?;
    if environment.is_empty() {
        return Err("resource.environmentId must not be empty".into());
    }
    let state = optional_string(operation, "state")?;
    if environment != plan_target_id {
        return Err("Local deletion receipt belongs to another target.".into());
    }
    Ok(if state.as_deref() == Some("OPERATION_STATE_SUCCEEDED") { "completed" } else { "pending" })
}
