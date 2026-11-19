//! The `operation` command reads the receipt of one request.

use crate::error::{Error, Kind, Result, err};
use crate::journal::{Journal, receipt_name};
use crate::{CONTRACT_VERSION, Context, Paths, legacy, valid_id};
use serde_json::{Map, Value};

type Report = Map<String, Value>;

/// The receipt of a request or operation identity, or `None` when the journal never accepted it.
fn read_by_id(context: &Context, paths: &Paths, id: &str) -> Result<Option<Report>> {
    let journal = match Journal::open(context, paths) {
        Ok(journal) => journal,
        Err(error) if error.not_found() => return Ok(None),
        Err(error) => return Err(error),
    };
    let state = journal.state()?;
    let indexed = state.operation_requests.get(id);
    let request_id = indexed.map_or(id, String::as_str);
    let receipt = match journal.receipt(request_id) {
        Ok(receipt) => receipt,
        Err(error) if error.not_found() && (indexed.is_some() || state.accepted(request_id)) => {
            return Err(err("accepted lifecycle receipt is unavailable; keep the owner journal for recovery"));
        }
        Err(error) if error.not_found() => return Ok(None),
        Err(error) => return Err(error),
    };
    if receipt.operation_id != id && receipt.request_id != id {
        return Err(err("lifecycle operation was not found"));
    }
    // A data reset fences old install requests. The fence outranks a receipt that still says retryable.
    if state.blocked(&receipt.request_id) {
        return Err(Error::new(Kind::RequestBlocked(receipt.request_id), "the local owner fenced this request; it cannot continue"));
    }
    let mut report: Report = journal.read(&receipt_name(&receipt.request_id))?;
    report.insert("operationId".into(), receipt.operation_id.into());
    report.insert("requestId".into(), receipt.request_id.into());
    report.insert("kind".into(), receipt.kind.into());
    report.insert("contractVersion".into(), CONTRACT_VERSION.into());
    Ok(Some(report))
}

/// Reads a request for one registration. Only a committed recovery correlation
/// lets the receipt of another request identity answer.
fn read_correlated(context: &Context, paths: &Paths, requested: &str, registration_id: &str) -> Result<Option<Report>> {
    if let Some(mut report) = read_by_id(context, paths, requested)? {
        if report.get("kind").and_then(Value::as_str) == Some("install-managed") {
            legacy::migrate(context, paths, requested, registration_id)?;
            report = read_by_id(context, paths, requested)?.ok_or_else(|| err("accepted lifecycle receipt is unavailable; keep the owner journal for recovery"))?;
        }
        if report.get("targetRegistrationId").and_then(Value::as_str) != Some(registration_id) {
            return Err(err("request target does not match registration"));
        }
        return Ok(Some(report));
    }
    let canonical = match Journal::open(context, paths) {
        Ok(journal) => journal.state()?.registration_request(registration_id).to_string(),
        Err(error) if error.not_found() => return Ok(None),
        Err(error) => return Err(error),
    };
    if canonical.is_empty() {
        return Ok(None);
    }
    let report = read_by_id(context, paths, &canonical)?.ok_or_else(|| err("accepted registration receipt is unavailable"))?;
    let field = |name: &str| report.get(name).and_then(Value::as_str);
    let recovered = field("correlationRequestId") == Some(requested) && field("kind") == Some("registration-recover") && field("targetRegistrationId") == Some(registration_id);
    Ok(recovered.then_some(report))
}

/// Resolves `operation --request-id ID [--registration-id ID]` or `--operation-id ID`.
pub fn operation(context: &Context, paths: &Paths, arguments: &[String]) -> Result<Value> {
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    let report = match arguments.as_slice() {
        ["--request-id", request, "--registration-id", registration] if valid_id(request) && valid_id(registration) => read_correlated(context, paths, request, registration)?,
        ["--operation-id" | "--request-id", id] if valid_id(id) => read_by_id(context, paths, id)?,
        _ => return Err(err("operation requires --operation-id ID")),
    };
    Ok(report.map_or(Value::Null, Value::Object))
}
