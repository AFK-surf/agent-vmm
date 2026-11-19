//! Requests of the retired `install-managed` command. The old owner wrote a
//! plan before it applied a Registration, and a version 1 completion when it
//! succeeded. Either file proves identity only: a recovery still needs a
//! current authorization and the original Host root.

use crate::error::{Error, Kind, Result, err};
use crate::journal::{self, Journal, PLAN_LIMIT, receipt_name};
use crate::{Context, Paths, valid_id};
use serde_json::Value;

pub fn registration_target(context: &Context, paths: &Paths, request_id: &str) -> Result<String> {
    let journal = Journal::open(context, paths)?;
    let completion = journal.dir().child("install-completions").and_then(|directory| directory.read(&format!("{request_id}.json"), 4096));
    match completion {
        Ok(data) => {
            let completed: Value = serde_json::from_slice(&data).map_err(|_| err("legacy completion identity is unreadable"))?;
            let registration = completed["registration_id"].as_str().unwrap_or_default();
            if completed["version"] != 1 || completed["operation_id"] != request_id || !valid_id(registration) {
                return Err(err("legacy completion identity is unreadable"));
            }
            return Ok(registration.to_string());
        }
        Err(error) if error.not_found() => {}
        Err(error) => return Err(error),
    }
    let data = journal.plan_dir(request_id, false)?.read("plan.json", PLAN_LIMIT)?;
    let plan: Value = serde_json::from_slice(&data).map_err(|_| err("legacy plan identity is unreadable"))?;
    let material = &plan["material"];
    let registration = material["registration_id"].as_str().unwrap_or_default();
    if plan["version"] != 1 || plan["operation_id"] != request_id || material["version"] != 1 || material["operation_id"] != request_id || !valid_id(registration) {
        return Err(err("legacy plan identity is unreadable"));
    }
    Ok(registration.to_string())
}

/// Binds an unbound historical receipt to its Registration once. It never
/// advances the active or the latest request.
pub fn migrate(context: &Context, paths: &Paths, request_id: &str, registration_id: &str) -> Result<()> {
    {
        let journal = Journal::open(context, paths)?;
        let state = journal.state()?;
        let receipt = journal.receipt(request_id)?;
        if receipt.target_registration_id == registration_id && !state.registration_request(registration_id).is_empty() {
            return Ok(());
        }
    }
    let _lock = journal::lock(context, paths)?;
    let journal = Journal::open(context, paths)?;
    let mut state = journal.state()?;
    if state.blocked(request_id) {
        return Err(Error::new(Kind::RequestBlocked(request_id.into()), "the local owner fenced this request; it cannot continue"));
    }
    let receipt = journal.receipt(request_id)?;
    if receipt.kind != "install-managed" || !receipt.target_registration_id.is_empty() && receipt.target_registration_id != registration_id {
        return Err(err("legacy request identity changed"));
    }
    if state.removed(registration_id) || !state.superseded_by(request_id).is_empty() || !receipt.superseded_by_request_id.is_empty() || receipt.outcome == "superseded" {
        return Err(err("legacy request was fenced"));
    }
    let owner = state.registration_request(registration_id);
    if !owner.is_empty() && owner != request_id {
        return Err(err("a newer request owns this Registration"));
    }
    if registration_target(context, paths, request_id)? != registration_id {
        return Err(err("legacy proof belongs to another Registration"));
    }
    if receipt.target_registration_id.is_empty() {
        let name = receipt_name(request_id);
        let mut raw: serde_json::Map<String, Value> = journal.read(&name)?;
        raw.insert("targetRegistrationId".into(), registration_id.into());
        journal.write(&name, &raw)?;
    }
    if state.registration_request(registration_id).is_empty() {
        state.registration_requests.insert(registration_id.into(), request_id.into());
        journal.write_state(&state)?;
    }
    Ok(())
}
