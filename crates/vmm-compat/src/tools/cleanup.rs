use super::*;
use vmm_core::CleanupKind;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    kind: String,
    plan_revision: Option<String>,
    #[serde(default)]
    confirm_reset: bool,
    #[serde(default)]
    confirm_irreversible: bool,
    #[serde(default)]
    confirmed_volume_ids: Vec<String>,
}

impl Calls<'_> {
    pub(super) async fn cleanup(&self, name: &str, args: Value) -> Result<Value, Status> {
        let input: Input = parse(args)?;
        let kind = match input.kind.as_str() {
            "reset" => CleanupKind::Reset,
            "discard" => CleanupKind::Discard,
            _ => return Err(Status::invalid_argument("invalid cleanup kind")),
        };
        let store = &self.runtime.core.store;
        let inventory = store
            .mcp_cleanup_inventory(&self.session, kind)
            .map_err(core_status)?;
        let plan = crate::operator::cleanup_plan(&self.session.scope_id, kind, inventory)?;
        if name == "cleanup_plan" {
            let impacts:Vec<_>=plan.impacts.into_iter().map(|i|{
                let resource=i.resource.unwrap_or_default();
                json!({"resource":{"kind":resource.kind,"id":resource.resource_id},"effect":i.effect,"irreversible":i.irreversible,"blocker":i.blocker})
            }).collect();
            return Ok(
                json!({"kind":input.kind,"revision":plan.revision.to_string(),"impacts":impacts}),
            );
        }
        let revision = input
            .plan_revision
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|r| *r != 0)
            .ok_or_else(|| Status::invalid_argument("invalid cleanup revision"))?;
        if revision != plan.revision {
            return Err(Status::aborted("cleanup plan is stale"));
        }
        let discard = kind == CleanupKind::Discard;
        if input.confirm_reset == discard
            || input.confirm_irreversible != discard
            || (!discard && !input.confirmed_volume_ids.is_empty())
        {
            return Err(Status::invalid_argument("confirmation does not match plan"));
        }
        let op = store
            .accept_mcp_cleanup(
                &self.session,
                kind,
                revision,
                &input.confirmed_volume_ids,
                &request_id(),
            )
            .await
            .map_err(core_status)?;
        self.runtime
            .core
            .drive_cleanup(&op.id)
            .await
            .map_err(core_status)?;
        Ok(json!({"kind":input.kind,"state":"succeeded"}))
    }
}
