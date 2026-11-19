use super::*;
use prost::Message;
use vmm_rpc::Code;
use crate::details::{ErrorInfo, ResourceInfo};

pub(super) fn failure(status: Status) -> ToolFailure {
    let (code, message) = match status.code() {
        Code::Cancelled => ("CANCELED", "request was canceled"),
        Code::DeadlineExceeded => ("TIMEOUT", "request timed out"),
        Code::InvalidArgument => ("INVALID_ARGUMENT", status.message()),
        Code::NotFound => ("NOT_FOUND", "resource was not found"),
        Code::AlreadyExists => ("ALREADY_EXISTS", "resource already exists"),
        Code::FailedPrecondition => ("FAILED_PRECONDITION", "runtime precondition failed"),
        Code::PermissionDenied | Code::Unauthenticated => {
            ("PERMISSION_DENIED", "operation is not permitted")
        }
        Code::ResourceExhausted => ("RESOURCE_EXHAUSTED", "resource limit exceeded"),
        Code::Aborted => ("CONFLICT", "resource changed concurrently"),
        Code::Unavailable => ("UNAVAILABLE", "runtime is unavailable"),
        _ => ("INTERNAL", "runtime operation failed"),
    };
    let mut result = ToolFailure {
        code: code.into(),
        message: message.into(),
        retryable: status.code() == Code::Unavailable,
        resource: None,
    };
    if let Ok(wire) = agent_vmm_proto::google::rpc::Status::decode(status.details()) {
        for detail in wire.details {
            match detail.type_url.as_str() {
                "type.googleapis.com/google.rpc.ErrorInfo" => {
                    if let Ok(info) = ErrorInfo::decode(detail.value.as_slice())
                        && info.domain == crate::error::DOMAIN
                    {
                        result.code = info.reason;
                        result.message = status.message().into();
                    }
                }
                "type.googleapis.com/google.rpc.ResourceInfo" => {
                    if let Ok(info) = ResourceInfo::decode(detail.value.as_slice()) {
                        result.resource =
                            Some(json!({"kind":info.resource_type,"id":info.resource_name}));
                    }
                }
                "type.googleapis.com/google.rpc.RetryInfo" => result.retryable = true,
                _ => {}
            }
        }
    }
    result
}
fn usage(value: Option<h::Usage>) -> Value {
    let u = value.unwrap_or_default();
    json!({"cpu_nanos":u.cpu_nanos,"memory_bytes":u.memory_bytes,"pids":u.pids,"read_bytes":u.read_bytes,
        "write_bytes":u.write_bytes,"disk_bytes":u.disk_bytes,"network_rx_bytes":u.network_rx_bytes,
        "network_tx_bytes":u.network_tx_bytes,"stale":u.stale})
}
pub(super) fn environment(env: h::AgentEnvironment) -> Value {
    let mut limits = json!({});
    if let Some(l) = &env.limits {
        if let Some(p) = l.pids.filter(|v| *v != 0) {
            limits["pids"] = p.into();
        }
        if let Some(d) = l.disk_bytes.filter(|v| *v != 0) {
            limits["disk_bytes"] = d.into();
        }
    }
    json!({"origin":env.origin().as_str_name().trim_start_matches("ENVIRONMENT_ORIGIN_").to_ascii_lowercase(),
        "limits":limits,"egress_mode":env.egress_mode().as_str_name().trim_start_matches("EGRESS_MODE_").to_ascii_lowercase(),
        "health":env.health.unwrap_or_default().state().as_str_name().trim_start_matches("HEALTH_STATE_").to_ascii_lowercase(),
        "usage":usage(env.usage),"containers":env.containers.len(),"volumes":env.volumes.len()})
}
pub(super) fn volume(v: h::Volume) -> Value {
    let mut result = json!({"id":v.id,"usage":usage(v.usage),"limit_bytes":v.limit_bytes});
    if !v.mounted_by.is_empty() {
        result["mounted_by"] = json!(v.mounted_by);
    }
    result
}
pub(super) fn container(v: h::Container) -> Value {
    let mut result = json!({"id":v.id,"image":v.image,"state":v.state().as_str_name().trim_start_matches("CONTAINER_STATE_").to_ascii_lowercase(),
        "usage":usage(v.usage)});
    if !v.instance_id.is_empty() {
        result["instance_id"] = v.instance_id.into();
    }
    if let Some(code) = v.exit_code {
        result["exit_code"] = code.into();
    }
    if let Some(workdir) = v.workdir.filter(|v| !v.is_empty()) {
        result["workdir"] = workdir.into();
    }
    if let Some(user) = v.user.filter(|v| !v.is_empty()) {
        result["user"] = user.into();
    }
    if !v.argv.is_empty() {
        result["argv"] = json!(v.argv);
    }
    if !v.volumes.is_empty() {
        result["volumes"] = Value::Array(
            v.volumes
                .into_iter()
                .map(|m| {
                    let mut mount = json!({"destination":m.destination,"read_only":m.read_only});
                    match m.source {
                        Some(h::volume_mount::Source::OwnedVolume(id)) => {
                            mount["owned_volume"] = id.into()
                        }
                        Some(h::volume_mount::Source::HostShare(id)) => {
                            mount["host_share"] = id.into()
                        }
                        None => {}
                    }
                    mount
                })
                .collect(),
        );
    }
    result
}
pub(super) fn image(v: h::Image) -> Value {
    let mut result = json!({"reference":v.reference,"logical_bytes":v.logical_bytes,"status":""});
    if !v.digest.is_empty() {
        result["digest"] = v.digest.into();
    }
    result
}
