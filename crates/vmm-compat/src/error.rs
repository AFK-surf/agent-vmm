use agent_vmm_proto::{google::rpc, host::v1::ErrorReason};
use prost::Message;
use prost_types::Any;
use std::collections::HashMap;
use tonic::{Code, Status};
use tonic_types::pb::ErrorInfo;

pub const DOMAIN: &str = "agent-vmm";

pub fn host_status(code: Code, reason: ErrorReason, message: &str) -> Status {
    with_metadata(code, reason, message, HashMap::new())
}

pub fn with_metadata(
    code: Code,
    reason: ErrorReason,
    message: &str,
    metadata: HashMap<String, String>,
) -> Status {
    let name = reason.as_str_name().trim_start_matches("ERROR_REASON_");
    detailed_status(code, name, message, metadata)
}

pub(crate) fn detailed_status(
    code: Code,
    reason: &str,
    message: &str,
    metadata: HashMap<String, String>,
) -> Status {
    let info = ErrorInfo {
        reason: reason.into(),
        domain: DOMAIN.into(),
        metadata,
    };
    let details = rpc::Status {
        code: code as i32,
        message: message.into(),
        details: vec![Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
            value: info.encode_to_vec(),
        }],
    };
    Status::with_details(code, message, details.encode_to_vec().into())
}

pub(crate) fn projection_failed() -> Status {
    let code = Code::Unavailable;
    let message = "the Host socket was saved but the egress policy was not published";
    let info = ErrorInfo {
        domain: DOMAIN.into(),
        reason: "POLICY_PROJECTION_FAILED".into(),
        metadata: Default::default(),
    };
    let retry = tonic_types::pb::RetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
    };
    let details = rpc::Status {
        code: code as i32,
        message: message.into(),
        details: vec![
            Any {
                type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
                value: info.encode_to_vec(),
            },
            Any {
                type_url: "type.googleapis.com/google.rpc.RetryInfo".into(),
                value: retry.encode_to_vec(),
            },
        ],
    };
    Status::with_details(code, message, details.encode_to_vec().into())
}

/// Matches host.v1 InvalidField so clients can point to the offending input.
pub fn invalid_field(field: &str, description: &str) -> Status {
    use tonic_types::pb::{BadRequest, bad_request::FieldViolation};
    let violation = BadRequest {
        field_violations: vec![FieldViolation {
            field: field.into(),
            description: description.into(),
            ..Default::default()
        }],
    };
    let message = "request validation failed";
    let details = rpc::Status {
        code: Code::InvalidArgument as i32,
        message: message.into(),
        details: vec![Any {
            type_url: "type.googleapis.com/google.rpc.BadRequest".into(),
            value: violation.encode_to_vec(),
        }],
    };
    Status::with_details(
        Code::InvalidArgument,
        message,
        details.encode_to_vec().into(),
    )
}

/// The gateway interprets the byte counts as unsigned decimal strings.
pub fn import_headroom(available: u64, required: u64) -> Status {
    with_metadata(
        Code::ResourceExhausted,
        ErrorReason::ResourceCapacityExhausted,
        "insufficient import storage headroom",
        HashMap::from([
            ("resource_dimension".into(), "storage_headroom".into()),
            ("stage".into(), "import_admission".into()),
            ("available_bytes".into(), available.to_string()),
            ("required_bytes".into(), required.to_string()),
        ]),
    )
}

pub use crate::context::core_status;

pub fn client_status(e: vmm_guest_proto::client::ClientError) -> Status {
    use vmm_guest_proto::client::ClientError;
    match e {
        ClientError::Rpc(err) => guest_status(&err),
        ClientError::Protocol(_) => Status::data_loss("invalid guest protocol response"),
        ClientError::Io(err) if err.get_ref().is_some_and(|e| e.is::<vmm_core::Error>()) => {
            let error = err
                .into_inner()
                .unwrap()
                .downcast::<vmm_core::Error>()
                .unwrap();
            core_status(*error)
        }
        ClientError::Io(err) if err.kind() == std::io::ErrorKind::TimedOut => {
            Status::deadline_exceeded("guest connection timed out")
        }
        ClientError::Io(_) => Status::unavailable("guest connection interrupted"),
    }
}
pub fn guest_status(err: &vmm_guest_proto::rpc::RpcError) -> Status {
    use vmm_guest_proto::rpc::ErrorCode as G;
    let code = match err.code {
        G::Unauthenticated => Code::Unauthenticated,
        G::UnsupportedVersion | G::FailedPrecondition => {
            return host_status(
                Code::FailedPrecondition,
                ErrorReason::RuntimePreconditionFailed,
                &err.message,
            );
        }
        G::InvalidArgument => Code::InvalidArgument,
        G::NotFound => Code::NotFound,
        G::AlreadyExists => Code::AlreadyExists,
        G::PathEscape | G::PermissionDenied => Code::PermissionDenied,
        G::ResourceExhausted => Code::ResourceExhausted,
        G::DeadlineExceeded => Code::DeadlineExceeded,
        G::Unavailable => Code::Unavailable,
        G::Internal => Code::Internal,
    };
    Status::new(code, &err.message)
}

pub(crate) fn with_resource(status: Status, kind: &str, name: &str) -> Status {
    use agent_vmm_proto::google::rpc;
    use prost::Message;
    let Ok(mut details) = rpc::Status::decode(status.details()) else {
        return status;
    };
    if !details
        .details
        .iter()
        .any(|d| d.type_url == "type.googleapis.com/google.rpc.ErrorInfo")
    {
        return status;
    }
    let resource = tonic_types::pb::ResourceInfo {
        resource_type: kind.into(),
        resource_name: name.into(),
        ..Default::default()
    };
    details.details.push(prost_types::Any {
        type_url: "type.googleapis.com/google.rpc.ResourceInfo".into(),
        value: resource.encode_to_vec(),
    });
    Status::with_details(
        status.code(),
        status.message(),
        details.encode_to_vec().into(),
    )
}
