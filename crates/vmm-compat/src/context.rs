use crate::error::{detailed_status, host_status, with_metadata};
use agent_vmm_proto::host::v1::ErrorReason;
use std::collections::HashMap;
use vmm_rpc::{Code, Request, Status};
use vmm_core::{CapacityDimension, Core, Error, Scope, Session};

/// Scope selected by the session interceptor, never by caller-provided resource IDs.
#[derive(Clone)]
pub struct RuntimeContext {
    pub(crate) core: Core,
    pub(crate) session: Session,
    pub(crate) scope: Scope,
    pub(crate) remote: Option<crate::session::RemoteIdentity>,
}

impl RuntimeContext {
    pub fn from_request<T>(core: &Core, req: &Request<T>) -> Result<Self, Status> {
        let session = req
            .extensions()
            .get::<Session>()
            .ok_or_else(|| Status::unauthenticated("runtime session required"))?
            .clone();
        let remote = req
            .extensions()
            .get::<crate::session::RemoteIdentity>()
            .cloned();
        let revoked = || {
            detailed_status(
                Code::Unauthenticated,
                "ALLOCATION_REVOKED",
                "remote allocation binding is no longer valid",
                HashMap::new(),
            )
        };
        let scope = if let Some(identity) = &remote {
            let allocation = core
                .store
                .operator_remote_allocation(&session.scope_id)
                .map_err(|e| match e {
                    Error::Missing(_) => revoked(),
                    other => core_status(other),
                })?;
            let scope = allocation.scope;
            if identity.session_id != session.id
                || identity.allocation_id != allocation.allocation_id
                || identity.generation != scope.generation
                || !matches!(&scope.origin, vmm_core::Origin::Remote { registration_id }
                    if registration_id == &identity.registration_id)
            {
                return Err(revoked());
            }
            scope
        } else {
            let scope = core.store.scope(&session.scope_id).map_err(core_status)?;
            if matches!(scope.origin, vmm_core::Origin::Remote { .. }) {
                return Err(revoked());
            }
            scope
        };
        Ok(Self {
            remote,
            core: core.clone(),
            session,
            scope,
        })
    }

    pub fn execution_authority(&self) -> Result<&str, Status> {
        match (&self.scope.origin, &self.remote) {
            (vmm_core::Origin::Local, _) => Ok(&self.session.id),
            (_, Some(remote)) => Ok(&remote.allocation_id),
            _ => Err(Status::unauthenticated(
                "remote allocation binding required",
            )),
        }
    }
    pub fn execution_owner(&self) -> &str {
        self.remote
            .as_ref()
            .map(|r| r.execution_owner_id.as_str())
            .filter(|owner| !owner.is_empty())
            .unwrap_or(&self.session.id)
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn session(&self) -> &Session {
        &self.session
    }
}

pub(crate) fn resource_id(id: &str) -> Result<(), Status> {
    if id.len() > 63
        || !id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(Status::invalid_argument("invalid resource ID"));
    }
    Ok(())
}

pub fn core_status(error: Error) -> Status {
    match error {
        Error::Missing("image reference" | "scope image reference" | "ready artifact") => {
            host_status(
                Code::NotFound,
                ErrorReason::ImageNotFound,
                "image not found",
            )
        }
        Error::Missing(resource) => Status::not_found(format!("{resource} not found")),
        Error::Invalid(field) => Status::invalid_argument(format!("invalid {field}")),
        Error::Conflict("request fingerprint") => host_status(
            Code::Aborted,
            ErrorReason::RequestIdConflict,
            "request ID reused with different input",
        ),
        Error::Conflict("volume identity") => Status::already_exists("volume already exists"),
        Error::Conflict(msg) => host_status(
            Code::FailedPrecondition,
            ErrorReason::LifecycleConflict,
            msg,
        ),
        Error::Draining => host_status(
            Code::FailedPrecondition,
            ErrorReason::VmAdmissionClosed,
            "admission is closed",
        ),
        Error::Capacity(dim) => {
            let resource = match dim {
                CapacityDimension::Pids => "pids",
                CapacityDimension::ComputeSlots => "compute_slots",
                CapacityDimension::StorageHeadroom => "storage_headroom",
            };
            with_metadata(
                Code::ResourceExhausted,
                ErrorReason::ResourceCapacityExhausted,
                "resource capacity exhausted",
                HashMap::from([
                    ("capacity_dimension".into(), dim.as_str().into()),
                    ("resource_dimension".into(), resource.into()),
                    ("stage".into(), "vm_capacity".into()),
                ]),
            )
        }
        Error::Fenced => host_status(
            Code::Aborted,
            ErrorReason::StaleContainerInstance,
            "stale container instance",
        ),
        Error::Interrupted => Status::unavailable("backend operation interrupted"),
        Error::Database(err) => {
            eprintln!(
                "agent-vmm: host_store_failure kind=database code={:?}",
                err.sqlite_error_code()
            );
            Status::internal("host store failure")
        }
        Error::Io(err) => {
            eprintln!(
                "agent-vmm: host_store_failure kind=io error_kind={:?} os={:?}",
                err.kind(),
                err.raw_os_error()
            );
            Status::internal("host store failure")
        }
        Error::Codec(err) => {
            eprintln!(
                "agent-vmm: host_store_failure kind=codec classify={:?}",
                err.classify()
            );
            Status::internal("host store failure")
        }
    }
}
