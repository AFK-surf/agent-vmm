use serde::{Deserialize, Serialize};
use vmm_core::{backend::*, *};

#[derive(Serialize, Deserialize)]
pub(super) struct Start {
    pub definition: SandboxDefinition,
    pub scope: Scope,
    pub image: ImageArtifact,
    pub volumes: Vec<Volume>,
    pub boot: BootIdentity,
    pub token: zeroize::Zeroizing<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
pub(super) enum Request {
    Install(Digest, String, u64),
    DiscardImage(Digest),
    Volume(Volume),
    DiscardVolume(String, String),
    Overlay(SandboxKey, RootOverlay),
    DiscardOverlay(SandboxKey, RootOverlay),
    Start(Box<Start>),
    Quiesce(BootIdentity),
    Shutdown(BootIdentity),
    Destroy(BootIdentity),
    List,
    Policy(BootIdentity, Scope),
    Bind(ServiceRoute),
    Revoke(String, i64),
    Exec(BootIdentity, Vec<String>),
    Process(BootIdentity, String),
    Workspace(BootIdentity, String, Option<Vec<u8>>),
    Logs(BootIdentity, String),
}

#[derive(Serialize, Deserialize)]
pub(super) enum Fault {
    Missing,
    Conflict,
    Fenced,
    Draining,
    Invalid,
    Slots,
    Pids,
    Storage,
    Interrupted,
    Io(String),
}
impl From<Error> for Fault {
    fn from(e: Error) -> Self {
        match e {
            Error::Missing(_) => Self::Missing,
            Error::Conflict(_) => Self::Conflict,
            Error::Fenced => Self::Fenced,
            Error::Draining => Self::Draining,
            Error::Invalid(_) => Self::Invalid,
            Error::Capacity(CapacityDimension::Pids) => Self::Pids,
            Error::Capacity(CapacityDimension::ComputeSlots) => Self::Slots,
            Error::Capacity(CapacityDimension::StorageHeadroom) => Self::Storage,
            Error::Interrupted => Self::Interrupted,
            other => Self::Io(other.to_string()),
        }
    }
}
impl From<Fault> for Error {
    fn from(f: Fault) -> Self {
        match f {
            Fault::Missing => Self::Missing("remote resource"),
            Fault::Conflict => Self::Conflict("remote resource"),
            Fault::Invalid => Self::Invalid("remote request"),
            Fault::Fenced => Self::Fenced,
            Fault::Draining => Self::Draining,
            Fault::Pids => Self::Capacity(CapacityDimension::Pids),
            Fault::Slots => Self::Capacity(CapacityDimension::ComputeSlots),
            Fault::Storage => Self::Capacity(CapacityDimension::StorageHeadroom),
            Fault::Interrupted => Self::Interrupted,
            Fault::Io(s) => std::io::Error::other(s).into(),
        }
    }
}

pub(super) async fn dispatch(backend: &dyn Dom0Backend, r: Request) -> Result<serde_json::Value> {
    use serde_json::to_value;
    let reply = match r {
        Request::Install(manifest_digest, staged_extent, physical_bytes) => to_value(
            backend
                .install_image_artifact(&ImageSource {
                    manifest_digest,
                    staged_extent,
                    physical_bytes,
                })
                .await?,
        ),
        Request::DiscardImage(d) => to_value(backend.discard_image_artifact(d).await?),
        Request::Volume(v) => {
            backend.allocate_volume(&v).await?;
            Ok(serde_json::Value::Null)
        }
        Request::DiscardVolume(s, v) => {
            backend.discard_volume(&s, &v).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Overlay(k, o) => {
            backend.allocate_overlay(&k, &o).await?;
            Ok(serde_json::Value::Null)
        }
        Request::DiscardOverlay(k, o) => {
            backend.discard_overlay(&k, &o).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Start(s) => to_value(
            backend
                .start_vm(Launch {
                    definition: &s.definition,
                    scope: &s.scope,
                    image: &s.image,
                    volumes: &s.volumes,
                    boot: &s.boot,
                    boot_token: &s.token,
                })
                .await?,
        ),
        Request::Quiesce(b) => {
            backend.quiesce_vm(&b).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Shutdown(b) => {
            backend.shutdown_vm(&b).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Destroy(b) => {
            backend.destroy_vm(&b).await?;
            Ok(serde_json::Value::Null)
        }
        Request::List => to_value(
            backend
                .list_live_vms()
                .await?
                .into_iter()
                .map(|v| (v.incarnation, v.quiesced))
                .collect::<Vec<_>>(),
        ),
        Request::Policy(b, s) => {
            backend.set_network_policy(&b, &s).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Bind(r) => {
            backend.bind_route(&r).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Revoke(r, v) => {
            backend.revoke_route(&r, v).await?;
            Ok(serde_json::Value::Null)
        }
        Request::Exec(b, a) => to_value(backend.exec(&b, &a).await?),
        Request::Process(b, id) => to_value(match backend.process(&b, &id).await? {
            ProcessState::Running => None,
            ProcessState::Exited(code) => Some(code),
        }),
        Request::Workspace(b, path, bytes) => to_value(
            backend
                .workspace(
                    &b,
                    match bytes {
                        Some(bytes) => WorkspaceRequest::Write { path, bytes },
                        None => WorkspaceRequest::Read(path),
                    },
                )
                .await?,
        ),
        Request::Logs(b, p) => to_value(backend.logs(&b, &p).await?),
    };
    Ok(reply?)
}
