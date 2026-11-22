//! In-process MCP tools use the environment selected by the verified bearer.
mod cleanup;
mod output;
mod service;
mod workload;
use crate::{RuntimeContext, error::core_status, runtime::RuntimeService, session::RemoteIdentity};
use agent_vmm_proto::host::v1::{self as h, agent_runtime_service_server::AgentRuntimeService};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_stream::StreamExt;
use vmm_rpc::{Request, Status};
use vmm_core::{
    Channel, EgressMode, Limits, Origin, RemoteScopePolicy, Scope, ScopeState, Session,
};
use vmm_mcp::{Binding, ToolFailure, ToolRuntime};
use zeroize::Zeroizing;

const INSTALLATION: &str = "agent-vmm-mcp";

pub struct NativeTools {
    runtime: Arc<RuntimeService>,
    channel: Channel,
    limits: Limits,
    sessions: Mutex<HashMap<String, CachedSession>>,
}
struct CachedSession {
    used: Instant,
    id: String,
    token: Zeroizing<[u8; 16]>,
}

impl NativeTools {
    pub fn open(runtime: Arc<RuntimeService>, available_disk: u64) -> vmm_core::Result<Self> {
        let store = &runtime.core.store;
        let installation = match store.appliance(INSTALLATION)? {
            Some(installation) => installation,
            None => store.install_appliance(INSTALLATION, &uuid::Uuid::new_v4().to_string())?,
        };
        let epoch = store
            .registration(&installation.registration_id)?
            .connection_epoch
            .checked_add(1)
            .ok_or(vmm_core::Error::Conflict(
                "MCP registration epoch exhausted",
            ))?;
        let channel = store.connect(&installation.registration_id, epoch)?;
        Ok(Self {
            runtime,
            channel,
            // Per-environment limits from the bundled controller manifest.
            limits: Limits {
                pids_max: 1024,
                disk_quota_bytes: available_disk.min(16 << 30),
                vcpus: 1,
                boot_ram_mb: 2048,
                max_ram_mb: 2048,
            },
            sessions: Mutex::default(),
        })
    }

    pub fn registration_id(&self) -> &str {
        &self.channel.registration_id
    }

    fn session(&self, binding: &Binding) -> Result<Calls<'_>, Status> {
        let id = &binding.allocation_id;
        let core = &self.runtime.core;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| Status::internal("MCP sessions unavailable"))?;
        sessions.retain(|_, s| s.used.elapsed() < Duration::from_secs(600));
        let session = match sessions.entry(id.clone()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let cached = entry.get_mut();
                let session = core
                    .store
                    .authenticate_session(&cached.id, cached.token.as_slice())
                    .map_err(core_status)?;
                cached.used = Instant::now();
                session
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let scope = Scope {
                    id: String::new(),
                    generation: 1,
                    revision: 1,
                    state: ScopeState::Active,
                    origin: Origin::Remote {
                        registration_id: self.channel.registration_id.clone(),
                    },
                    limits: self.limits.clone(),
                    egress_mode: EgressMode::PublicInternet,
                    host_socket_rules: Vec::new(),
                };
                let policy = RemoteScopePolicy {
                    revision: 1,
                    max_pids: self.limits.pids_max,
                    max_disk_quota_bytes: self.limits.disk_quota_bytes,
                    max_egress_mode: scope.egress_mode.clone(),
                };
                core.store
                    .ensure_remote_scope(&self.channel, id, "MCP client", scope, &policy)
                    .map_err(core_status)?;
                let (session, token) = core
                    .store
                    .issue_remote_session(&self.channel, id, 1)
                    .map_err(core_status)?;
                entry.insert(CachedSession {
                    used: Instant::now(),
                    id: session.id.clone(),
                    token: Zeroizing::new(token),
                });
                session
            }
        };
        let identity = RemoteIdentity {
            session_id: session.id.clone(),
            registration_id: self.channel.registration_id.clone(),
            allocation_id: id.clone(),
            generation: 1,
            execution_owner_id: format!("appliance:{id}"),
        };
        Ok(Calls {
            runtime: &self.runtime,
            session,
            identity,
        })
    }
}

struct Calls<'a> {
    runtime: &'a RuntimeService,
    session: Session,
    identity: RemoteIdentity,
}
impl Calls<'_> {
    fn request<T>(&self, message: T) -> Result<Request<T>, Status> {
        if !self
            .runtime
            .core
            .store
            .session_is_current(&self.session)
            .map_err(core_status)?
        {
            return Err(Status::unauthenticated("MCP session expired"));
        }
        let mut request = Request::new(message);
        request.extensions_mut().insert(self.session.clone());
        request.extensions_mut().insert(self.identity.clone());
        Ok(request)
    }
    async fn get(&self, id: &str) -> Result<h::Container, Status> {
        let request = self.request(h::GetContainerRequest {
            container_id: id.into(),
            expected_configuration: None,
        })?;
        self.runtime
            .get_container(request)
            .await?
            .into_inner()
            .container
            .ok_or_else(missing)
    }
    async fn dispatch(&self, name: &str, args: Value) -> Result<Value, Status> {
        match name {
            "environment_get" => {
                let request = self.request(h::GetEnvironmentRequest {})?;
                let env = self
                    .runtime
                    .get_environment(request)
                    .await?
                    .into_inner()
                    .environment
                    .ok_or_else(missing)?;
                Ok(output::environment(env))
            }
            "container_list" => {
                let page: Page = parse(args)?;
                let request = self.request(h::ListContainersRequest {})?;
                let items = self
                    .runtime
                    .list_containers(request)
                    .await?
                    .into_inner()
                    .containers
                    .into_iter()
                    .map(output::container)
                    .filter(|v| page.state.is_empty() || v["state"] == page.state)
                    .collect();
                page.slice(items, "id")
            }
            "container_get" => Ok(output::container(
                self.get(&parse::<ContainerId>(args)?.container_id).await?,
            )),
            "image_list" => {
                let page: Page = parse(args)?;
                let items = self
                    .runtime
                    .list_images(self.request(h::ListImagesRequest::default())?)
                    .await?
                    .into_inner()
                    .images
                    .into_iter()
                    .map(output::image)
                    .collect();
                page.slice(items, "reference")
            }
            "volume_list" => {
                let page: Page = parse(args)?;
                let items = self
                    .runtime
                    .list_volumes(self.request(h::ListVolumesRequest {})?)
                    .await?
                    .into_inner()
                    .volumes
                    .into_iter()
                    .map(output::volume)
                    .collect();
                page.slice(items, "id")
            }
            "volume_create" => {
                let input: VolumeInput = parse(args)?;
                let response = self
                    .runtime
                    .create_volume(self.request(h::CreateVolumeRequest {
                        request_id: request_id(),
                        volume_id: input.volume_id,
                        owner_uid: input.owner_uid,
                        owner_gid: input.owner_gid,
                        mode: input.mode,
                    })?)
                    .await?;
                Ok(output::volume(
                    response.into_inner().volume.ok_or_else(missing)?,
                ))
            }
            "volume_delete" => {
                let input: VolumeInput = parse(args)?;
                self.runtime
                    .delete_volume(self.request(h::DeleteVolumeRequest {
                        request_id: request_id(),
                        volume_id: input.volume_id,
                    })?)
                    .await?;
                Ok(json!({}))
            }
            "image_pull" | "image_delete" => self.image_tool(name, parse(args)?).await,
            "cleanup_execute" | "cleanup_plan" => self.cleanup(name, args).await,
            "mesh_service_list" | "service_export" | "service_import" | "service_route_revoke" => {
                self.service(name, args).await
            }
            _ => self.workload(name, args).await,
        }
    }
    async fn image_tool(&self, name: &str, input: Reference) -> Result<Value, Status> {
        if name == "image_delete" {
            let images = self
                .runtime
                .list_images(self.request(h::ListImagesRequest::default())?)
                .await?
                .into_inner()
                .images;
            let image = images
                .into_iter()
                .find(|i| i.reference == input.reference)
                .ok_or_else(|| Status::not_found("image not found"))?;
            self.runtime
                .delete_image(self.request(h::DeleteImageRequest {
                    request_id: request_id(),
                    reference: image.reference,
                    expected_generation_id: image.generation_id,
                    expected_digest: image.digest,
                    ..Default::default()
                })?)
                .await?;
            return Ok(json!({}));
        }
        let mut updates = self
            .runtime
            .pull_image(self.request(h::PullImageRequest {
                request_id: request_id(),
                reference: input.reference.clone(),
            })?)
            .await?
            .into_inner();
        let mut last = None;
        while let Some(update) = updates.next().await {
            last = Some(update?);
        }
        let result = last.ok_or_else(missing)?;
        Ok(
            json!({"requested_reference":input.reference,"reference":result.canonical_reference,"logical_bytes":result.logical_bytes,"status":result.status,"digest":result.digest.unwrap_or_default()}),
        )
    }
}

#[async_trait::async_trait]
impl ToolRuntime for NativeTools {
    async fn call(
        &self,
        binding: &Binding,
        name: &str,
        arguments: Map<String, Value>,
    ) -> Result<Value, ToolFailure> {
        let calls = self.session(binding).map_err(output::failure)?;
        calls
            .dispatch(name, Value::Object(arguments))
            .await
            .map_err(output::failure)
    }
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T, Status> {
    serde_json::from_value(value).map_err(|e| Status::invalid_argument(e.to_string()))
}
fn missing() -> Status {
    Status::data_loss("runtime response is incomplete")
}
fn request_id() -> String {
    format!("mcp-{}", uuid::Uuid::new_v4().simple())
}
#[derive(Deserialize)]
struct Reference {
    reference: String,
}
#[derive(Deserialize)]
struct ContainerId {
    container_id: String,
}
#[derive(Deserialize)]
struct VolumeInput {
    volume_id: String,
    owner_uid: Option<u32>,
    owner_gid: Option<u32>,
    mode: Option<u32>,
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct Page {
    limit: Option<usize>,
    cursor: String,
    state: String,
}
impl Page {
    fn slice(&self, mut items: Vec<Value>, key: &str) -> Result<Value, Status> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) || self.cursor.len() > 1024 {
            return Err(Status::invalid_argument("invalid pagination"));
        }
        let start = if self.cursor.is_empty() {
            0
        } else {
            self.cursor
                .parse::<usize>()
                .map_err(|_| Status::invalid_argument("invalid cursor"))?
        };
        if start > items.len() {
            return Err(Status::invalid_argument("invalid cursor"));
        }
        items.sort_by(|a, b| a[key].as_str().cmp(&b[key].as_str()));
        let end = (start + limit).min(items.len());
        let mut page = json!({"items":&items[start..end]});
        if end < items.len() {
            page["next_cursor"] = end.to_string().into();
        }
        Ok(page)
    }
}
