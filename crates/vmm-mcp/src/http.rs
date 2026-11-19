use crate::{Binding, TokenAuthority};
use axum::{
    Router,
    extract::{Extension, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
};
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, JsonObject,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
    },
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use tower::ServiceExt;

/// A tool call receives only the allocation selected by the verified Bearer middleware.
#[async_trait::async_trait]
pub trait ToolRuntime: Send + Sync + 'static {
    async fn call(
        &self,
        binding: &Binding,
        name: &str,
        arguments: JsonObject,
    ) -> Result<Value, ToolFailure>;
}

#[derive(serde::Serialize, Debug)]
pub struct ToolFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<Value>,
}

#[derive(Deserialize)]
struct Definition {
    timeout: u64,
    tool: Tool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Catalog {
    output_schema: JsonObject,
    tools: Vec<Definition>,
}

static TOOLS: LazyLock<BTreeMap<String, Definition>> = LazyLock::new(|| {
    let catalog: Catalog =
        serde_json::from_str(include_str!("catalog.json")).expect("bundled MCP catalog");
    let schema = Arc::new(catalog.output_schema);
    catalog
        .tools
        .into_iter()
        .map(|mut d| {
            d.tool.output_schema = Some(schema.clone());
            (d.tool.name.to_string(), d)
        })
        .collect()
});

struct Limits {
    calls: Semaphore,
    execs: Semaphore,
    mutations: Semaphore,
}

#[derive(Clone)]
struct Adapter {
    binding: Binding,
    runtime: Arc<dyn ToolRuntime>,
    limits: Arc<Limits>,
}

impl ServerHandler for Adapter {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("agent-vmm-mcp", "1")
                .with_website_url("https://github.com/AFK-surf/agent-vmm"),
        )
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(
            TOOLS.values().map(|d| d.tool.clone()).collect(),
        ))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        TOOLS.get(name).map(|d| d.tool.clone())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let definition = TOOLS
            .get(request.name.as_ref())
            .ok_or_else(|| ErrorData::invalid_params("unknown tool", None))?;
        let run = async {
            let class = if request.name == "container_exec" {
                Some(&self.limits.execs)
            } else if definition
                .tool
                .annotations
                .as_ref()
                .and_then(|a| a.read_only_hint)
                == Some(true)
            {
                None
            } else {
                Some(&self.limits.mutations)
            };
            let _class = match class {
                Some(s) => Some(s.acquire().await.expect("private semaphore")),
                None => None,
            };
            let _permit = self
                .limits
                .calls
                .acquire()
                .await
                .expect("private semaphore");
            self.runtime
                .call(
                    &self.binding,
                    &request.name,
                    request.arguments.unwrap_or_default(),
                )
                .await
        };
        let outcome = tokio::time::timeout(Duration::from_secs(definition.timeout), run).await;
        let error = match outcome {
            Ok(Ok(data)) => {
                let envelope = json!({"ok":true,"data":data});
                if envelope.to_string().len() <= 2 << 20 {
                    return Ok(CallToolResult::structured(envelope).into());
                }
                ToolFailure {
                    code: "RESOURCE_EXHAUSTED".into(),
                    message: "serialized tool result exceeds limit".into(),
                    retryable: false,
                    resource: None,
                }
            }
            Ok(Err(error)) => error,
            Err(_) => ToolFailure {
                code: "TIMEOUT".into(),
                message: "request timed out".into(),
                retryable: false,
                resource: None,
            },
        };
        Ok(CallToolResult::structured_error(json!({"ok":false,"error":error})).into())
    }
}

type Transport = StreamableHttpService<Adapter, LocalSessionManager>;

struct Frontend {
    runtime: Arc<dyn ToolRuntime>,
    sessions: Mutex<HashMap<String, (Instant, Transport)>>,
}

pub fn router(authority: TokenAuthority, runtime: Arc<dyn ToolRuntime>) -> Router {
    let state = Arc::new(Frontend {
        runtime,
        sessions: Mutex::default(),
    });
    authority.protect(Router::new().route("/mcp", any(dispatch)).with_state(state))
}

async fn dispatch(
    State(state): State<Arc<Frontend>>,
    Extension(binding): Extension<Binding>,
    request: Request,
) -> Response {
    let mut sessions = state.sessions.lock().await;
    let now = Instant::now();
    sessions.retain(|_, (seen, _)| now.duration_since(*seen) < Duration::from_secs(300));
    if request.headers().contains_key("mcp-session-id")
        && !sessions.contains_key(&binding.allocation_id)
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let entry = sessions
        .entry(binding.allocation_id.clone())
        .or_insert_with(|| {
            let adapter = Adapter {
                binding,
                runtime: state.runtime.clone(),
                limits: Arc::new(Limits {
                    calls: Semaphore::new(16),
                    execs: Semaphore::new(8),
                    mutations: Semaphore::new(4),
                }),
            };
            let config = StreamableHttpServerConfig::default().with_max_request_body_bytes(4 << 20);
            (
                now,
                StreamableHttpService::new(
                    move || Ok(adapter.clone()),
                    Arc::new(LocalSessionManager::default()),
                    config,
                ),
            )
        });
    entry.0 = now;
    let service = entry.1.clone();
    drop(sessions);
    match service.oneshot(request).await {
        Ok(response) => response.into_response(),
        Err(never) => match never {},
    }
}
