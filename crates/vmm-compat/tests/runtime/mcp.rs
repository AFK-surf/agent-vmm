use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use vmm_compat::{runtime::RuntimeService, tools::NativeTools};
use vmm_mcp::{FileSigningKeys, TokenAuthority};

struct McpClient {
    http: Client,
    url: String,
    token: String,
    sid: Option<String>,
}

impl McpClient {
    async fn rpc(&self, method: &str, id: Option<u32>, params: Value) -> Response {
        let mut body = json!({"jsonrpc":"2.0","method":method,"params":params});
        if let Some(id) = id {
            body["id"] = id.into();
        }
        let mut request = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(body.to_string());
        if let Some(sid) = &self.sid {
            request = request
                .header("mcp-session-id", sid)
                .header("mcp-protocol-version", "2025-03-26");
        }
        request.send().await.unwrap()
    }
    async fn open(url: &str, token: String) -> Self {
        let mut client = Self {
            http: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            url: url.into(),
            token,
            sid: None,
        };
        let init = json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"native-test","version":"1"}});
        let response = client.rpc("initialize", Some(1), init).await;
        assert_eq!(response.status(), StatusCode::OK);
        client.sid = Some(
            response.headers()["mcp-session-id"]
                .to_str()
                .unwrap()
                .into(),
        );
        assert_eq!(
            client
                .rpc("notifications/initialized", None, json!({}))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        client
    }
    async fn tool(&self, name: &str, args: Value) -> Value {
        let response = self
            .rpc("tools/call", Some(2), json!({"name":name,"arguments":args}))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.text().await.unwrap();
        let frame = serde_json::from_str::<Value>(&body)
            .ok()
            .or_else(|| {
                body.lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
                    .find(|v| v.get("id").is_some())
            })
            .unwrap();
        frame["result"]["structuredContent"].clone()
    }
}

pub async fn exercise(runtime: Arc<RuntimeService>, state: &Path) {
    let keys = state.join("native-mcp-keys");
    std::fs::create_dir(&keys).unwrap();
    let authority = TokenAuthority::new(Arc::new(FileSigningKeys::open(&keys).unwrap()));
    let tools = Arc::new(NativeTools::open(runtime, 1 << 20).unwrap());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(vmm_mcp::serve(listener, authority.clone(), tools, async {
        let _ = stopped.await;
    }));
    let owner = McpClient::open(&url, authority.issue().unwrap()).await;
    let other = McpClient::open(&url, authority.issue().unwrap()).await;
    assert_eq!(
        owner.tool("environment_get", json!({})).await["data"]["origin"],
        "remote"
    );
    let stale =
        owner.tool("cleanup_plan", json!({"kind":"discard"})).await["data"]["revision"].clone();
    let created = owner
        .tool("volume_create", json!({"volume_id":"documents"}))
        .await;
    assert_eq!(created["data"]["id"], "documents", "{created}");
    assert_eq!(
        other.tool("volume_list", json!({})).await["data"]["items"],
        json!([])
    );
    assert_eq!(
        owner.tool("volume_list", json!({})).await["data"]["items"][0]["id"],
        "documents"
    );
    let mut confirm = json!({"kind":"discard","confirm_irreversible":true,"plan_revision":stale});
    assert_eq!(
        owner.tool("cleanup_execute", confirm.clone()).await["ok"],
        false
    );
    confirm["plan_revision"] =
        owner.tool("cleanup_plan", json!({"kind":"discard"})).await["data"]["revision"].clone();
    assert_eq!(
        owner.tool("cleanup_execute", confirm.clone()).await["ok"],
        false
    );
    confirm["confirmed_volume_ids"] = json!(["documents"]);
    let discarded = owner.tool("cleanup_execute", confirm).await;
    assert_eq!(discarded["data"]["state"], "succeeded", "{discarded}");
    assert_eq!(owner.tool("volume_list", json!({})).await["ok"], false);
    assert_eq!(other.tool("volume_list", json!({})).await["ok"], true);
    authority.rotate().unwrap();
    assert_eq!(
        other.rpc("tools/list", Some(3), json!({})).await.status(),
        StatusCode::UNAUTHORIZED
    );
    drop(owner);
    drop(other);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
