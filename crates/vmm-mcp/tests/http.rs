use reqwest::{Client, Response, StatusCode};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use vmm_mcp::{Binding, FileSigningKeys, TokenAuthority, ToolFailure, ToolRuntime, router};

struct ScopeReader;

#[async_trait::async_trait]
impl ToolRuntime for ScopeReader {
    async fn call(
        &self,
        binding: &Binding,
        name: &str,
        args: Map<String, Value>,
    ) -> Result<Value, ToolFailure> {
        assert_eq!(name, "environment_get");
        assert!(args.is_empty());
        Ok(json!({"scope":binding.allocation_id}))
    }
}

async fn rpc_result(response: Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    if let Ok(value) = serde_json::from_str(&body) {
        return value;
    }
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
        .find(|v| v.get("id").is_some())
        .expect("MCP response event")
}

#[tokio::test]
async fn initialized_session_only_dispatches_for_its_bearer_over_http() {
    let state = tempfile::tempdir().unwrap();
    let authority = TokenAuthority::new(Arc::new(FileSigningKeys::open(state.path()).unwrap()));
    let owner = authority.issue().unwrap();
    let other = authority.issue().unwrap();
    let app = router(authority.clone(), Arc::new(ScopeReader));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let rpc = |token: &str, method: &str, id: Option<u32>, params: Value, session: Option<&str>| {
        let mut body = json!({"jsonrpc":"2.0","method":method,"params":params});
        if let Some(id) = id {
            body["id"] = json!(id)
        }
        let mut req = client
            .post(&url)
            .bearer_auth(token)
            .header("accept", "application/json, text/event-stream")
            .json(&body);
        if let Some(s) = session {
            req = req
                .header("mcp-session-id", s)
                .header("mcp-protocol-version", "2025-03-26");
        }
        req.send()
    };
    let init = json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}});
    assert_eq!(
        rpc("invalid", "initialize", Some(1), init.clone(), None)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = rpc(&owner, "initialize", Some(1), init, None)
        .await
        .unwrap();
    let sid = response.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        rpc_result(response).await["result"]["serverInfo"]["name"],
        "agent-vmm-mcp"
    );
    let sid = Some(sid.as_str());
    assert_eq!(
        rpc(&owner, "notifications/initialized", None, json!({}), sid)
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    let list = rpc_result(
        rpc(&owner, "tools/list", Some(2), json!({}), sid)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 23);
    let args = json!({"name":"environment_get","arguments":{}});
    assert_eq!(
        rpc(&other, "tools/call", Some(3), args.clone(), sid)
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let called = rpc_result(
        rpc(&owner, "tools/call", Some(3), args.clone(), sid)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        called["result"]["structuredContent"]["data"]["scope"],
        authority.verify(&owner).unwrap().unwrap().allocation_id
    );
    authority.rotate().unwrap();
    assert_eq!(
        rpc(&owner, "tools/call", Some(4), args, sid)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    server.abort();
    let _ = server.await;
}
