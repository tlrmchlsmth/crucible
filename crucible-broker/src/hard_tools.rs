//! nm-hard-tools aggregation behind the privileged broker.
//!
//! The sandbox never receives an upstream URL or bearer token. At broker startup we discover the
//! configured services' bounded tool schemas, prefix their names, and add forwarding routes to the
//! broker's own tool router.

use anyhow::{Context, Result};
use rmcp::handler::server::router::tool::{ToolRoute, ToolRouter};
use rmcp::model::{CallToolResponse, CallToolResult, ContentBlock, Tool};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const PROTOCOL_VERSION: &str = "2026-07-28";
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum HardToolsError {
    #[error("nm-hard-tools service {name:?} has an empty bearer token")]
    EmptyToken { name: String },
    #[error("nm-hard-tools tool name collision: {name}")]
    ToolNameCollision { name: String },
    #[error("MCP response exceeds the {MAX_RESPONSE_BYTES}-byte limit")]
    ResponseTooLarge,
    #[error("MCP JSON-RPC response id did not match request {request_id}")]
    ResponseIdMismatch { request_id: u64 },
    #[error("upstream returned HTTP {status}: {error}")]
    Rpc {
        status: reqwest::StatusCode,
        error: Value,
    },
    #[error("upstream returned HTTP {status}")]
    Http { status: reqwest::StatusCode },
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceConfig {
    name: String,
    url: String,
    #[serde(default)]
    bearer_token_env: Option<String>,
}

#[derive(Clone)]
struct Service {
    name: String,
    url: String,
    bearer_token: Option<String>,
    client: reqwest::Client,
    request_id: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ProxiedTool {
    service: Arc<Service>,
    upstream_name: String,
    exposed: Tool,
}

#[derive(Clone, Default)]
pub struct HardTools {
    tools: Arc<Vec<ProxiedTool>>,
}

impl HardTools {
    pub fn empty() -> Self {
        Self::default()
    }

    pub async fn from_env() -> Result<Self> {
        let Some(raw) = std::env::var("BROKER_HARD_TOOLS")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(Self::empty());
        };
        Self::from_json(&raw).await
    }

    async fn from_json(raw: &str) -> Result<Self> {
        let configs: Vec<ServiceConfig> =
            serde_json::from_str(raw).context("parsing BROKER_HARD_TOOLS")?;
        let mut tools = Vec::new();
        for config in configs {
            let bearer_token = match &config.bearer_token_env {
                None => None,
                Some(name) => Some(
                    std::env::var(name)
                        .with_context(|| {
                            format!(
                                "nm-hard-tools service {:?} requires environment variable {name:?}",
                                config.name
                            )
                        })?
                        .trim()
                        .to_owned(),
                ),
            };
            let service = Arc::new(Service {
                name: config.name,
                url: config.url,
                bearer_token,
                client: reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .context("building nm-hard-tools HTTP client")?,
                request_id: Arc::new(AtomicU64::new(1)),
            });
            if service.bearer_token.as_deref() == Some("") {
                return Err(HardToolsError::EmptyToken {
                    name: service.name.clone(),
                }
                .into());
            }
            let discovered = service
                .list_tools()
                .await
                .with_context(|| format!("discovering nm-hard-tools service {:?}", service.name))?;
            for mut tool in discovered {
                let upstream_name = tool.name.to_string();
                tool.name = format!("hard_{}_{}", service.name, upstream_name).into();
                let description = tool.description.take().unwrap_or_default();
                tool.description =
                    Some(format!("nm-hard-tools service `{}`: {description}", service.name).into());
                tools.push(ProxiedTool {
                    service: service.clone(),
                    upstream_name,
                    exposed: tool,
                });
            }
        }
        Ok(Self {
            tools: Arc::new(tools),
        })
    }

    pub fn install<S>(&self, router: &mut ToolRouter<S>) -> Result<()>
    where
        S: Send + Sync + 'static,
    {
        for proxy in self.tools.iter() {
            if router.has_route(&proxy.exposed.name) {
                return Err(HardToolsError::ToolNameCollision {
                    name: proxy.exposed.name.to_string(),
                }
                .into());
            }
            let service = proxy.service.clone();
            let upstream_name = proxy.upstream_name.clone();
            router.add_route(ToolRoute::new_dyn(proxy.exposed.clone(), move |context| {
                let service = service.clone();
                let upstream_name = upstream_name.clone();
                let arguments = context.arguments.unwrap_or_default();
                Box::pin(async move {
                    let result = match service.call_tool(&upstream_name, arguments).await {
                        Ok(result) => result,
                        Err(error) => CallToolResult::error(vec![ContentBlock::text(format!(
                            "nm-hard-tools service {:?} failed: {error:#}",
                            service.name
                        ))]),
                    };
                    Ok(CallToolResponse::Complete(result))
                })
            }));
        }
        Ok(())
    }
}

impl Service {
    async fn list_tools(&self) -> Result<Vec<Tool>> {
        let result = self.request("tools/list", json!({})).await?;
        serde_json::from_value(
            result
                .get("tools")
                .cloned()
                .context("tools/list result omitted tools")?,
        )
        .context("decoding nm-hard-tools tool definitions")
    }

    async fn call_tool(&self, name: &str, arguments: Map<String, Value>) -> Result<CallToolResult> {
        let result = self
            .request("tools/call", json!({"name": name, "arguments": arguments}))
            .await?;
        serde_json::from_value(result).context("decoding nm-hard-tools tool result")
    }

    async fn request(&self, method: &str, mut params: Value) -> Result<Value> {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
            "io.modelcontextprotocol/clientInfo": {
                "name": "crucible-broker",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        let id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut request = self
            .client
            .post(&self.url)
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .header("mcp-protocol-version", PROTOCOL_VERSION)
            .header("mcp-method", method);
        if let Some(name) = params.get("name").and_then(Value::as_str) {
            request = request.header("mcp-name", name);
        }
        if let Some(token) = &self.bearer_token {
            request = request.bearer_auth(token);
        }
        let mut response = request
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .with_context(|| format!("POST {}", self.url))?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE_BYTES as u64)
        {
            return Err(HardToolsError::ResponseTooLarge.into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.context("reading MCP response")? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(HardToolsError::ResponseTooLarge.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let envelope: Value =
            serde_json::from_slice(&bytes).context("decoding MCP JSON-RPC response")?;
        if envelope.get("id") != Some(&json!(id)) {
            return Err(HardToolsError::ResponseIdMismatch { request_id: id }.into());
        }
        if let Some(error) = envelope.get("error") {
            return Err(HardToolsError::Rpc {
                status,
                error: error.clone(),
            }
            .into());
        }
        if !status.is_success() {
            return Err(HardToolsError::Http { status }.into());
        }
        envelope
            .get("result")
            .cloned()
            .context("MCP JSON-RPC response omitted result")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::Request;
    use axum::response::Response;
    use axum::routing::post;

    async fn upstream(request: Request) -> Response {
        assert_eq!(request.headers()["mcp-protocol-version"], PROTOCOL_VERSION);
        let header_method = request.headers()["mcp-method"].to_str().unwrap().to_owned();
        let header_name = request
            .headers()
            .get("mcp-name")
            .map(|value| value.to_str().unwrap().to_owned());
        let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let envelope: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            envelope["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
            PROTOCOL_VERSION
        );
        let id = envelope["id"].clone();
        assert_eq!(header_method, envelope["method"].as_str().unwrap());
        let value = match envelope["method"].as_str().unwrap() {
            "tools/list" => {
                assert!(header_name.is_none());
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"tools": [{
                        "name": "ping",
                        "description": "Return pong.",
                        "inputSchema": {"type": "object", "additionalProperties": false},
                        "annotations": {"readOnlyHint": true}
                    }]}
                })
            }
            "tools/call" => {
                assert_eq!(header_name.as_deref(), Some("ping"));
                assert_eq!(envelope["params"]["name"], "ping");
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [{"type": "text", "text": "pong"}],
                        "structuredContent": {"value": "pong"},
                        "isError": false
                    }
                })
            }
            other => panic!("unexpected method {other}"),
        };
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(value.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn discovers_prefixes_and_forwards_nm_hard_tools() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().route("/mcp", post(upstream)))
                .await
                .unwrap();
        });
        let registry = HardTools::from_json(&format!(
            r#"[{{"name":"eval","url":"http://{address}/mcp"}}]"#
        ))
        .await
        .unwrap();
        assert_eq!(registry.tools.len(), 1);
        assert_eq!(registry.tools[0].exposed.name, "hard_eval_ping");
        assert_eq!(
            registry.tools[0]
                .exposed
                .annotations
                .as_ref()
                .and_then(|value| value.read_only_hint),
            Some(true)
        );
        let result = registry.tools[0]
            .service
            .call_tool("ping", Map::new())
            .await
            .unwrap();
        assert_eq!(result.structured_content, Some(json!({"value": "pong"})));
        server.abort();
    }
}
