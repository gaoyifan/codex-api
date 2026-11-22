use std::sync::Arc;

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use http::header::{AUTHORIZATION, USER_AGENT};
use http::{HeaderMap, StatusCode};
use secrecy::ExposeSecret;
use serde_json::{Map, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as TungsteniteError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use crate::credentials::{CredentialManager, CredentialSnapshot};
use crate::upstream_headers::{
    CODEX_ORIGINATOR, CODEX_USER_AGENT, CODEX_VERSION, codex_passthrough_headers,
};

const OPENAI_BETA_VALUE: &str = "responses_websockets=2026-02-06";
pub(crate) type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const RESPONSES_LITE_METADATA_KEY: &str =
    "ws_request_header_x_openai_internal_codex_responses_lite";

/// Opens one authenticated ChatGPT Codex Responses WebSocket connection.
///
/// An HTTP 401 during the initial WebSocket handshake triggers one serialized
/// credential refresh and exactly one retry. Other failures are returned
/// immediately and never cause a transport fallback.
pub(crate) async fn connect_upstream_websocket(
    base_url: &Url,
    credential_manager: Arc<CredentialManager>,
    downstream_headers: &HeaderMap,
) -> anyhow::Result<UpstreamWebSocket> {
    let credentials = credential_manager.credentials().await?;

    match connect_once(base_url, downstream_headers, &credentials).await {
        Err(TungsteniteError::Http(response)) if response.status() == StatusCode::UNAUTHORIZED => {
            let refreshed = credential_manager
                .refresh_after_unauthorized(credentials.generation)
                .await?;
            Ok(connect_once(base_url, downstream_headers, &refreshed).await?)
        }
        result => result.map_err(Into::into),
    }
}

pub(crate) fn responses_lite_prewarm(
    request: &mut Value,
    model: &Value,
    request_key: &str,
) -> anyhow::Result<Value> {
    let object = request
        .as_object_mut()
        .context("invalid upstream Responses request")?;
    let tools = object
        .remove("tools")
        .filter(|tools| tools.as_array().is_some_and(|tools| !tools.is_empty()));
    let client_instructions = object.remove("instructions");
    let base_instructions = model
        .get("base_instructions")
        .and_then(Value::as_str)
        .context("invalid upstream Responses request")?;

    normalize_responses_lite_fields(object, request_key);
    let mut prewarm: Map<_, _> = object
        .iter()
        .filter(|(key, _)| key.as_str() != "input")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let mut input = vec![serde_json::json!({
        "type": "additional_tools",
        "role": "developer",
    })];
    input[0]["tools"] = tools.unwrap_or_else(|| Value::Array(route_marker_tools()));
    input.push(developer_message(base_instructions));
    if let Some(Value::String(instructions)) = client_instructions
        && !instructions.is_empty()
    {
        input.push(developer_message(&instructions));
    }
    prewarm.insert("input".to_owned(), Value::Array(input));
    prewarm.insert("generate".to_owned(), Value::Bool(false));
    Ok(Value::Object(prewarm))
}

pub(crate) async fn send_responses_prewarm(
    upstream: &mut UpstreamWebSocket,
    prewarm: &Value,
) -> anyhow::Result<String> {
    upstream
        .send(UpstreamMessage::Text(prewarm.to_string().into()))
        .await?;
    loop {
        match upstream.next().await {
            Some(Ok(UpstreamMessage::Text(text))) => {
                let event: Value = serde_json::from_str(text.as_str())?;
                match event.get("type").and_then(Value::as_str) {
                    Some("response.completed") => {
                        return event
                            .pointer("/response/id")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .context("upstream prewarm response is missing an id");
                    }
                    Some("error" | "response.failed" | "response.incomplete") => {
                        bail!("upstream prewarm failed");
                    }
                    _ => {}
                }
            }
            Some(Ok(UpstreamMessage::Ping(payload))) => {
                upstream.send(UpstreamMessage::Pong(payload)).await?
            }
            Some(Ok(UpstreamMessage::Pong(_))) => {}
            Some(Err(error)) => return Err(error.into()),
            _ => bail!("upstream closed during prewarm"),
        }
    }
}

fn normalize_responses_lite_fields(object: &mut Map<String, Value>, request_key: &str) {
    object.insert("parallel_tool_calls".to_owned(), Value::Bool(false));
    object.insert(
        "type".to_owned(),
        Value::String("response.create".to_owned()),
    );
    object.insert("store".to_owned(), Value::Bool(false));
    object.insert("stream".to_owned(), Value::Bool(true));
    object
        .entry("tool_choice")
        .or_insert_with(|| Value::String("auto".to_owned()));
    object
        .entry("include")
        .or_insert_with(|| serde_json::json!(["reasoning.encrypted_content"]));
    object
        .entry("prompt_cache_key")
        .or_insert_with(|| Value::String(request_key.to_owned()));
    object
        .entry("text")
        .or_insert_with(|| serde_json::json!({"verbosity": "low"}));
    let reasoning = object
        .entry("reasoning")
        .or_insert_with(|| serde_json::json!({"effort": "medium"}));
    if let Some(reasoning) = reasoning.as_object_mut() {
        reasoning.insert("context".to_owned(), Value::String("all_turns".to_owned()));
    }
    let metadata = object
        .entry("client_metadata")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(metadata) = metadata.as_object_mut() {
        metadata.insert(
            RESPONSES_LITE_METADATA_KEY.to_owned(),
            Value::String("true".to_owned()),
        );
        metadata
            .entry("session_id")
            .or_insert_with(|| Value::String(request_key.to_owned()));
        metadata
            .entry("thread_id")
            .or_insert_with(|| Value::String(request_key.to_owned()));
    }
}

fn developer_message(text: &str) -> Value {
    serde_json::json!({
        "type": "message",
        "role": "developer",
        "content": [{"type": "input_text", "text": text}],
    })
}

fn route_marker_tools() -> Vec<Value> {
    ["functions", "utility", "helpers"]
        .into_iter()
        .map(|name| {
            serde_json::json!({
                "type": "namespace",
                "name": name,
                "description": "",
                "tools": [{
                    "type": "function",
                    "name": "noop",
                    "description": "No operation.",
                    "strict": false,
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false,
                    },
                }],
            })
        })
        .collect()
}

async fn connect_once(
    base_url: &Url,
    downstream_headers: &HeaderMap,
    credentials: &CredentialSnapshot,
) -> Result<UpstreamWebSocket, TungsteniteError> {
    let websocket_url = responses_websocket_url(base_url);
    let mut request = websocket_url.as_str().into_client_request()?;

    let mut authorization = HeaderValue::from_str(&format!(
        "Bearer {}",
        credentials.access_token.expose_secret()
    ))?;
    authorization.set_sensitive(true);
    let headers = request.headers_mut();
    headers.insert(AUTHORIZATION, authorization);
    headers.insert(
        "chatgpt-account-id",
        HeaderValue::from_str(&credentials.account_id)?,
    );
    for (name, value) in [
        ("openai-beta", OPENAI_BETA_VALUE),
        ("originator", CODEX_ORIGINATOR),
        ("version", CODEX_VERSION),
        (USER_AGENT.as_str(), CODEX_USER_AGENT),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    headers.extend(codex_passthrough_headers(downstream_headers));

    connect_async(request)
        .await
        .map(|(stream, _response)| stream)
}

fn responses_websocket_url(base_url: &Url) -> Url {
    let mut url = base_url.clone();
    let websocket_scheme = if base_url.scheme() == "http" {
        "ws"
    } else {
        "wss"
    };
    url.set_scheme(websocket_scheme)
        .expect("validated HTTP(S) upstream URL accepts a WebSocket scheme");

    let base_path = base_url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/responses"));
    url.set_query(None);
    url.set_fragment(None);
    url
}
