use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, USER_AGENT};
use http::{HeaderMap, StatusCode};
use http::{HeaderName, HeaderValue};
use secrecy::ExposeSecret;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as TungsteniteError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use crate::credentials::{CredentialError, CredentialManager, CredentialSnapshot};
use crate::upstream_headers::{
    CODEX_ORIGINATOR, CODEX_USER_AGENT, CODEX_VERSION, codex_passthrough_headers,
};

const RESPONSES_PATH: &str = "responses";
const OPENAI_BETA_HEADER: HeaderName = HeaderName::from_static("openai-beta");
const OPENAI_BETA_VALUE: &str = "responses_websockets=2026-02-06";
const CHATGPT_ACCOUNT_ID_HEADER: HeaderName = HeaderName::from_static("chatgpt-account-id");
const ORIGINATOR_HEADER: HeaderName = HeaderName::from_static("originator");
const VERSION_HEADER: HeaderName = HeaderName::from_static("version");
pub(crate) type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const RESPONSES_LITE_METADATA_KEY: &str =
    "ws_request_header_x_openai_internal_codex_responses_lite";

#[derive(Debug, Error)]
pub(crate) enum UpstreamWebSocketError {
    #[error("failed to obtain upstream credentials")]
    Credentials(#[source] CredentialError),

    #[error("failed to refresh upstream credentials after an authentication rejection")]
    CredentialRefresh(#[source] CredentialError),

    #[error("upstream WebSocket authentication was rejected")]
    AuthenticationRejected,

    #[error("upstream WebSocket handshake failed with HTTP status {status}")]
    Handshake { status: StatusCode },

    #[error("failed to build upstream WebSocket request")]
    Request,

    #[error("failed to connect to upstream WebSocket")]
    Transport(#[source] TungsteniteError),
}

/// Opens one authenticated ChatGPT Codex Responses WebSocket connection.
///
/// An HTTP 401 during the initial WebSocket handshake triggers one serialized
/// credential refresh and exactly one retry. Other failures are returned
/// immediately and never cause a transport fallback.
pub(crate) async fn connect_upstream_websocket(
    base_url: &Url,
    credential_manager: Arc<CredentialManager>,
    downstream_headers: &HeaderMap,
) -> Result<UpstreamWebSocket, UpstreamWebSocketError> {
    let credentials = credential_manager
        .credentials()
        .await
        .map_err(UpstreamWebSocketError::Credentials)?;

    match connect_once(base_url, downstream_headers, &credentials).await {
        Err(UpstreamWebSocketError::AuthenticationRejected) => {
            let refreshed = credential_manager
                .refresh_after_unauthorized(credentials.generation)
                .await
                .map_err(UpstreamWebSocketError::CredentialRefresh)?;
            connect_once(base_url, downstream_headers, &refreshed).await
        }
        result => result,
    }
}

pub(crate) async fn prepare_responses_lite(
    upstream: &mut UpstreamWebSocket,
    request: &mut Value,
    model: &Value,
    request_key: &str,
) -> Result<(), UpstreamWebSocketError> {
    if model.get("use_responses_lite").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }

    let prewarm = responses_lite_prewarm(request, model, request_key)?;
    let previous_response_id = send_responses_prewarm(upstream, &prewarm).await?;
    request["previous_response_id"] = Value::String(previous_response_id);
    Ok(())
}

pub(crate) fn responses_lite_prewarm(
    request: &mut Value,
    model: &Value,
    request_key: &str,
) -> Result<Value, UpstreamWebSocketError> {
    let object = request
        .as_object_mut()
        .ok_or(UpstreamWebSocketError::Request)?;
    let tools = object
        .remove("tools")
        .and_then(|value| value.as_array().cloned())
        .filter(|tools| !tools.is_empty());
    let disable_tools = tools.is_none();
    let tools = tools.unwrap_or_else(route_marker_tools);
    let client_instructions = object
        .remove("instructions")
        .and_then(|value| value.as_str().map(str::to_owned));
    let base_instructions = model
        .get("base_instructions")
        .and_then(Value::as_str)
        .ok_or(UpstreamWebSocketError::Request)?;

    normalize_responses_lite_fields(object, request_key, disable_tools);
    let mut prewarm = Value::Object(object.clone());
    let prewarm_object = prewarm
        .as_object_mut()
        .expect("cloned Responses request remains an object");
    let mut input = vec![serde_json::json!({
        "type": "additional_tools",
        "role": "developer",
        "tools": tools,
    })];
    input.push(developer_message(base_instructions));
    if let Some(instructions) = client_instructions.filter(|value| !value.is_empty()) {
        input.push(developer_message(&instructions));
    }
    prewarm_object.insert("input".to_owned(), Value::Array(input));
    prewarm_object.insert("generate".to_owned(), Value::Bool(false));
    prewarm_object.insert("tool_choice".to_owned(), Value::String("auto".to_owned()));
    Ok(prewarm)
}

pub(crate) async fn send_responses_prewarm(
    upstream: &mut UpstreamWebSocket,
    prewarm: &Value,
) -> Result<String, UpstreamWebSocketError> {
    upstream
        .send(UpstreamMessage::Text(prewarm.to_string().into()))
        .await
        .map_err(UpstreamWebSocketError::Transport)?;
    let previous_response_id = loop {
        match upstream.next().await {
            Some(Ok(UpstreamMessage::Text(text))) => {
                let event: Value = serde_json::from_str(text.as_str())
                    .map_err(|_| UpstreamWebSocketError::Request)?;
                match event.get("type").and_then(Value::as_str) {
                    Some("response.completed") => {
                        break event
                            .pointer("/response/id")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .ok_or(UpstreamWebSocketError::Request)?;
                    }
                    Some("error" | "response.failed" | "response.incomplete") => {
                        return Err(UpstreamWebSocketError::Request);
                    }
                    _ => {}
                }
            }
            Some(Ok(UpstreamMessage::Ping(payload))) => upstream
                .send(UpstreamMessage::Pong(payload))
                .await
                .map_err(UpstreamWebSocketError::Transport)?,
            Some(Ok(UpstreamMessage::Pong(_))) => {}
            Some(Ok(UpstreamMessage::Close(_))) | None => {
                return Err(UpstreamWebSocketError::Request);
            }
            Some(Ok(UpstreamMessage::Binary(_) | UpstreamMessage::Frame(_))) => {
                return Err(UpstreamWebSocketError::Request);
            }
            Some(Err(error)) => return Err(UpstreamWebSocketError::Transport(error)),
        }
    };

    Ok(previous_response_id)
}

fn normalize_responses_lite_fields(
    object: &mut Map<String, Value>,
    request_key: &str,
    disable_tools: bool,
) {
    object.insert("parallel_tool_calls".to_owned(), Value::Bool(false));
    object.insert(
        "type".to_owned(),
        Value::String("response.create".to_owned()),
    );
    object.insert("store".to_owned(), Value::Bool(false));
    object.insert("stream".to_owned(), Value::Bool(true));
    if disable_tools {
        object.insert("tool_choice".to_owned(), Value::String("none".to_owned()));
    }
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
) -> Result<UpstreamWebSocket, UpstreamWebSocketError> {
    let websocket_url = responses_websocket_url(base_url);
    let mut request = websocket_url
        .as_str()
        .into_client_request()
        .map_err(|_| UpstreamWebSocketError::Request)?;

    let mut authorization = HeaderValue::from_str(&format!(
        "Bearer {}",
        credentials.access_token.expose_secret()
    ))
    .map_err(|_| UpstreamWebSocketError::Request)?;
    authorization.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, authorization);
    request.headers_mut().insert(
        CHATGPT_ACCOUNT_ID_HEADER,
        HeaderValue::from_str(&credentials.account_id)
            .map_err(|_| UpstreamWebSocketError::Request)?,
    );
    request.headers_mut().insert(
        OPENAI_BETA_HEADER,
        HeaderValue::from_static(OPENAI_BETA_VALUE),
    );
    request.headers_mut().insert(
        ORIGINATOR_HEADER,
        HeaderValue::from_static(CODEX_ORIGINATOR),
    );
    request
        .headers_mut()
        .insert(VERSION_HEADER, HeaderValue::from_static(CODEX_VERSION));
    request
        .headers_mut()
        .insert(USER_AGENT, HeaderValue::from_static(CODEX_USER_AGENT));
    request
        .headers_mut()
        .extend(codex_passthrough_headers(downstream_headers));

    connect_async(request)
        .await
        .map(|(stream, _response)| stream)
        .map_err(classify_connect_error)
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
    url.set_path(&format!("{base_path}/{RESPONSES_PATH}"));
    url.set_query(None);
    url.set_fragment(None);
    url
}

fn classify_connect_error(error: TungsteniteError) -> UpstreamWebSocketError {
    match error {
        TungsteniteError::Http(response) if response.status() == StatusCode::UNAUTHORIZED => {
            UpstreamWebSocketError::AuthenticationRejected
        }
        TungsteniteError::Http(response) => UpstreamWebSocketError::Handshake {
            status: response.status(),
        },
        error => UpstreamWebSocketError::Transport(error),
    }
}
