use http::{HeaderMap, HeaderValue};
use serde_json::Value;

pub(crate) const CODEX_ORIGINATOR: &str = "codex_cli_rs";
pub(crate) const CODEX_VERSION: &str = "0.153.4";
pub(crate) const CODEX_USER_AGENT: &str = "codex_cli_rs/0.153.4 (codex-api)";

const CODEX_PASSTHROUGH_HEADERS: &[&str] = &[
    "originator",
    "session_id",
    "thread_id",
    "session-id",
    "thread-id",
    "x-client-request-id",
    "user-agent",
    "x-codex-beta-features",
    "x-codex-turn-state",
    "x-codex-turn-metadata",
    "x-codex-window-id",
    "x-codex-parent-thread-id",
    "x-openai-subagent",
    "x-openai-memgen-request",
    "x-responsesapi-include-timing-metrics",
    "x-openai-internal-codex-responses-lite",
];

pub(crate) fn codex_passthrough_headers(source: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for &name in CODEX_PASSTHROUGH_HEADERS {
        if let Some(value) = source.get(name) {
            headers.insert(name, value.clone());
        }
    }
    headers
}

/// Preserve prompt-cache routing for clients that supply only the body cache key.
/// This upstream affinity hint does not identify a downstream conversation.
pub(crate) fn codex_request_headers(source: &HeaderMap, body: &Value) -> HeaderMap {
    let mut headers = codex_passthrough_headers(source);
    if !headers.contains_key("session_id")
        && !headers.contains_key("session-id")
        && let Some(key) = body
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
        && let Ok(session_id) = HeaderValue::from_str(key)
    {
        headers.insert("session_id", session_id);
    }
    headers
}
