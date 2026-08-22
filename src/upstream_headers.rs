use http::HeaderMap;

pub(crate) const CODEX_ORIGINATOR: &str = "codex_cli_rs";
pub(crate) const CODEX_VERSION: &str = "0.147.0";
pub(crate) const CODEX_USER_AGENT: &str = "codex_cli_rs/0.147.0 (codex-api)";

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
