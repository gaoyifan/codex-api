use std::{
    fs,
    net::TcpListener as StdTcpListener,
    path::Path,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use eventsource_stream::Eventsource;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use tokio::{
    net::TcpStream,
    process::Command,
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const AUTH_PATH: &str = "/home/yifan/.codex-test/auth.json";
const STATE_PATH: &str = "/home/yifan/.codex-test/codex-api.sqlite3";
const MODEL: &str = "gpt-5.6-luna";
const PROMPT: &str = "Reply with exactly OK.";

#[tokio::test]
#[ignore = "uses the real ChatGPT subscription and persistent live-test database"]
async fn live_chatgpt_contract_supports_responses_chat_and_websocket() -> Result<()> {
    ensure!(
        Path::new(AUTH_PATH).is_file(),
        "the live ChatGPT auth seed is unavailable"
    );

    let reserved_listener =
        StdTcpListener::bind("127.0.0.1:0").context("failed to reserve a local live-test port")?;
    let listen_address = reserved_listener
        .local_addr()
        .context("failed to read the reserved live-test address")?;
    drop(reserved_listener);

    let run_nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_nanos();
    let api_key_id = format!("live-contract-{}-{run_nonce}", std::process::id());
    let api_key_secret = format!("sk-local-{run_nonce}");

    let config_directory = tempfile::tempdir().context("failed to create a config directory")?;
    let config_path = config_directory.path().join("live.toml");
    let config = format!(
        r#"[server]
listen = "{listen_address}"
enable_websockets = true

[state]
path = "{STATE_PATH}"

[upstream]
base_url = "https://chatgpt.com/backend-api/codex"
oauth_token_url = "https://auth.openai.com/oauth/token"
auth_file = "{AUTH_PATH}"
supports_websockets = true

[[api_keys]]
id = "{api_key_id}"
secret = "{api_key_secret}"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#
    );
    fs::write(&config_path, config).context("failed to write the live-test config")?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_codex-api"));
    command
        .arg("--config")
        .arg(&config_path)
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut service = command
        .spawn()
        .context("failed to start the codex-api process")?;

    let startup_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(listen_address).await.is_ok() {
            break;
        }
        if let Some(status) = service
            .try_wait()
            .context("failed to inspect the codex-api process")?
        {
            bail!("codex-api exited before listening with status {status}");
        }
        ensure!(
            Instant::now() < startup_deadline,
            "codex-api did not start listening in time"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .context("failed to build the live-test HTTP client")?;
    let http_base = format!("http://{listen_address}");

    let responses_http = client
        .post(format!("{http_base}/v1/responses"))
        .bearer_auth(&api_key_secret)
        .json(&json!({
            "model": MODEL,
            "input": PROMPT,
            "stream": true,
            "store": false,
            "reasoning": { "effort": "low" }
        }))
        .send()
        .await
        .context("the live Responses HTTP request failed")?;
    if responses_http.status() != StatusCode::OK {
        let status = responses_http.status();
        let error: Value = responses_http
            .json()
            .await
            .context("the live Responses HTTP error was not JSON")?;
        let detail = match error.get("detail") {
            Some(Value::Array(items)) => Value::Array(
                items
                    .iter()
                    .map(|item| {
                        json!({
                            "type": item.get("type"),
                            "loc": item.get("loc"),
                            "msg": item.get("msg"),
                        })
                    })
                    .collect(),
            ),
            Some(Value::String(detail)) => Value::String(detail.clone()),
            _ => Value::Null,
        };
        bail!(
            "the live Responses HTTP request returned status {status}, detail={detail}, type={:?}, code={:?}, param={:?}",
            error.pointer("/error/type").and_then(Value::as_str),
            error.pointer("/error/code").and_then(Value::as_str),
            error.pointer("/error/param").and_then(Value::as_str),
        );
    }
    let content_type = responses_http
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    ensure!(
        content_type.starts_with("text/event-stream"),
        "the live Responses HTTP request did not return SSE"
    );

    let mut response_events = responses_http.bytes_stream().eventsource();
    let responses_terminal: Value = timeout(Duration::from_secs(180), async {
        while let Some(event) = response_events.next().await {
            let event = event.context("the live Responses SSE stream was malformed")?;
            if event.data.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&event.data)
                .context("the live Responses SSE event was not JSON")?;
            match value.get("type").and_then(Value::as_str) {
                Some("response.completed") => return Ok::<Value, anyhow::Error>(value),
                Some("response.incomplete" | "response.failed" | "error") => {
                    bail!("the live Responses HTTP request did not complete successfully")
                }
                _ => {}
            }
        }
        bail!("the live Responses SSE stream ended without a terminal event")
    })
    .await
    .context("the live Responses SSE request timed out")??;
    let responses_usage = responses_terminal
        .pointer("/response/usage")
        .context("the live Responses terminal event omitted usage")?;
    let responses_input_tokens = responses_usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .context("the live Responses usage omitted input tokens")?;
    let responses_output_tokens = responses_usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .context("the live Responses usage omitted output tokens")?;
    let responses_total_tokens = responses_usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .context("the live Responses usage omitted total tokens")?;
    ensure!(
        responses_input_tokens > 0
            && responses_output_tokens > 0
            && responses_total_tokens >= responses_input_tokens + responses_output_tokens,
        "the live Responses HTTP usage was not internally consistent"
    );

    let chat_http = client
        .post(format!("{http_base}/v1/chat/completions"))
        .bearer_auth(&api_key_secret)
        .json(&json!({
            "model": MODEL,
            "messages": [{ "role": "user", "content": PROMPT }],
            "stream": false,
            "n": 1,
            "reasoning_effort": "low"
        }))
        .send()
        .await
        .context("the live Chat Completions request failed")?;
    ensure!(
        chat_http.status() == StatusCode::OK,
        "the live Chat Completions request returned status {}",
        chat_http.status()
    );
    let chat: Value = chat_http
        .json()
        .await
        .context("the live Chat Completions response was not JSON")?;
    ensure!(
        chat.get("object").and_then(Value::as_str) == Some("chat.completion"),
        "the live Chat Completions response had the wrong object type"
    );
    ensure!(
        chat.get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty())
            && chat.get("model").and_then(Value::as_str) == Some(MODEL)
            && chat.get("created").and_then(Value::as_u64).is_some(),
        "the live Chat Completions response omitted terminal metadata"
    );
    let choices = chat
        .get("choices")
        .and_then(Value::as_array)
        .context("the live Chat Completions response omitted choices")?;
    if !(choices.len() == 1
        && choices[0].pointer("/message/role").and_then(Value::as_str) == Some("assistant")
        && choices[0]
            .pointer("/message/content")
            .and_then(Value::as_str)
            .is_some()
        && choices[0].get("finish_reason").and_then(Value::as_str) == Some("stop"))
    {
        bail!(
            "the live Chat response had role={:?}, content_kind={}, content_length={:?}, finish_reason={:?}",
            choices
                .first()
                .and_then(|choice| choice.pointer("/message/role"))
                .and_then(Value::as_str),
            choices
                .first()
                .and_then(|choice| choice.pointer("/message/content"))
                .map(|value| match value {
                    Value::Null => "null",
                    Value::String(_) => "string",
                    _ => "other",
                })
                .unwrap_or("missing"),
            choices
                .first()
                .and_then(|choice| choice.pointer("/message/content"))
                .and_then(Value::as_str)
                .map(str::len),
            choices
                .first()
                .and_then(|choice| choice.get("finish_reason"))
                .and_then(Value::as_str),
        );
    }
    let chat_usage = chat
        .get("usage")
        .context("the live Chat Completions response omitted usage")?;
    let chat_prompt_tokens = chat_usage
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .context("the live Chat Completions usage omitted prompt tokens")?;
    let chat_completion_tokens = chat_usage
        .get("completion_tokens")
        .and_then(Value::as_u64)
        .context("the live Chat Completions usage omitted completion tokens")?;
    let chat_total_tokens = chat_usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .context("the live Chat Completions usage omitted total tokens")?;
    ensure!(
        chat_prompt_tokens > 0
            && chat_completion_tokens > 0
            && chat_total_tokens >= chat_prompt_tokens + chat_completion_tokens,
        "the live Chat Completions usage was not internally consistent"
    );

    let websocket_url = format!("ws://{listen_address}/v1/responses");
    let mut websocket_request = websocket_url
        .into_client_request()
        .context("failed to construct the live Responses WebSocket request")?;
    websocket_request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {api_key_secret}"))
            .context("failed to construct the downstream authorization header")?,
    );
    let (mut websocket, upgrade_response) = connect_async(websocket_request)
        .await
        .context("the live Responses WebSocket handshake failed")?;
    ensure!(
        upgrade_response.status() == StatusCode::SWITCHING_PROTOCOLS,
        "the live Responses WebSocket handshake did not upgrade"
    );
    websocket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": MODEL,
                "input": PROMPT,
                "store": false,
                "reasoning": { "effort": "low" }
            })
            .to_string()
            .into(),
        ))
        .await
        .context("failed to send the live Responses WebSocket request")?;

    let websocket_terminal: Value = timeout(Duration::from_secs(180), async {
        loop {
            match websocket.next().await {
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(text.as_str())
                        .context("a live Responses WebSocket event was not JSON")?;
                    match value.get("type").and_then(Value::as_str) {
                        Some("response.completed") => return Ok::<Value, anyhow::Error>(value),
                        Some("response.incomplete" | "response.failed" | "error") => {
                            bail!(
                                "the live Responses WebSocket request did not complete successfully"
                            )
                        }
                        _ => {}
                    }
                }
                Some(Ok(Message::Ping(payload))) => websocket
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to answer a live Responses WebSocket ping")?,
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    bail!("the live Responses WebSocket closed before its terminal event")
                }
                Some(Ok(Message::Binary(_))) => {
                    bail!("the live Responses WebSocket returned an unexpected binary message")
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => {
                    return Err(error).context("the live Responses WebSocket stream failed");
                }
            }
        }
    })
    .await
    .context("the live Responses WebSocket request timed out")??;
    let websocket_usage = websocket_terminal
        .pointer("/response/usage")
        .context("the live Responses WebSocket terminal event omitted usage")?;
    let websocket_input_tokens = websocket_usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .context("the live Responses WebSocket usage omitted input tokens")?;
    let websocket_output_tokens = websocket_usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .context("the live Responses WebSocket usage omitted output tokens")?;
    let websocket_total_tokens = websocket_usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .context("the live Responses WebSocket usage omitted total tokens")?;
    ensure!(
        websocket_input_tokens > 0
            && websocket_output_tokens > 0
            && websocket_total_tokens >= websocket_input_tokens + websocket_output_tokens,
        "the live Responses WebSocket usage was not internally consistent"
    );
    websocket
        .close(None)
        .await
        .context("failed to close the live Responses WebSocket")?;

    let database_options = SqliteConnectOptions::new()
        .filename(STATE_PATH)
        .read_only(true);
    let mut database = SqliteConnection::connect_with(&database_options)
        .await
        .context("failed to open the public live-test request log")?;
    let request_rows = sqlx::query(
        "SELECT api_protocol, transport, model, status, input_tokens, output_tokens \
         FROM request_logs WHERE api_key_id = ? ORDER BY id",
    )
    .bind(&api_key_id)
    .fetch_all(&mut database)
    .await
    .context("failed to read the public live-test request log")?;
    ensure!(
        request_rows.len() == 3,
        "the live flow did not commit exactly three request-log rows"
    );
    let expected_rows = [
        ("responses", "http_sse"),
        ("chat_completions", "http_sse"),
        ("responses", "websocket"),
    ];
    for (row, (expected_protocol, expected_transport)) in request_rows.iter().zip(expected_rows) {
        let input_tokens: Option<i64> = row
            .try_get("input_tokens")
            .context("a live request-log row omitted input tokens")?;
        let output_tokens: Option<i64> = row
            .try_get("output_tokens")
            .context("a live request-log row omitted output tokens")?;
        ensure!(
            row.try_get::<String, _>("api_protocol")? == expected_protocol
                && row.try_get::<String, _>("transport")? == expected_transport
                && row.try_get::<String, _>("model")? == MODEL
                && row.try_get::<String, _>("status")? == "completed"
                && input_tokens.is_some_and(|tokens| tokens > 0)
                && output_tokens.is_some_and(|tokens| tokens > 0),
            "a live request-log row did not describe its completed operation"
        );
    }
    drop(database);

    let process_id = service
        .id()
        .context("the codex-api process had no operating-system ID")?;
    let signal_status = Command::new("kill")
        .arg("-TERM")
        .arg(process_id.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("failed to signal the codex-api process")?;
    ensure!(signal_status.success(), "failed to send codex-api SIGTERM");
    let exit_status = timeout(Duration::from_secs(30), service.wait())
        .await
        .context("codex-api did not stop after SIGTERM")?
        .context("failed to wait for the codex-api process")?;
    ensure!(
        exit_status.success(),
        "codex-api did not exit successfully after the live flow"
    );

    Ok(())
}
