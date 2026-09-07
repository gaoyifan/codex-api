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
    let trace_path = config_directory.path().join("service.log");
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
        .stdout(Stdio::from(fs::File::create(&trace_path)?))
        .stderr(Stdio::null())
        .env("RUST_LOG", "codex_api::http_api::responses_session=debug")
        .env("NO_COLOR", "1")
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

    let session_token = format!("violet{run_nonce}");
    let session_prompt =
        format!("Remember this token for later: {session_token}. Reply with exactly OK.");
    let first_started = Instant::now();
    let responses_http = client
        .post(format!("{http_base}/v1/responses"))
        .bearer_auth(&api_key_secret)
        .header("thread_id", &api_key_id)
        .json(&json!({
            "model": MODEL,
            "input": session_prompt,
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
            "the live Responses HTTP request returned status {status}, detail={detail}, type={:?}, code={:?}, param={:?}, message={:?}",
            error.pointer("/error/type").and_then(Value::as_str),
            error.pointer("/error/code").and_then(Value::as_str),
            error.pointer("/error/param").and_then(Value::as_str),
            error.pointer("/error/message").and_then(Value::as_str),
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

    let (responses_terminal, first_output, first_event) =
        read_live_response(responses_http, first_started).await?;
    eprintln!(
        "live Responses initial: first_event_ms={}",
        first_event.as_millis()
    );
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

    let mut history = vec![
        json!({"type":"message","role":"user","content":[{"type":"input_text","text":session_prompt}]}),
    ];
    history.extend(first_output);
    let mut previous_id = responses_terminal["response"]["id"].clone();
    for mode in ["explicit", "full_history", "reconnect"] {
        let input = json!({"type":"message","role":"user","content":[{"type":"input_text","text":"What token did I ask you to remember? Reply with only that token."}]});
        history.push(input.clone());
        let mut payload = json!({
            "model":MODEL,"input":[input],"previous_response_id":previous_id,
            "stream":true,"store":false,"reasoning":{"effort":"low"}
        });
        if mode == "full_history" {
            payload["input"] = json!(history);
            payload
                .as_object_mut()
                .unwrap()
                .remove("previous_response_id");
        }
        let mut request = client
            .post(format!("{http_base}/v1/responses"))
            .bearer_auth(&api_key_secret)
            .header("thread_id", &api_key_id)
            .json(&payload);
        if mode == "reconnect" {
            // An immutable handshake change forces a new socket while retaining the logical snapshot.
            request = request.header(
                "user-agent",
                "codex_cli_rs/0.153.4 (codex-api live reconnect)",
            );
        }
        let started = Instant::now();
        let response = request
            .send()
            .await
            .context("live session continuation failed")?;
        ensure!(
            response.status() == StatusCode::OK,
            "live {mode} returned {}",
            response.status()
        );
        let (terminal, output, first_event) = read_live_response(response, started).await?;
        let text = output
            .iter()
            .filter(|item| item["role"] == "assistant")
            .filter_map(|item| item["content"].as_array())
            .flatten()
            .filter_map(|part| part["text"].as_str())
            .collect::<String>();
        ensure!(
            text.contains(&session_token),
            "live {mode} lost the remembered context"
        );
        previous_id = terminal["response"]["id"].clone();
        history.extend(output);
        eprintln!(
            "live Responses {mode}: first_event_ms={}",
            first_event.as_millis()
        );
    }
    let traces = fs::read_to_string(&trace_path)?;
    let preparations: Vec<_> = traces
        .lines()
        .filter(|line| line.contains("Prepared Responses WebSocket"))
        .collect();
    ensure!(
        preparations.len() == 4,
        "expected four prepared HTTP Responses operations"
    );
    ensure!(
        preparations
            .iter()
            .filter(|line| line.contains("connection_reused=false"))
            .count()
            == 2,
        "expected two WS handshakes across the live HTTP session"
    );
    ensure!(
        preparations
            .iter()
            .filter(|line| line.contains("context_reused=false"))
            .count()
            == 2,
        "expected only the initial and rebuilt contexts to prewarm"
    );
    eprintln!("live Responses session: requests=4 handshakes=2 prewarms=2");

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
        request_rows.len() == 6,
        "the live flow did not commit exactly six request-log rows"
    );
    let expected_rows = [
        ("responses", "http_sse"),
        ("responses", "http_sse"),
        ("responses", "http_sse"),
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

async fn read_live_response(
    response: reqwest::Response,
    started: Instant,
) -> Result<(Value, Vec<Value>, Duration)> {
    let mut events = response.bytes_stream().eventsource();
    timeout(Duration::from_secs(180), async {
        let mut output = Vec::new();
        let mut first_event = None;
        while let Some(event) = events.next().await {
            let event = event.context("live Responses SSE stream was malformed")?;
            first_event.get_or_insert_with(|| started.elapsed());
            if event.data.trim().is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_str(&event.data).context("live SSE event was not JSON")?;
            match value["type"].as_str() {
                Some("response.output_item.done") => output.push(value["item"].clone()),
                Some("response.completed") => {
                    if let Some(items) = value
                        .pointer("/response/output")
                        .and_then(Value::as_array)
                        .filter(|items| !items.is_empty())
                    {
                        output = items.clone();
                    }
                    return Ok((value, output, first_event.unwrap()));
                }
                Some("response.incomplete" | "response.failed" | "error") => {
                    bail!("live Responses request did not complete successfully")
                }
                _ => {}
            }
        }
        bail!("live Responses stream ended without a terminal event")
    })
    .await
    .context("live Responses request timed out")?
}
