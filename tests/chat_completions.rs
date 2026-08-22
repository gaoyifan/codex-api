use std::{
    collections::VecDeque,
    convert::Infallible,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::post,
};
use reqwest::{Client, Response as ClientResponse};
use serde_json::{Value, json};
use sqlx::{Connection, Row, SqliteConnection};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const API_KEY: &str = "sk-chat-test";
const MODEL: &str = "gpt-test";

struct SseChunk {
    delay: Duration,
    data: String,
}

enum UpstreamReply {
    Sse(Vec<SseChunk>),
    Json(StatusCode, Value),
}

impl UpstreamReply {
    fn events(events: Vec<Value>) -> Self {
        Self::Sse(
            events
                .into_iter()
                .map(|event| SseChunk {
                    delay: Duration::ZERO,
                    data: sse_event(&event),
                })
                .collect(),
        )
    }

    fn raw(data: impl Into<String>) -> Self {
        Self::Sse(vec![SseChunk {
            delay: Duration::ZERO,
            data: data.into(),
        }])
    }
}

struct UpstreamState {
    replies: Mutex<VecDeque<UpstreamReply>>,
    requests: Mutex<Vec<Value>>,
    latest_headers: Mutex<HeaderMap>,
    request_count: AtomicUsize,
}

struct ScriptedUpstream {
    addr: SocketAddr,
    state: Arc<UpstreamState>,
    task: JoinHandle<()>,
}

impl ScriptedUpstream {
    async fn start(replies: Vec<UpstreamReply>) -> Self {
        let state = Arc::new(UpstreamState {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
            latest_headers: Mutex::new(HeaderMap::new()),
            request_count: AtomicUsize::new(0),
        });
        let app = Router::new()
            .route("/responses", post(upstream_handler))
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self { addr, state, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn request_count(&self) -> usize {
        self.state.request_count.load(Ordering::SeqCst)
    }

    async fn requests(&self) -> Vec<Value> {
        self.state.requests.lock().await.clone()
    }
}

impl Drop for ScriptedUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream_handler(
    State(state): State<Arc<UpstreamState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    state.request_count.fetch_add(1, Ordering::SeqCst);
    let request = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.requests.lock().await.push(request);
    *state.latest_headers.lock().await = headers;

    let reply = state.replies.lock().await.pop_front().unwrap_or_else(|| {
        UpstreamReply::Json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": {"message": "no scripted upstream reply"}}),
        )
    });

    match reply {
        UpstreamReply::Json(status, value) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
        UpstreamReply::Sse(chunks) => {
            let stream = async_stream::stream! {
                for chunk in chunks {
                    if !chunk.delay.is_zero() {
                        sleep(chunk.delay).await;
                    }
                    yield Ok::<Bytes, Infallible>(Bytes::from(chunk.data));
                }
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(stream))
                .unwrap()
        }
    }
}

struct TestRelay {
    _directory: TempDir,
    child: Child,
    client: Client,
    base_url: String,
    database_path: PathBuf,
}

impl TestRelay {
    async fn start(upstream: &ScriptedUpstream) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let listen = unused_addr();
        let auth_path = directory.path().join("auth.json");
        let database_path = directory.path().join("state.sqlite3");
        let config_path = directory.path().join("config.toml");

        std::fs::write(
            &auth_path,
            serde_json::to_vec_pretty(&json!({
                "auth_mode": "chatgpt",
                "OPENAI_API_KEY": null,
                "tokens": {
                    "id_token": "unused-id-token",
                    "access_token": "eyJhbGciOiJub25lIn0.eyJleHAiOjQxMDI0NDQ4MDB9.test",
                    "refresh_token": "unused-refresh-token",
                    "account_id": "acct-test"
                },
                "last_refresh": "2026-08-09T00:00:00Z"
            }))
            .unwrap(),
        )
        .unwrap();

        let config = format!(
            r#"
[server]
listen = "{listen}"
enable_websockets = false

[state]
path = "{}"

[upstream]
base_url = "{}"
oauth_token_url = "{}/oauth/token"
auth_file = "{}"
supports_websockets = false

[[api_keys]]
id = "chat-client"
secret = "{API_KEY}"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
            database_path.display(),
            upstream.base_url(),
            upstream.base_url(),
            auth_path.display(),
        );
        std::fs::write(&config_path, config).unwrap();

        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
            .arg("--config")
            .arg(&config_path)
            .arg("serve")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("codex-api exited before listening: {status}");
            }
            if TcpStream::connect(listen).await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "codex-api did not listen on {listen}"
            );
            sleep(Duration::from_millis(20)).await;
        }

        Self {
            _directory: directory,
            child,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            base_url: format!("http://{listen}"),
            database_path,
        }
    }

    fn request(&self, body: &Value) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}/v1/chat/completions", self.base_url))
            .bearer_auth(API_KEY)
            .json(body)
    }

    async fn post(&self, body: &Value) -> ClientResponse {
        self.request(body).send().await.unwrap()
    }

    async fn database(&self) -> SqliteConnection {
        SqliteConnection::connect(&format!("sqlite://{}", self.database_path.display()))
            .await
            .unwrap()
    }
}

impl Drop for TestRelay {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn unused_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn sse_event(event: &Value) -> String {
    let event_type = event["type"].as_str().unwrap();
    format!("event: {event_type}\ndata: {event}\n\n")
}

fn usage() -> Value {
    json!({
        "input_tokens": 13,
        "input_tokens_details": {"cached_tokens": 3},
        "output_tokens": 8,
        "output_tokens_details": {"reasoning_tokens": 2},
        "total_tokens": 21
    })
}

fn completed_event(id: &str, output: Value) -> Value {
    json!({
        "type": "response.completed",
        "sequence_number": 9,
        "response": {
            "id": id,
            "object": "response",
            "created_at": 1_753_000_123,
            "status": "completed",
            "model": MODEL,
            "output": output,
            "usage": usage()
        }
    })
}

fn incomplete_event(id: &str, reason: &str, output: Value) -> Value {
    json!({
        "type": "response.incomplete",
        "sequence_number": 9,
        "response": {
            "id": id,
            "object": "response",
            "created_at": 1_753_000_123,
            "status": "incomplete",
            "incomplete_details": {"reason": reason},
            "model": MODEL,
            "output": output,
            "usage": usage()
        }
    })
}

fn text_output(text: &str) -> Value {
    json!([{
        "id": "msg_1",
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [{
            "type": "output_text",
            "text": text,
            "annotations": [],
            "logprobs": []
        }]
    }])
}

fn minimal_request() -> Value {
    json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "Hello"}]
    })
}

async fn response_json(response: ClientResponse) -> (StatusCode, Value) {
    let status = response.status();
    let body = response.text().await.unwrap();
    let json = serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("response was not JSON ({error}): {body}"));
    (status, json)
}

async fn assert_gateway_error(response: ClientResponse) {
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "body: {body}");
    assert!(body["error"].is_object(), "body: {body}");
}

#[tokio::test]
async fn accepts_only_the_documented_non_streaming_defaults() {
    let upstream = ScriptedUpstream::start(vec![
        UpstreamReply::events(vec![completed_event(
            "resp_omitted",
            text_output("omitted"),
        )]),
        UpstreamReply::events(vec![completed_event("resp_null", text_output("null"))]),
        UpstreamReply::events(vec![completed_event(
            "resp_explicit",
            text_output("explicit"),
        )]),
    ])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let requests = [
        minimal_request(),
        json!({
            "model": MODEL,
            "messages": [{"role": "user", "content": "Hello"}],
            "stream": null,
            "n": null
        }),
        json!({
            "model": MODEL,
            "messages": [{"role": "user", "content": "Hello"}],
            "stream": false,
            "n": 1
        }),
    ];

    for request in requests {
        let (status, body) = response_json(relay.post(&request).await).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["object"], "chat.completion");
    }

    let upstream_requests = upstream.requests().await;
    assert_eq!(upstream_requests.len(), 3);
    for request in upstream_requests {
        assert_eq!(request["model"], MODEL);
        assert_eq!(request["stream"], true);
        assert_eq!(request["store"], false);
        assert!(request.get("n").is_none());
    }
}

#[tokio::test]
async fn streams_chat_text_and_terminal_usage() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![
        json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": {
                "id": "resp_stream",
                "created_at": 1_753_000_123,
                "model": MODEL
            }
        }),
        json!({
            "type": "response.output_text.delta",
            "sequence_number": 1,
            "delta": "Hello"
        }),
        json!({
            "type": "response.output_text.delta",
            "sequence_number": 2,
            "delta": " world"
        }),
        completed_event("resp_stream", text_output("Hello world")),
    ])])
    .await;
    let relay = TestRelay::start(&upstream).await;
    let request = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "Hello"}],
        "max_completion_tokens": 1024,
        "stream": true,
        "stream_options": {"include_usage": true}
    });

    let response = relay.post(&request).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    let body = response.text().await.unwrap();
    assert!(body.ends_with("data: [DONE]\n\n"), "body: {body}");
    let chunks = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|data| *data != "[DONE]")
        .map(|data| serde_json::from_str::<Value>(data).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "Hello");
    assert_eq!(chunks[2]["choices"][0]["delta"]["content"], " world");
    assert_eq!(chunks[3]["choices"][0]["finish_reason"], "stop");
    assert!(chunks[3].get("usage").is_none());
    assert!(chunks[4]["choices"].as_array().unwrap().is_empty());
    assert_eq!(chunks[4]["usage"]["prompt_tokens"], 13);
    assert_eq!(chunks[4]["usage"]["completion_tokens"], 8);

    let sent = upstream.requests().await;
    assert_eq!(sent.len(), 1);
    assert!(sent[0].get("max_completion_tokens").is_none());
    assert!(sent[0].get("stream_options").is_none());
}

#[tokio::test]
async fn chat_completions_forwards_the_codex_header_allowlist() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![completed_event(
        "resp_headers",
        text_output("done"),
    )])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let response = relay
        .request(&minimal_request())
        .header("x-openai-subagent", "reviewer")
        .header("x-codex-installation-id", "must-not-pass")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let headers = upstream.state.latest_headers.lock().await;
    assert_eq!(headers.get("x-openai-subagent").unwrap(), "reviewer");
    assert!(!headers.contains_key("x-codex-installation-id"));
    assert_ne!(
        headers.get(header::AUTHORIZATION).unwrap(),
        format!("Bearer {API_KEY}").as_str()
    );
}

#[tokio::test]
async fn reasoning_effort_accepts_null_as_omitted_and_forwards_max() {
    let upstream = ScriptedUpstream::start(vec![
        UpstreamReply::events(vec![completed_event(
            "resp_null_reasoning",
            text_output("null"),
        )]),
        UpstreamReply::events(vec![completed_event(
            "resp_max_reasoning",
            text_output("max"),
        )]),
    ])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let mut omitted = minimal_request();
    omitted["reasoning_effort"] = Value::Null;
    let (status, body) = response_json(relay.post(&omitted).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let mut maximum = minimal_request();
    maximum["reasoning_effort"] = json!("max");
    let (status, body) = response_json(relay.post(&maximum).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let sent = upstream.requests().await;
    assert_eq!(sent.len(), 2);
    assert!(sent[0].get("reasoning").is_none());
    assert_eq!(sent[1]["reasoning"], json!({"effort": "max"}));
}

#[tokio::test]
async fn converts_all_supported_message_forms_tools_and_controls_in_order() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![completed_event(
        "resp_conversion",
        text_output("done"),
    )])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let request = json!({
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "system string"},
            {"role": "system", "content": [
                {"type": "text", "text": "system part one"},
                {"type": "text", "text": "system part two"}
            ]},
            {"role": "developer", "content": "developer string"},
            {"role": "developer", "content": [
                {"type": "text", "text": "developer part one"},
                {"type": "text", "text": "developer part two"}
            ]},
            {"role": "user", "content": "user string"},
            {"role": "user", "content": [{"type": "text", "text": "user part"}]},
            {"role": "assistant", "content": "assistant string"},
            {"role": "assistant", "content": [{"type": "text", "text": "assistant part"}]},
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [
                    {
                        "id": "call_weather",
                        "type": "function",
                        "function": {"name": "weather", "arguments": "{\"city\":\"Paris\"}"}
                    },
                    {
                        "id": "call_time",
                        "type": "function",
                        "function": {"name": "time", "arguments": "{\"zone\":\"UTC\"}"}
                    }
                ]
            },
            {"role": "tool", "tool_call_id": "call_weather", "content": "sunny"},
            {"role": "tool", "tool_call_id": "call_time", "content": "12:00"}
        ],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "weather",
                    "description": "Look up weather",
                    "parameters": {
                        "type": "object",
                        "properties": {"city": {"type": "string"}},
                        "required": ["city"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "time",
                    "parameters": {"type": "object", "properties": {}},
                    "strict": true
                }
            }
        ],
        "tool_choice": {"type": "function", "function": {"name": "weather"}},
        "parallel_tool_calls": true,
        "reasoning_effort": "low",
        "max_completion_tokens": 4096,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "answer",
                "schema": {
                    "type": "object",
                    "properties": {"answer": {"type": "string"}},
                    "required": ["answer"],
                    "additionalProperties": false
                },
                "strict": true
            }
        },
        "stream": false,
        "n": 1
    });

    let (status, body) = response_json(relay.post(&request).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let sent = upstream.requests().await;
    assert_eq!(sent.len(), 1);
    let sent = &sent[0];
    assert_eq!(sent["stream"], true);
    assert_eq!(sent["store"], false);
    assert_eq!(sent["parallel_tool_calls"], true);
    assert_eq!(sent["reasoning"], json!({"effort": "low"}));
    assert_eq!(
        sent["instructions"],
        "system string\nsystem part one\nsystem part two"
    );
    assert_eq!(
        sent["text"],
        json!({
            "format": {
                "type": "json_schema",
                "name": "answer",
                "schema": {
                    "type": "object",
                    "properties": {"answer": {"type": "string"}},
                    "required": ["answer"],
                    "additionalProperties": false
                },
                "strict": true
            }
        })
    );
    assert!(sent.get("max_completion_tokens").is_none());
    assert_eq!(
        sent["tool_choice"],
        json!({"type": "function", "name": "weather"})
    );
    assert_eq!(
        sent["tools"],
        json!([
            {
                "type": "function",
                "name": "weather",
                "description": "Look up weather",
                "parameters": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"]
                },
                "strict": false
            },
            {
                "type": "function",
                "name": "time",
                "parameters": {"type": "object", "properties": {}},
                "strict": true
            }
        ])
    );
    assert_eq!(
        sent["input"],
        json!([
            {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "developer string"}]},
            {"type": "message", "role": "developer", "content": [
                {"type": "input_text", "text": "developer part one"},
                {"type": "input_text", "text": "developer part two"}
            ]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "user string"}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "user part"}]},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "assistant string"}]},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "assistant part"}]},
            {"type": "function_call", "call_id": "call_weather", "name": "weather", "arguments": "{\"city\":\"Paris\"}"},
            {"type": "function_call", "call_id": "call_time", "name": "time", "arguments": "{\"zone\":\"UTC\"}"},
            {"type": "function_call_output", "call_id": "call_weather", "output": "sunny"},
            {"type": "function_call_output", "call_id": "call_time", "output": "12:00"}
        ])
    );
    for chat_only_field in ["messages", "reasoning_effort", "max_completion_tokens", "n"] {
        assert!(
            sent.get(chat_only_field).is_none(),
            "Chat-only field {chat_only_field} leaked upstream: {sent}"
        );
    }
}

#[tokio::test]
async fn maps_each_simple_tool_choice_to_the_responses_wire_shape() {
    let upstream = ScriptedUpstream::start(
        ["none", "auto", "required"]
            .into_iter()
            .map(|choice| {
                UpstreamReply::events(vec![completed_event(
                    &format!("resp_{choice}"),
                    text_output(choice),
                )])
            })
            .collect(),
    )
    .await;
    let relay = TestRelay::start(&upstream).await;

    for choice in ["none", "auto", "required"] {
        let request = json!({
            "model": MODEL,
            "messages": [{"role": "user", "content": "choose"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "lookup",
                    "parameters": {"type": "object", "properties": {}}
                }
            }],
            "tool_choice": choice
        });
        let (status, body) = response_json(relay.post(&request).await).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
    }

    let sent = upstream.requests().await;
    assert_eq!(sent.len(), 3);
    for (request, choice) in sent.iter().zip(["none", "auto", "required"]) {
        assert_eq!(request["tool_choice"], choice);
        assert_eq!(request["tools"][0]["strict"], false);
    }
}

#[tokio::test]
async fn function_tools_map_omitted_or_null_parameters_and_null_strictness() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![completed_event(
        "resp_nullable_tools",
        text_output("done"),
    )])])
    .await;
    let relay = TestRelay::start(&upstream).await;
    let request = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "Use a tool"}],
        "tools": [
            {"type": "function", "function": {"name": "without_schema"}},
            {
                "type": "function",
                "function": {
                    "name": "nullable_schema",
                    "parameters": null,
                    "strict": null
                }
            }
        ]
    });

    let (status, body) = response_json(relay.post(&request).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let sent = upstream.requests().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0]["tools"],
        json!([
            {
                "type": "function",
                "name": "without_schema",
                "parameters": null,
                "strict": false
            },
            {
                "type": "function",
                "name": "nullable_schema",
                "parameters": null,
                "strict": false
            }
        ])
    );
}

#[tokio::test]
async fn builds_the_chat_completion_only_from_the_terminal_full_response() {
    let terminal = completed_event(
        "resp_authoritative",
        json!([
            {
                "id": "msg_a",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "text": "Hello, ", "annotations": [], "logprobs": []},
                    {"type": "refusal", "refusal": "policy refusal"}
                ]
            },
            {"id": "reasoning_1", "type": "reasoning", "summary": [{"type": "summary_text", "text": "private chain"}]},
            {
                "id": "msg_b",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "world", "annotations": [], "logprobs": []}]
            }
        ]),
    );
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![
        json!({"type": "response.created", "sequence_number": 0, "response": {"id": "resp_wrong"}}),
        json!({"type": "response.output_text.delta", "sequence_number": 1, "delta": "WRONG DELTA"}),
        terminal,
    ])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let response = relay.post(&minimal_request()).await;
    assert!(
        response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["id"], "resp_authoritative");
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["created"], 1_753_000_123_i64);
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["choices"].as_array().unwrap().len(), 1);
    assert_eq!(body["choices"][0]["index"], 0);
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["choices"][0]["message"]["content"], "Hello, world");
    assert_eq!(body["choices"][0]["message"]["refusal"], "policy refusal");
    assert_eq!(body["usage"]["prompt_tokens"], 13);
    assert_eq!(body["usage"]["prompt_tokens_details"]["cached_tokens"], 3);
    assert_eq!(body["usage"]["completion_tokens"], 8);
    assert_eq!(
        body["usage"]["completion_tokens_details"]["reasoning_tokens"],
        2
    );
    assert_eq!(body["usage"]["total_tokens"], 21);
    let serialized = body.to_string();
    assert!(!serialized.contains("WRONG DELTA"));
    assert!(!serialized.contains("private chain"));
}

#[tokio::test]
async fn preserves_explicitly_empty_text_and_refusal_parts_as_empty_strings() {
    let terminal = completed_event(
        "resp_empty_parts",
        json!([{
            "id": "msg_empty",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [
                {"type": "output_text", "text": "", "annotations": [], "logprobs": []},
                {"type": "refusal", "refusal": ""}
            ]
        }]),
    );
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![terminal])]).await;
    let relay = TestRelay::start(&upstream).await;

    let (status, body) = response_json(relay.post(&minimal_request()).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let message = &body["choices"][0]["message"];
    assert_eq!(message["content"], "");
    assert_eq!(message["refusal"], "");
}

#[tokio::test]
async fn uses_completed_output_items_when_the_private_terminal_omits_output() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![
        json!({
            "type": "response.output_item.done",
            "sequence_number": 3,
            "item": {
                "id": "msg_private",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "private endpoint output",
                    "annotations": [],
                    "logprobs": []
                }]
            }
        }),
        completed_event("resp_private_terminal", json!([])),
    ])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let (status, body) = response_json(relay.post(&minimal_request()).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "private endpoint output"
    );
}

#[tokio::test]
async fn completed_items_do_not_mask_a_non_array_terminal_output() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![
        json!({
            "type": "response.output_item.done",
            "sequence_number": 3,
            "item": {
                "id": "msg_fallback",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "must not be used"}]
            }
        }),
        completed_event("resp_malformed_output", json!({"not": "an array"})),
    ])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    assert_gateway_error(relay.post(&minimal_request()).await).await;
    assert_eq!(upstream.request_count(), 1);
}

#[tokio::test]
async fn maps_responses_function_calls_to_ordered_chat_tool_calls() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![completed_event(
        "resp_tools",
        json!([
            {"id": "reasoning_1", "type": "reasoning", "summary": []},
            {
                "id": "fc_internal_a",
                "type": "function_call",
                "status": "completed",
                "call_id": "call_a",
                "name": "first",
                "arguments": "{\"value\":1}"
            },
            {
                "id": "fc_internal_b",
                "type": "function_call",
                "status": "completed",
                "call_id": "call_b",
                "name": "second",
                "arguments": "{\"value\":2}"
            }
        ]),
    )])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let (status, body) = response_json(relay.post(&minimal_request()).await).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["choices"][0]["finish_reason"], "tool_calls");
    let message = &body["choices"][0]["message"];
    assert_eq!(message["role"], "assistant");
    assert!(message.as_object().unwrap().contains_key("content"));
    assert!(message["content"].is_null());
    assert!(message["refusal"].is_null());
    assert_eq!(
        message["tool_calls"],
        json!([
            {
                "id": "call_a",
                "type": "function",
                "function": {"name": "first", "arguments": "{\"value\":1}"}
            },
            {
                "id": "call_b",
                "type": "function",
                "function": {"name": "second", "arguments": "{\"value\":2}"}
            }
        ])
    );
}

#[tokio::test]
async fn maps_supported_incomplete_reasons_to_chat_finish_reasons() {
    let upstream = ScriptedUpstream::start(vec![
        UpstreamReply::events(vec![incomplete_event(
            "resp_length",
            "max_output_tokens",
            text_output("truncated"),
        )]),
        UpstreamReply::events(vec![incomplete_event(
            "resp_filter",
            "content_filter",
            text_output("filtered"),
        )]),
    ])
    .await;
    let relay = TestRelay::start(&upstream).await;

    for expected in ["length", "content_filter"] {
        let (status, body) = response_json(relay.post(&minimal_request()).await).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["choices"][0]["finish_reason"], expected);
        assert_eq!(body["usage"]["total_tokens"], 21);
    }
}

#[tokio::test]
async fn does_not_send_partial_json_before_the_terminal_response() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::Sse(vec![
        SseChunk {
            delay: Duration::ZERO,
            data: sse_event(&json!({
                "type": "response.output_text.delta",
                "sequence_number": 1,
                "delta": "partial text"
            })),
        },
        SseChunk {
            delay: Duration::from_millis(800),
            data: sse_event(&completed_event(
                "resp_after_wait",
                text_output("terminal text"),
            )),
        },
    ])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let response = relay.request(&minimal_request()).send();
    tokio::pin!(response);
    assert!(
        timeout(Duration::from_millis(150), &mut response)
            .await
            .is_err(),
        "the downstream HTTP response began before the upstream terminal event"
    );
    assert_eq!(upstream.request_count(), 1);

    let response = timeout(Duration::from_secs(3), &mut response)
        .await
        .expect("buffered Chat Completion never arrived")
        .unwrap();
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["choices"][0]["message"]["content"], "terminal text");
    assert!(!body.to_string().contains("partial text"));
}

#[tokio::test]
async fn downstream_disconnect_during_chat_aggregation_marks_the_request_canceled() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::Sse(vec![
        SseChunk {
            delay: Duration::ZERO,
            data: sse_event(&json!({
                "type": "response.output_text.delta",
                "sequence_number": 1,
                "delta": "started"
            })),
        },
        SseChunk {
            delay: Duration::from_secs(5),
            data: sse_event(&completed_event(
                "resp_after_disconnect",
                text_output("too late"),
            )),
        },
    ])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let client = relay.client.clone();
    let url = format!("{}/v1/chat/completions", relay.base_url);
    let request = minimal_request();
    let downstream = tokio::spawn(async move {
        client
            .post(url)
            .bearer_auth(API_KEY)
            .json(&request)
            .send()
            .await
    });
    timeout(Duration::from_secs(2), async {
        while upstream.request_count() == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Chat request never reached upstream");
    downstream.abort();
    let _ = downstream.await;

    let mut database = relay.database().await;
    let status = timeout(Duration::from_secs(2), async {
        loop {
            let status = sqlx::query("SELECT status FROM request_logs ORDER BY id DESC LIMIT 1")
                .fetch_optional(&mut database)
                .await
                .unwrap()
                .map(|row| row.get::<String, _>("status"));
            if status.as_deref() == Some("canceled") {
                break status.unwrap();
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("request log did not become canceled");
    assert_eq!(status, "canceled");
}

#[tokio::test]
async fn downstream_disconnect_after_chat_terminal_arrives_preserves_usage_and_completion() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::Sse(vec![SseChunk {
        delay: Duration::from_secs(1),
        data: sse_event(&completed_event(
            "resp_blocked_accounting",
            text_output("complete"),
        )),
    }])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let client = relay.client.clone();
    let url = format!("{}/v1/chat/completions", relay.base_url);
    let request = minimal_request();
    let downstream = tokio::spawn(async move {
        client
            .post(url)
            .bearer_auth(API_KEY)
            .json(&request)
            .send()
            .await
    });
    timeout(Duration::from_secs(2), async {
        while upstream.request_count() == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Chat request never reached upstream");

    let mut database = relay.database().await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut database)
        .await
        .expect("hold the terminal accounting write");
    sleep(Duration::from_millis(1_200)).await;
    assert!(
        !downstream.is_finished(),
        "Chat response finished while terminal accounting was write-locked"
    );

    downstream.abort();
    let _ = downstream.await;
    sleep(Duration::from_millis(100)).await;
    sqlx::query("COMMIT")
        .execute(&mut database)
        .await
        .expect("release the terminal accounting write");

    let row = timeout(Duration::from_secs(2), async {
        loop {
            let row = sqlx::query(
                "SELECT status, http_status, input_tokens, cached_input_tokens, output_tokens, \
                 cost_usd FROM request_logs ORDER BY id DESC LIMIT 1",
            )
            .fetch_optional(&mut database)
            .await
            .unwrap();
            if row
                .as_ref()
                .is_some_and(|row| row.get::<String, _>("status") != "started")
            {
                break row.unwrap();
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("blocked terminal request log was not finalized");
    assert_eq!(row.get::<String, _>("status"), "completed");
    assert_eq!(row.get::<i64, _>("http_status"), 200);
    assert_eq!(row.get::<i64, _>("input_tokens"), 13);
    assert_eq!(row.get::<i64, _>("cached_input_tokens"), 3);
    assert_eq!(row.get::<i64, _>("output_tokens"), 8);
    assert_eq!(row.get::<String, _>("cost_usd"), "0.000058300");
}

#[tokio::test]
async fn downstream_disconnect_after_chat_eof_preserves_upstream_error() {
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::Sse(vec![SseChunk {
        delay: Duration::from_secs(1),
        data: sse_event(&json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": {"id": "resp_without_terminal", "status": "in_progress"}
        })),
    }])])
    .await;
    let relay = TestRelay::start(&upstream).await;

    let client = relay.client.clone();
    let url = format!("{}/v1/chat/completions", relay.base_url);
    let request = minimal_request();
    let downstream = tokio::spawn(async move {
        client
            .post(url)
            .bearer_auth(API_KEY)
            .json(&request)
            .send()
            .await
    });
    timeout(Duration::from_secs(2), async {
        while upstream.request_count() == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Chat request never reached upstream");

    let mut database = relay.database().await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut database)
        .await
        .expect("hold the EOF accounting write");
    sleep(Duration::from_millis(1_200)).await;
    assert!(
        !downstream.is_finished(),
        "Chat error response finished while EOF accounting was write-locked"
    );

    downstream.abort();
    let _ = downstream.await;
    sleep(Duration::from_millis(100)).await;
    sqlx::query("COMMIT")
        .execute(&mut database)
        .await
        .expect("release the EOF accounting write");

    let row = timeout(Duration::from_secs(2), async {
        loop {
            let row = sqlx::query(
                "SELECT status, http_status, input_tokens, cached_input_tokens, output_tokens, \
                 cost_usd FROM request_logs ORDER BY id DESC LIMIT 1",
            )
            .fetch_optional(&mut database)
            .await
            .unwrap();
            if row
                .as_ref()
                .is_some_and(|row| row.get::<String, _>("status") != "started")
            {
                break row.unwrap();
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("blocked EOF request log was not finalized");
    assert_eq!(row.get::<String, _>("status"), "upstream_error");
    assert_eq!(row.get::<i64, _>("http_status"), 502);
    assert_eq!(row.get::<Option<i64>, _>("input_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("cached_input_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("output_tokens"), None);
    assert_eq!(row.get::<Option<String>, _>("cost_usd"), None);
}

#[tokio::test]
async fn unaccountable_chat_terminal_returns_and_records_bad_gateway() {
    let unaccountable_tokens = i64::MAX as u64 + 1;
    let mut terminal = completed_event("resp_unaccountable", text_output("complete"));
    terminal["response"]["usage"] = json!({
        "input_tokens": unaccountable_tokens,
        "input_tokens_details": {"cached_tokens": 0},
        "output_tokens": 0,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": unaccountable_tokens
    });
    let upstream = ScriptedUpstream::start(vec![UpstreamReply::events(vec![terminal])]).await;
    let relay = TestRelay::start(&upstream).await;

    let (status, body) = response_json(relay.post(&minimal_request()).await).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "body: {body}");

    let mut database = relay.database().await;
    let row = sqlx::query(
        "SELECT status, http_status, input_tokens, cached_input_tokens, output_tokens, cost_usd \
         FROM request_logs ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&mut database)
    .await
    .expect("read unaccountable Chat request log");
    assert_eq!(row.get::<String, _>("status"), "upstream_error");
    assert_eq!(row.get::<i64, _>("http_status"), 502);
    assert_eq!(row.get::<Option<i64>, _>("input_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("cached_input_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("output_tokens"), None);
    assert_eq!(row.get::<Option<String>, _>("cost_usd"), None);
}

#[tokio::test]
async fn turns_upstream_http_errors_error_events_and_failed_responses_into_gateway_errors() {
    let failed = json!({
        "type": "response.failed",
        "sequence_number": 4,
        "response": {
            "id": "resp_failed",
            "object": "response",
            "created_at": 1_753_000_123,
            "status": "failed",
            "model": MODEL,
            "output": [],
            "error": {"code": "server_error", "message": "upstream failed"},
            "usage": usage()
        }
    });
    let upstream = ScriptedUpstream::start(vec![
        UpstreamReply::Json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": {"type": "server_error", "message": "unavailable"}}),
        ),
        UpstreamReply::events(vec![json!({
            "type": "error",
            "sequence_number": 1,
            "error": {"type": "server_error", "code": "server_error", "message": "boom", "param": null}
        })]),
        UpstreamReply::events(vec![failed]),
    ])
    .await;
    let relay = TestRelay::start(&upstream).await;

    for _ in 0..3 {
        assert_gateway_error(relay.post(&minimal_request()).await).await;
    }
    assert_eq!(upstream.request_count(), 3);
}

#[tokio::test]
async fn rejects_missing_malformed_or_unusable_terminal_responses() {
    let mut missing_usage = completed_event("resp_no_usage", text_output("not billable"));
    missing_usage["response"]
        .as_object_mut()
        .unwrap()
        .remove("usage");
    let unknown_incomplete = incomplete_event(
        "resp_unknown_incomplete",
        "mystery_reason",
        text_output("unknown"),
    );
    let upstream = ScriptedUpstream::start(vec![
        UpstreamReply::events(vec![json!({
            "type": "response.output_text.delta",
            "sequence_number": 1,
            "delta": "EOF follows"
        })]),
        UpstreamReply::raw("event: response.completed\ndata: {not-json}\n\n"),
        UpstreamReply::events(vec![json!({
            "type": "response.completed",
            "sequence_number": 2,
            "response": {"status": "completed"}
        })]),
        UpstreamReply::events(vec![missing_usage]),
        UpstreamReply::events(vec![unknown_incomplete]),
    ])
    .await;
    let relay = TestRelay::start(&upstream).await;

    for _ in 0..5 {
        assert_gateway_error(relay.post(&minimal_request()).await).await;
    }
    assert_eq!(upstream.request_count(), 5);
}

#[tokio::test]
async fn rejects_hosted_custom_program_and_shell_output_items() {
    let outputs = [
        json!([{
            "id": "ws_1",
            "type": "web_search_call",
            "status": "completed",
            "action": {"type": "search", "query": "example"}
        }]),
        json!([{
            "id": "ct_1",
            "type": "custom_tool_call",
            "status": "completed",
            "call_id": "call_custom",
            "name": "custom",
            "input": "payload"
        }]),
        json!([{
            "id": "pc_1",
            "type": "computer_call",
            "status": "completed",
            "call_id": "call_program",
            "action": {"type": "click", "x": 1, "y": 2}
        }]),
        json!([{
            "id": "sh_1",
            "type": "local_shell_call",
            "status": "completed",
            "call_id": "call_shell",
            "action": {"type": "exec", "command": ["true"]}
        }]),
    ];
    let upstream = ScriptedUpstream::start(
        outputs
            .into_iter()
            .enumerate()
            .map(|(index, output)| {
                UpstreamReply::events(vec![completed_event(
                    &format!("resp_unsupported_{index}"),
                    output,
                )])
            })
            .collect(),
    )
    .await;
    let relay = TestRelay::start(&upstream).await;

    for _ in 0..4 {
        assert_gateway_error(relay.post(&minimal_request()).await).await;
    }
    assert_eq!(upstream.request_count(), 4);
}

#[tokio::test]
async fn rejected_chat_requests_retain_valid_reasoning_effort_in_the_ledger() {
    let upstream = ScriptedUpstream::start(Vec::new()).await;
    let relay = TestRelay::start(&upstream).await;
    let request = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "Hello"}],
        "reasoning_effort": "high",
        "temperature": 0.2
    });

    let (status, body) = response_json(relay.post(&request).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(upstream.request_count(), 0);

    let mut database = relay.database().await;
    let row =
        sqlx::query("SELECT reasoning_effort, status FROM request_logs ORDER BY id DESC LIMIT 1")
            .fetch_one(&mut database)
            .await
            .unwrap();
    assert_eq!(row.get::<String, _>("reasoning_effort"), "high");
    assert_eq!(row.get::<String, _>("status"), "rejected");
}

#[tokio::test]
async fn rejects_empty_text_part_arrays_before_accessing_upstream() {
    let upstream = ScriptedUpstream::start(Vec::new()).await;
    let relay = TestRelay::start(&upstream).await;
    let cases = [
        json!({
            "model": MODEL,
            "messages": [{"role": "user", "content": []}]
        }),
        json!({
            "model": MODEL,
            "messages": [{"role": "assistant", "content": []}]
        }),
        json!({
            "model": MODEL,
            "messages": [
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_empty",
                        "type": "function",
                        "function": {"name": "empty", "arguments": "{}"}
                    }]
                },
                {"role": "tool", "tool_call_id": "call_empty", "content": []}
            ]
        }),
    ];

    for request in cases {
        let (status, body) = response_json(relay.post(&request).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        assert_eq!(body["error"]["type"], "invalid_request_error");
    }
    assert_eq!(upstream.request_count(), 0);
}

#[tokio::test]
async fn rejects_every_unsupported_chat_category_before_accessing_upstream() {
    let upstream = ScriptedUpstream::start(Vec::new()).await;
    let relay = TestRelay::start(&upstream).await;

    let cases = vec![
        ("zero choices", json!({"n": 0})),
        ("multiple choices", json!({"n": 2})),
        (
            "image content",
            json!({"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "https://example.test/image.png"}}]}]}),
        ),
        (
            "input audio content",
            json!({"messages": [{"role": "user", "content": [{"type": "input_audio", "input_audio": {"data": "AA==", "format": "wav"}}]}]}),
        ),
        (
            "file content",
            json!({"messages": [{"role": "user", "content": [{"type": "file", "file": {"file_id": "file_1"}}]}]}),
        ),
        ("verbosity", json!({"verbosity": "low"})),
        (
            "legacy functions",
            json!({"functions": [{"name": "legacy", "parameters": {"type": "object"}}]}),
        ),
        ("legacy function choice", json!({"function_call": "auto"})),
        (
            "legacy function role",
            json!({"messages": [{"role": "function", "name": "legacy", "content": "result"}]}),
        ),
        (
            "custom tool",
            json!({"tools": [{"type": "custom", "custom": {"name": "custom"}}]}),
        ),
        (
            "allowed tools extension",
            json!({"tool_choice": {"type": "allowed_tools", "mode": "auto", "tools": [{"type": "function", "name": "lookup"}]}}),
        ),
        ("deprecated max tokens", json!({"max_tokens": 50})),
        (
            "unsupported Responses output token limit",
            json!({"max_output_tokens": 50}),
        ),
        ("stop sequences", json!({"stop": ["END"]})),
        ("log probabilities", json!({"logprobs": true})),
        ("top log probabilities", json!({"top_logprobs": 3})),
        ("logit bias", json!({"logit_bias": {"123": 1}})),
        ("presence penalty", json!({"presence_penalty": 0.5})),
        ("frequency penalty", json!({"frequency_penalty": 0.5})),
        ("seed", json!({"seed": 42})),
        (
            "prediction",
            json!({"prediction": {"type": "content", "content": "expected"}}),
        ),
        (
            "web search options",
            json!({"web_search_options": {"search_context_size": "low"}}),
        ),
        ("temperature", json!({"temperature": 0.2})),
        ("top p", json!({"top_p": 0.9})),
        ("modalities", json!({"modalities": ["text", "audio"]})),
        (
            "audio output controls",
            json!({"audio": {"voice": "alloy", "format": "wav"}}),
        ),
        (
            "stream options",
            json!({"stream_options": {"include_usage": true}}),
        ),
        ("storage", json!({"store": true})),
        ("metadata", json!({"metadata": {"trace": "value"}})),
        ("service tier", json!({"service_tier": "default"})),
        ("end-user identifier", json!({"user": "user-1"})),
        (
            "unknown top-level field",
            json!({"future_option_that_must_not_be_ignored": true}),
        ),
        (
            "unknown message field",
            json!({"messages": [{"role": "user", "content": "Hello", "name": "not-supported"}]}),
        ),
        (
            "non-function assistant tool call",
            json!({"messages": [{"role": "assistant", "content": null, "tool_calls": [{"id": "call_1", "type": "custom", "custom": {"name": "custom"}}]}]}),
        ),
    ];

    for (name, patch) in cases {
        let mut request = minimal_request();
        for (key, value) in patch.as_object().unwrap() {
            request[key] = value.clone();
        }
        let (status, body) = response_json(relay.post(&request).await).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{name} was not rejected: {body}"
        );
        assert_eq!(
            body["error"]["type"], "invalid_request_error",
            "{name} did not return an OpenAI-style validation error: {body}"
        );
        assert_eq!(
            upstream.request_count(),
            0,
            "{name} reached the upstream server"
        );
    }
}
