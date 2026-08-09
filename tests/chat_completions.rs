use std::{
    collections::VecDeque,
    convert::Infallible,
    net::{SocketAddr, TcpListener as StdTcpListener},
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
    http::{StatusCode, header},
    response::Response,
    routing::post,
};
use reqwest::{Client, Response as ClientResponse};
use serde_json::{Value, json};
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

async fn upstream_handler(State(state): State<Arc<UpstreamState>>, body: Bytes) -> Response<Body> {
    state.request_count.fetch_add(1, Ordering::SeqCst);
    let request = serde_json::from_slice(&body).unwrap_or_else(|_| Value::Null);
    state.requests.lock().await.push(request);

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
            {"role": "system", "content": [{"type": "text", "text": "system part"}]},
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
        "max_completion_tokens": 321,
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
    assert_eq!(sent["max_output_tokens"], 321);
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
            {"type": "message", "role": "system", "content": [{"type": "input_text", "text": "system string"}]},
            {"type": "message", "role": "system", "content": [{"type": "input_text", "text": "system part"}]},
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
async fn rejects_every_unsupported_chat_category_before_accessing_upstream() {
    let upstream = ScriptedUpstream::start(Vec::new()).await;
    let relay = TestRelay::start(&upstream).await;

    let cases = vec![
        ("streaming", json!({"stream": true})),
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
        (
            "structured output",
            json!({"response_format": {"type": "json_object"}}),
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
