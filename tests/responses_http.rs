use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_stream::stream;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use bytes::Bytes;
use eventsource_stream::{Event, Eventsource};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_stream::iter;

const DOWNSTREAM_KEY: &str = "sk-downstream-test";
const UPSTREAM_ACCESS_TOKEN: &str = "upstream-access-token";
const ACCOUNT_ID: &str = "account-test";
const MODEL: &str = "gpt-test";

#[derive(Debug)]
struct ScriptChunk {
    delay: Duration,
    bytes: Bytes,
}

impl ScriptChunk {
    fn immediate(bytes: impl Into<Bytes>) -> Self {
        Self {
            delay: Duration::ZERO,
            bytes: bytes.into(),
        }
    }

    fn after(delay: Duration, bytes: impl Into<Bytes>) -> Self {
        Self {
            delay,
            bytes: bytes.into(),
        }
    }
}

#[derive(Debug)]
struct ScriptedResponse {
    status: StatusCode,
    content_type: &'static str,
    chunks: Vec<ScriptChunk>,
    hold_open: bool,
    dropped: Option<Arc<Notify>>,
}

impl ScriptedResponse {
    fn sse(chunks: Vec<ScriptChunk>) -> Self {
        Self {
            status: StatusCode::OK,
            content_type: "text/event-stream",
            chunks,
            hold_open: false,
            dropped: None,
        }
    }

    fn json(status: StatusCode, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            chunks: vec![ScriptChunk::immediate(value.to_string())],
            hold_open: false,
            dropped: None,
        }
    }
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    headers: HeaderMap,
    body: Value,
}

#[derive(Clone)]
struct FakeUpstreamState {
    responses: Arc<Mutex<VecDeque<ScriptedResponse>>>,
    captured: mpsc::UnboundedSender<CapturedRequest>,
    contacts: Arc<AtomicUsize>,
}

struct NotifyOnDrop(Option<Arc<Notify>>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        if let Some(notify) = self.0.take() {
            notify.notify_one();
        }
    }
}

struct FakeUpstream {
    addr: SocketAddr,
    captured: mpsc::UnboundedReceiver<CapturedRequest>,
    contacts: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start(responses: Vec<ScriptedResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let addr = listener.local_addr().expect("fake upstream address");
        let (captured_tx, captured_rx) = mpsc::unbounded_channel();
        let contacts = Arc::new(AtomicUsize::new(0));
        let state = FakeUpstreamState {
            responses: Arc::new(Mutex::new(responses.into())),
            captured: captured_tx,
            contacts: Arc::clone(&contacts),
        };

        let app = Router::new()
            .route("/responses", post(fake_responses))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("fake upstream server failed");
        });

        Self {
            addr,
            captured: captured_rx,
            contacts,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn contacts(&self) -> usize {
        self.contacts.load(Ordering::SeqCst)
    }

    async fn next_request(&mut self) -> CapturedRequest {
        timeout(Duration::from_secs(2), self.captured.recv())
            .await
            .expect("relay did not contact fake upstream in time")
            .expect("fake upstream capture channel closed")
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake_responses(
    State(state): State<FakeUpstreamState>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<Value>,
) -> Response<Body> {
    state.contacts.fetch_add(1, Ordering::SeqCst);
    state
        .captured
        .send(CapturedRequest { headers, body })
        .expect("test still owns request capture receiver");

    let response = state
        .responses
        .lock()
        .await
        .pop_front()
        .expect("fake upstream response script exhausted");
    let status = response.status;
    let content_type = response.content_type;
    let body = stream! {
        let _notify_on_drop = NotifyOnDrop(response.dropped);
        for chunk in response.chunks {
            if !chunk.delay.is_zero() {
                sleep(chunk.delay).await;
            }
            yield Ok::<Bytes, Infallible>(chunk.bytes);
        }
        if response.hold_open {
            std::future::pending::<()>().await;
        }
    };

    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from_stream(body))
        .expect("build fake upstream response")
}

struct Relay {
    addr: SocketAddr,
    child: Child,
    _temp_dir: TempDir,
}

impl Relay {
    async fn start(upstream_base_url: &str) -> Self {
        let temp_dir = tempfile::tempdir().expect("create relay test directory");
        let auth_path = temp_dir.path().join("auth.json");
        let state_path = temp_dir.path().join("state.sqlite3");
        let config_path = temp_dir.path().join("config.toml");

        std::fs::write(
            &auth_path,
            serde_json::to_vec_pretty(&json!({
                "OPENAI_API_KEY": null,
                "tokens": {
                    "id_token": "test-id-token",
                    "access_token": UPSTREAM_ACCESS_TOKEN,
                    "refresh_token": "test-refresh-token",
                    "account_id": ACCOUNT_ID
                },
                "last_refresh": "2099-01-01T00:00:00Z"
            }))
            .expect("serialize auth seed"),
        )
        .expect("write auth seed");

        let reservation =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve downstream port");
        let addr = reservation.local_addr().expect("reserved address");
        let config = format!(
            r#"[server]
listen = "{addr}"
enable_websockets = false

[state]
path = "{}"

[upstream]
base_url = "{upstream_base_url}"
oauth_token_url = "{upstream_base_url}/oauth/token"
auth_file = "{}"
supports_websockets = false

[[api_keys]]
id = "test-client"
secret = "{DOWNSTREAM_KEY}"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
            state_path.display(),
            auth_path.display(),
        );
        std::fs::write(&config_path, config).expect("write relay configuration");

        drop(reservation);
        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn codex-api");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().expect("poll codex-api") {
                panic!("codex-api exited before listening with {status}");
            }
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "codex-api did not start in time");
            sleep(Duration::from_millis(20)).await;
        }

        Self {
            addr,
            child,
            _temp_dir: temp_dir,
        }
    }

    fn responses_url(&self) -> String {
        format!("http://{}/v1/responses", self.addr)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build downstream client")
}

fn authorized_request(client: &reqwest::Client, relay: &Relay) -> reqwest::RequestBuilder {
    client
        .post(relay.responses_url())
        .bearer_auth(DOWNSTREAM_KEY)
}

async fn parse_sse_bytes(bytes: Bytes) -> Vec<Event> {
    let source = iter([Ok::<Bytes, Infallible>(bytes)]).eventsource();
    let mut source = Box::pin(source);
    let mut events = Vec::new();
    while let Some(event) = source.next().await {
        events.push(event.expect("valid downstream SSE framing"));
    }
    events
}

fn completed_sse() -> String {
    let completed = json!({
        "type": "response.completed",
        "sequence_number": 2,
        "response": {
            "id": "resp_test",
            "object": "response",
            "created_at": 1_786_233_600,
            "status": "completed",
            "model": MODEL,
            "output": [],
            "usage": {
                "input_tokens": 11,
                "input_tokens_details": {"cached_tokens": 3},
                "output_tokens": 5,
                "output_tokens_details": {"reasoning_tokens": 2},
                "total_tokens": 16
            }
        }
    });
    format!("event: response.completed\ndata: {completed}\n\n")
}

#[tokio::test]
async fn responses_requires_stream_true_before_contacting_upstream() {
    let upstream = FakeUpstream::start(Vec::new()).await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    for body in [
        json!({"model": MODEL, "input": "hello"}),
        json!({"model": MODEL, "input": "hello", "stream": null}),
        json!({"model": MODEL, "input": "hello", "stream": false}),
    ] {
        let response = authorized_request(&client, &relay)
            .json(&body)
            .send()
            .await
            .expect("send invalid stream request");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: Value = response.json().await.expect("OpenAI error JSON");
        assert_eq!(error["error"]["type"], "invalid_request_error");
        assert_eq!(error["error"]["param"], "stream");
    }

    assert_eq!(upstream.contacts(), 0);
}

#[tokio::test]
async fn responses_rejects_storage_and_background_modes_before_upstream() {
    let upstream = FakeUpstream::start(Vec::new()).await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    for (field, body) in [
        (
            "store",
            json!({"model": MODEL, "input": "hello", "stream": true, "store": true}),
        ),
        (
            "background",
            json!({"model": MODEL, "input": "hello", "stream": true, "background": true}),
        ),
    ] {
        let response = authorized_request(&client, &relay)
            .json(&body)
            .send()
            .await
            .expect("send unsupported mode request");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: Value = response.json().await.expect("OpenAI error JSON");
        assert_eq!(error["error"]["type"], "invalid_request_error");
        assert_eq!(error["error"]["param"], field);
    }

    assert_eq!(upstream.contacts(), 0);
}

#[tokio::test]
async fn responses_sends_subscription_headers_and_normalized_body_upstream() {
    let mut upstream =
        FakeUpstream::start(vec![ScriptedResponse::sse(vec![ScriptChunk::immediate(
            completed_sse(),
        )])])
        .await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();
    let downstream_body = json!({
        "model": MODEL,
        "input": [{
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }],
        "instructions": "Be concise",
        "stream": true,
        "reasoning": {"effort": "low", "summary": "auto"},
        "tools": [{
            "type": "function",
            "name": "weather",
            "description": "Get weather",
            "parameters": {"type": "object", "properties": {}}
        }],
        "parallel_tool_calls": true,
        "max_output_tokens": 64,
        "previous_response_id": "resp_previous",
        "prompt_cache_key": "cache-test",
        "include": ["reasoning.encrypted_content"]
    });

    let response = authorized_request(&client, &relay)
        .json(&downstream_body)
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.expect("consume downstream stream");

    let captured = upstream.next_request().await;
    assert_eq!(
        captured
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer upstream-access-token")
    );
    assert_ne!(
        captured
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer sk-downstream-test")
    );
    assert_eq!(
        captured
            .headers
            .get("ChatGPT-Account-ID")
            .and_then(|value| value.to_str().ok()),
        Some(ACCOUNT_ID)
    );
    assert_eq!(
        captured
            .headers
            .get("originator")
            .and_then(|value| value.to_str().ok()),
        Some("codex_cli_rs")
    );
    assert!(
        captured
            .headers
            .get(USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("codex_cli_rs/0.147.0")),
        "upstream User-Agent should identify the baseline Codex client"
    );
    assert_eq!(
        captured
            .headers
            .get(ACCEPT)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    assert_eq!(
        captured
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );

    let mut expected = downstream_body;
    expected["store"] = json!(false);
    assert_eq!(captured.body, expected);
}

#[tokio::test]
async fn responses_delivers_the_first_event_before_upstream_finishes() {
    let created = json!({
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_slow", "status": "in_progress"}
    });
    let upstream = FakeUpstream::start(vec![ScriptedResponse::sse(vec![
        ScriptChunk::immediate(format!("event: response.created\ndata: {created}\n\n")),
        ScriptChunk::after(Duration::from_millis(1_200), completed_sse()),
    ])])
    .await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = timeout(
        Duration::from_millis(600),
        authorized_request(&client, &relay)
            .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
            .send(),
    )
    .await
    .expect("relay buffered the upstream stream before sending headers")
    .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );

    let mut events = Box::pin(response.bytes_stream().eventsource());
    let first = timeout(Duration::from_millis(600), events.next())
        .await
        .expect("first SSE event was buffered")
        .expect("stream ended before first event")
        .expect("first event framing");
    assert_eq!(first.event, "response.created");
    assert_eq!(serde_json::from_str::<Value>(&first.data).unwrap(), created);

    assert!(
        timeout(Duration::from_millis(300), events.next())
            .await
            .is_err(),
        "terminal event arrived before its scripted upstream delay"
    );
    let terminal = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("terminal event did not arrive")
        .expect("stream ended before terminal event")
        .expect("terminal event framing");
    assert_eq!(terminal.event, "response.completed");
}

#[tokio::test]
async fn responses_preserves_sse_semantics_across_arbitrary_chunk_boundaries() {
    let terminal = completed_sse();
    let chunks = vec![
        ScriptChunk::immediate("id: cre"),
        ScriptChunk::immediate("ated-1\r\nevent: response.cre"),
        ScriptChunk::immediate("ated\r\ndata: {\"type\":\"response.created\",\"sequence_"),
        ScriptChunk::immediate("number\":0}\r"),
        ScriptChunk::immediate("\n\r\nid: extension-9\nevent: codex.rate_limit.updated\nda"),
        ScriptChunk::immediate("ta: {\"type\":\"codex.rate_limit.updated\",\"remaining\":7}\n\n"),
        ScriptChunk::immediate(Bytes::copy_from_slice(&terminal.as_bytes()[..17])),
        ScriptChunk::immediate(Bytes::copy_from_slice(&terminal.as_bytes()[17..49])),
        ScriptChunk::immediate(Bytes::copy_from_slice(&terminal.as_bytes()[49..])),
    ];
    let upstream = FakeUpstream::start(vec![ScriptedResponse::sse(chunks)]).await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = authorized_request(&client, &relay)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.bytes().await.expect("read downstream SSE");
    let raw = std::str::from_utf8(&bytes).expect("downstream SSE is UTF-8");
    assert!(!raw.contains("[DONE]"));
    assert!(
        !raw.contains("\r\n"),
        "relay should emit canonical LF framing"
    );

    let events = parse_sse_bytes(bytes).await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].id, "created-1");
    assert_eq!(events[0].event, "response.created");
    assert_eq!(
        serde_json::from_str::<Value>(&events[0].data).unwrap(),
        json!({"type": "response.created", "sequence_number": 0})
    );
    assert_eq!(events[1].id, "extension-9");
    assert_eq!(events[1].event, "codex.rate_limit.updated");
    assert_eq!(
        serde_json::from_str::<Value>(&events[1].data).unwrap(),
        json!({"type": "codex.rate_limit.updated", "remaining": 7})
    );
    assert_eq!(events[2].event, "response.completed");
    let terminal: Value = serde_json::from_str(&events[2].data).unwrap();
    assert_eq!(terminal["response"]["usage"]["input_tokens"], 11);
    assert_eq!(
        terminal["response"]["usage"]["input_tokens_details"]["cached_tokens"],
        3
    );
    assert_eq!(terminal["response"]["usage"]["output_tokens"], 5);
}

#[tokio::test]
async fn responses_forwards_each_usage_bearing_terminal_event_type() {
    let terminals = [
        ("response.completed", "completed"),
        ("response.incomplete", "incomplete"),
        ("response.failed", "failed"),
    ];
    let scripts = terminals
        .iter()
        .map(|(event, status)| {
            let data = json!({
                "type": event,
                "sequence_number": 1,
                "response": {
                    "id": format!("resp_{status}"),
                    "status": status,
                    "model": MODEL,
                    "output": [],
                    "usage": {
                        "input_tokens": 2,
                        "input_tokens_details": {"cached_tokens": 1},
                        "output_tokens": 3,
                        "total_tokens": 5
                    }
                }
            });
            ScriptedResponse::sse(vec![ScriptChunk::immediate(format!(
                "event: {event}\ndata: {data}\n\n"
            ))])
        })
        .collect();
    let upstream = FakeUpstream::start(scripts).await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    for (expected_event, expected_status) in terminals {
        let response = authorized_request(&client, &relay)
            .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
            .send()
            .await
            .expect("send terminal event request");
        assert_eq!(response.status(), StatusCode::OK);
        let events = parse_sse_bytes(response.bytes().await.unwrap()).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, expected_event);
        let data: Value = serde_json::from_str(&events[0].data).unwrap();
        assert_eq!(data["response"]["status"], expected_status);
        assert_eq!(data["response"]["usage"]["total_tokens"], 5);
    }
}

#[tokio::test]
async fn responses_preserves_an_upstream_error_before_streaming_starts() {
    let upstream_error = json!({
        "error": {
            "message": "upstream rate limit",
            "type": "requests",
            "param": null,
            "code": "rate_limit_exceeded"
        }
    });
    let upstream = FakeUpstream::start(vec![ScriptedResponse::json(
        StatusCode::TOO_MANY_REQUESTS,
        upstream_error.clone(),
    )])
    .await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = authorized_request(&client, &relay)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    );
    assert_eq!(
        response.json::<Value>().await.expect("upstream error JSON"),
        upstream_error
    );
}

#[tokio::test]
async fn responses_suppresses_a_malformed_terminal_event() {
    let created = json!({"type": "response.created", "sequence_number": 0});
    let upstream = FakeUpstream::start(vec![ScriptedResponse::sse(vec![
        ScriptChunk::immediate(format!("event: response.created\ndata: {created}\n\n")),
        ScriptChunk::immediate("event: response.completed\ndata: {not-json}\n\n"),
    ])])
    .await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = authorized_request(&client, &relay)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response
        .bytes()
        .await
        .expect("stream closes after bad terminal");
    assert!(
        !String::from_utf8_lossy(&bytes).contains("not-json"),
        "a malformed terminal must not be presented as a successful terminal"
    );
    let events = parse_sse_bytes(bytes).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, "response.created");
}

#[tokio::test]
async fn responses_clean_eof_without_terminal_does_not_fabricate_completion() {
    let delta = json!({
        "type": "response.output_text.delta",
        "sequence_number": 1,
        "item_id": "msg_1",
        "output_index": 0,
        "content_index": 0,
        "delta": "partial"
    });
    let upstream = FakeUpstream::start(vec![ScriptedResponse::sse(vec![ScriptChunk::immediate(
        format!("event: response.output_text.delta\ndata: {delta}\n\n"),
    )])])
    .await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = authorized_request(&client, &relay)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.bytes().await.expect("stream ends at upstream EOF");
    let raw = String::from_utf8_lossy(&bytes);
    assert!(!raw.contains("response.completed"));
    assert!(!raw.contains("response.incomplete"));
    assert!(!raw.contains("response.failed"));
    assert!(!raw.contains("[DONE]"));
    let events = parse_sse_bytes(bytes).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(&events[0].data).unwrap(),
        delta
    );
}

#[tokio::test]
async fn responses_does_not_forward_an_event_truncated_at_eof() {
    let created = json!({"type": "response.created", "sequence_number": 0});
    let upstream = FakeUpstream::start(vec![ScriptedResponse::sse(vec![
        ScriptChunk::immediate(format!("event: response.created\ndata: {created}\n\n")),
        ScriptChunk::immediate(
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"",
        ),
    ])])
    .await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = authorized_request(&client, &relay)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response
        .bytes()
        .await
        .expect("stream ends at truncated EOF");
    assert!(
        !String::from_utf8_lossy(&bytes).contains("output_text.delta"),
        "incomplete SSE records must not be forwarded"
    );
    let events = parse_sse_bytes(bytes).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, "response.created");
}

#[tokio::test]
async fn responses_client_disconnect_cancels_the_upstream_stream() {
    let upstream_stream_dropped = Arc::new(Notify::new());
    let created = json!({"type": "response.created", "sequence_number": 0});
    let mut script = ScriptedResponse::sse(vec![ScriptChunk::immediate(format!(
        "event: response.created\ndata: {created}\n\n"
    ))]);
    script.hold_open = true;
    script.dropped = Some(Arc::clone(&upstream_stream_dropped));
    let upstream = FakeUpstream::start(vec![script]).await;
    let relay = Relay::start(&upstream.base_url()).await;
    let client = client();

    let response = authorized_request(&client, &relay)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(response.status(), StatusCode::OK);
    let mut events = Box::pin(response.bytes_stream().eventsource());
    let first = timeout(Duration::from_secs(1), events.next())
        .await
        .expect("first event did not arrive")
        .expect("stream ended before first event")
        .expect("first event framing");
    assert_eq!(first.event, "response.created");

    drop(events);
    drop(client);
    timeout(Duration::from_secs(3), upstream_stream_dropped.notified())
        .await
        .expect("dropping the downstream response did not cancel upstream");
}
