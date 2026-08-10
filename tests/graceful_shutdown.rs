#![cfg(unix)]

use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade};
use axum::http::{StatusCode, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use eventsource_stream::Eventsource;
use futures_util::{SinkExt, StreamExt};
use http::header::AUTHORIZATION;
use serde_json::{Value, json};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const API_KEY_ID: &str = "lifecycle-client";
const API_KEY: &str = "sk-lifecycle-test";
const MODEL: &str = "gpt-lifecycle";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
enum UpstreamEvent {
    HttpRequest,
    WebSocketCreate,
}

#[derive(Clone)]
struct UpstreamState {
    events: mpsc::UnboundedSender<UpstreamEvent>,
}

struct FakeUpstream {
    addr: SocketAddr,
    events: mpsc::UnboundedReceiver<UpstreamEvent>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeUpstream {
    async fn start() -> Self {
        let (event_sender, events) = mpsc::unbounded_channel();
        let state = UpstreamState {
            events: event_sender,
        };
        let app = Router::new()
            .route("/responses", get(upstream_websocket).post(upstream_http))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind lifecycle upstream");
        let addr = listener
            .local_addr()
            .expect("read lifecycle upstream address");
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve lifecycle upstream");
        });
        Self { addr, events, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn expect_http_request(&mut self) {
        match timeout(TEST_TIMEOUT, self.events.recv())
            .await
            .expect("timed out waiting for upstream HTTP request")
            .expect("upstream event channel closed")
        {
            UpstreamEvent::HttpRequest => {}
            event => panic!("expected upstream HTTP request, got {event:?}"),
        }
    }

    async fn expect_websocket_create(&mut self) {
        match timeout(TEST_TIMEOUT, self.events.recv())
            .await
            .expect("timed out waiting for upstream response.create")
            .expect("upstream event channel closed")
        {
            UpstreamEvent::WebSocketCreate => {}
            event => panic!("expected upstream response.create, got {event:?}"),
        }
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream_http(State(state): State<UpstreamState>) -> Response {
    let _ = state.events.send(UpstreamEvent::HttpRequest);
    let created = json!({
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp-active", "status": "in_progress"}
    });
    let body = stream! {
        yield Ok::<Bytes, Infallible>(Bytes::from(format!(
            "event: response.created\ndata: {created}\n\n"
        )));
        std::future::pending::<()>().await;
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(body))
        .expect("build active upstream SSE response")
}

async fn upstream_websocket(
    websocket: WebSocketUpgrade,
    State(state): State<UpstreamState>,
) -> impl IntoResponse {
    websocket.on_upgrade(move |socket| serve_upstream_websocket(socket, state))
}

async fn serve_upstream_websocket(mut socket: WebSocket, state: UpstreamState) {
    while let Some(message) = socket.recv().await {
        match message {
            Ok(AxumMessage::Text(text)) => {
                let value: Value = match serde_json::from_str(text.as_str()) {
                    Ok(value) => value,
                    Err(_) => return,
                };
                if value.get("type").and_then(Value::as_str) == Some("response.create") {
                    let _ = state.events.send(UpstreamEvent::WebSocketCreate);
                    let created = json!({
                        "type": "response.created",
                        "sequence_number": 0,
                        "response": {"id": "resp-ws-active", "status": "in_progress"}
                    });
                    if socket
                        .send(AxumMessage::Text(created.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Ok(AxumMessage::Ping(payload)) => {
                if socket.send(AxumMessage::Pong(payload)).await.is_err() {
                    return;
                }
            }
            Ok(AxumMessage::Close(_)) | Err(_) => return,
            Ok(AxumMessage::Binary(_) | AxumMessage::Pong(_)) => {}
        }
    }
}

struct RelayProcess {
    child: Child,
    addr: SocketAddr,
    database_path: PathBuf,
    _directory: TempDir,
}

#[derive(Debug)]
struct ShutdownResult {
    exited_in_time: bool,
    successful: bool,
}

impl RelayProcess {
    async fn start(upstream: &FakeUpstream) -> Self {
        let directory = tempfile::tempdir().expect("create lifecycle fixture directory");
        let auth_path = directory.path().join("auth.json");
        let database_path = directory.path().join("state.sqlite3");
        let config_path = directory.path().join("config.toml");
        std::fs::write(
            &auth_path,
            serde_json::to_vec(&json!({
                "auth_mode": "chatgpt",
                "tokens": {
                    "id_token": "test-id-token",
                    "access_token": "eyJhbGciOiJub25lIn0.eyJleHAiOjQxMDI0NDQ4MDB9.test",
                    "refresh_token": "test-refresh-token",
                    "account_id": "acct-lifecycle"
                },
                "last_refresh": "2026-08-09T00:00:00Z"
            }))
            .expect("serialize lifecycle auth seed"),
        )
        .expect("write lifecycle auth seed");

        let reservation =
            StdTcpListener::bind("127.0.0.1:0").expect("reserve lifecycle downstream address");
        let addr = reservation
            .local_addr()
            .expect("read lifecycle downstream address");
        let config = format!(
            r#"[server]
listen = "{addr}"
enable_websockets = true

[state]
path = "{}"

[upstream]
base_url = "{}"
oauth_token_url = "{}/oauth/token"
auth_file = "{}"
supports_websockets = true

[[api_keys]]
id = "{API_KEY_ID}"
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
        std::fs::write(&config_path, config).expect("write lifecycle configuration");

        let mut command = Command::new(env!("CARGO_BIN_EXE_codex-api"));
        command
            .arg("--config")
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        drop(reservation);
        let mut child = command.spawn().expect("spawn lifecycle relay");
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait().expect("poll lifecycle relay") {
                panic!("codex-api exited before listening with {status}");
            }
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "codex-api did not start listening"
            );
            sleep(Duration::from_millis(20)).await;
        }
        Self {
            child,
            addr,
            database_path,
            _directory: directory,
        }
    }

    fn responses_url(&self) -> String {
        format!("http://{}/v1/responses", self.addr)
    }

    fn chat_url(&self) -> String {
        format!("http://{}/v1/chat/completions", self.addr)
    }

    fn websocket_url(&self) -> String {
        format!("ws://{}/v1/responses", self.addr)
    }

    async fn terminate(&mut self) -> ShutdownResult {
        let process_id = self.child.id().expect("lifecycle relay process ID");
        let signal = Command::new("kill")
            .arg("-TERM")
            .arg(process_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .expect("send SIGTERM to lifecycle relay");
        assert!(signal.success(), "kill -TERM failed");

        match timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => ShutdownResult {
                exited_in_time: true,
                successful: status.success(),
            },
            Ok(Err(error)) => panic!("failed to wait for lifecycle relay: {error}"),
            Err(_) => {
                let _ = self.child.start_kill();
                let _ = self.child.wait().await;
                ShutdownResult {
                    exited_in_time: false,
                    successful: false,
                }
            }
        }
    }
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Debug)]
struct RequestLog {
    status: String,
    http_status: Option<i64>,
    api_protocol: String,
    transport: String,
}

async fn request_log(path: &Path) -> RequestLog {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let mut database = SqliteConnection::connect_with(&options)
        .await
        .expect("open lifecycle request log");
    let row = sqlx::query(
        "SELECT status, http_status, api_protocol, transport FROM request_logs \
         WHERE api_key_id = ?",
    )
    .bind(API_KEY_ID)
    .fetch_one(&mut database)
    .await
    .expect("read lifecycle request log row");
    RequestLog {
        status: row.get("status"),
        http_status: row.get("http_status"),
        api_protocol: row.get("api_protocol"),
        transport: row.get("transport"),
    }
}

fn assert_shutdown_and_log(
    shutdown: ShutdownResult,
    log: RequestLog,
    protocol: &str,
    transport: &str,
    http_status: Option<i64>,
) {
    assert!(
        shutdown.exited_in_time && shutdown.successful,
        "SIGTERM did not complete a successful bounded shutdown; request log was {log:?}"
    );
    assert_eq!(log.status, "canceled");
    assert_eq!(log.http_status, http_status);
    assert_eq!(log.api_protocol, protocol);
    assert_eq!(log.transport, transport);
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(TEST_TIMEOUT)
        .build()
        .expect("build lifecycle HTTP client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_cancels_active_responses_sse_before_bounded_exit() {
    let mut upstream = FakeUpstream::start().await;
    let mut relay = RelayProcess::start(&upstream).await;
    let response = client()
        .post(relay.responses_url())
        .bearer_auth(API_KEY)
        .json(&json!({"model": MODEL, "input": "hold", "stream": true}))
        .send()
        .await
        .expect("start active downstream Responses stream");
    assert_eq!(response.status(), StatusCode::OK);
    upstream.expect_http_request().await;
    let mut events = response.bytes_stream().eventsource();
    let created = timeout(TEST_TIMEOUT, events.next())
        .await
        .expect("timed out waiting for downstream SSE event")
        .expect("downstream SSE ended before response.created")
        .expect("decode downstream SSE event");
    assert_eq!(created.event, "response.created");

    let shutdown = relay.terminate().await;
    let log = request_log(&relay.database_path).await;
    assert_shutdown_and_log(shutdown, log, "responses", "http_sse", Some(200));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_cancels_active_chat_aggregation_before_bounded_exit() {
    let mut upstream = FakeUpstream::start().await;
    let mut relay = RelayProcess::start(&upstream).await;
    let chat_client = client();
    let chat_url = relay.chat_url();
    let request = tokio::spawn(async move {
        chat_client
            .post(chat_url)
            .bearer_auth(API_KEY)
            .json(&json!({
                "model": MODEL,
                "messages": [{"role": "user", "content": "hold"}]
            }))
            .send()
            .await
    });
    upstream.expect_http_request().await;
    assert!(
        !request.is_finished(),
        "Chat response completed before SIGTERM"
    );

    let shutdown = relay.terminate().await;
    let log = request_log(&relay.database_path).await;
    assert_shutdown_and_log(shutdown, log, "chat_completions", "http_sse", None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_cancels_active_websocket_operation_before_bounded_exit() {
    let mut upstream = FakeUpstream::start().await;
    let mut relay = RelayProcess::start(&upstream).await;
    let mut request = relay
        .websocket_url()
        .into_client_request()
        .expect("build downstream WebSocket request");
    request.headers_mut().insert(
        AUTHORIZATION,
        format!("Bearer {API_KEY}")
            .parse()
            .expect("build downstream authorization header"),
    );
    let (mut socket, response) = connect_async(request)
        .await
        .expect("upgrade lifecycle downstream WebSocket");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    socket
        .send(Message::Text(
            json!({"type": "response.create", "model": MODEL, "input": "hold"})
                .to_string()
                .into(),
        ))
        .await
        .expect("send active response.create");
    upstream.expect_websocket_create().await;
    let created = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("timed out waiting for downstream WebSocket event")
        .expect("downstream WebSocket closed before response.created")
        .expect("read downstream WebSocket event");
    let Message::Text(created) = created else {
        panic!("expected downstream response.created text frame, got {created:?}");
    };
    let created: Value =
        serde_json::from_str(created.as_str()).expect("decode downstream response.created event");
    assert_eq!(created["type"], "response.created");

    let shutdown = relay.terminate().await;
    let log = request_log(&relay.database_path).await;
    assert_shutdown_and_log(shutdown, log, "responses", "websocket", None);
}
