use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{CloseFrame as AxumCloseFrame, Message as AxumMessage, WebSocket};
use axum::extract::{Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const CLIENT_KEY: &str = "sk-local-client";
const API_KEY_ID: &str = "client-a";
const ACCOUNT_ID: &str = "account-test";
const MODEL: &str = "gpt-5.6-luna";
const FALLBACK_MODEL: &str = "gpt-5.6-fallback";
const WS_BETA: &str = "responses_websockets=2026-02-06";
const OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const TEST_TIMEOUT: Duration = Duration::from_secs(4);

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
enum UpstreamCommand {
    Text(String),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close { code: u16, reason: String },
    Abort,
}

#[derive(Debug)]
struct UpstreamConnection {
    id: usize,
    commands: mpsc::UnboundedSender<UpstreamCommand>,
}

impl UpstreamConnection {
    fn send_json(&self, event: Value) {
        self.commands
            .send(UpstreamCommand::Text(event.to_string()))
            .expect("fake upstream connection should still be open");
    }

    fn abort(&self) {
        self.commands
            .send(UpstreamCommand::Abort)
            .expect("fake upstream connection should still be open");
    }

    fn close(&self, code: u16, reason: &str) {
        self.commands
            .send(UpstreamCommand::Close {
                code,
                reason: reason.to_owned(),
            })
            .expect("fake upstream connection should still be open");
    }
}

#[derive(Debug)]
enum UpstreamEvent {
    Handshake {
        headers: HeaderMap,
        accepted: bool,
    },
    Connected(UpstreamConnection),
    Text {
        connection_id: usize,
        text: String,
    },
    Ping {
        connection_id: usize,
        payload: Vec<u8>,
    },
    Pong {
        connection_id: usize,
        payload: Vec<u8>,
    },
    Close {
        connection_id: usize,
        code: Option<u16>,
    },
    OAuth {
        headers: HeaderMap,
        body: Value,
    },
}

#[derive(Clone)]
struct FakeUpstreamState {
    required_authorization: Option<String>,
    refreshed_access_token: String,
    events: mpsc::UnboundedSender<UpstreamEvent>,
    handshake_count: Arc<AtomicUsize>,
    connection_count: Arc<AtomicUsize>,
}

struct FakeUpstream {
    addr: SocketAddr,
    events: mpsc::UnboundedReceiver<UpstreamEvent>,
    handshake_count: Arc<AtomicUsize>,
    connection_count: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start(required_access_token: Option<&str>, refreshed_access_token: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let addr = listener.local_addr().expect("fake upstream address");
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let handshake_count = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let state = FakeUpstreamState {
            required_authorization: required_access_token.map(|token| format!("Bearer {token}")),
            refreshed_access_token: refreshed_access_token.to_string(),
            events: events_tx,
            handshake_count: Arc::clone(&handshake_count),
            connection_count: Arc::clone(&connection_count),
        };
        let app = Router::new()
            .route("/responses", get(fake_responses_websocket))
            .route("/oauth/token", post(fake_oauth_token))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve fake upstream");
        });

        Self {
            addr,
            events: events_rx,
            handshake_count,
            connection_count,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn oauth_url(&self) -> String {
        format!("http://{}/oauth/token", self.addr)
    }

    fn handshake_count(&self) -> usize {
        self.handshake_count.load(Ordering::SeqCst)
    }

    fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::SeqCst)
    }

    async fn next_event(&mut self) -> UpstreamEvent {
        timeout(TEST_TIMEOUT, self.events.recv())
            .await
            .expect("timed out waiting for fake upstream event")
            .expect("fake upstream event channel closed")
    }

    async fn expect_handshake(&mut self) -> (HeaderMap, bool) {
        match self.next_event().await {
            UpstreamEvent::Handshake { headers, accepted } => (headers, accepted),
            event => panic!("expected upstream handshake, got {event:?}"),
        }
    }

    async fn expect_connection(&mut self) -> UpstreamConnection {
        match self.next_event().await {
            UpstreamEvent::Connected(connection) => connection,
            event => panic!("expected upstream connection, got {event:?}"),
        }
    }

    async fn expect_text(&mut self, connection_id: usize) -> Value {
        match self.next_event().await {
            UpstreamEvent::Text {
                connection_id: observed_id,
                text,
            } => {
                assert_eq!(observed_id, connection_id);
                serde_json::from_str(&text).expect("upstream request should be JSON")
            }
            event => panic!("expected upstream text frame, got {event:?}"),
        }
    }

    async fn expect_close(&mut self, connection_id: usize) -> Option<u16> {
        match self.next_event().await {
            UpstreamEvent::Close {
                connection_id: observed_id,
                code,
            } => {
                assert_eq!(observed_id, connection_id);
                code
            }
            event => panic!("expected upstream close, got {event:?}"),
        }
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake_responses_websocket(
    State(state): State<FakeUpstreamState>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    state.handshake_count.fetch_add(1, Ordering::SeqCst);
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok());
    let accepted = state
        .required_authorization
        .as_deref()
        .is_none_or(|required| authorization == Some(required));
    state
        .events
        .send(UpstreamEvent::Handshake {
            headers: headers.clone(),
            accepted,
        })
        .expect("fake upstream observer should be alive");

    if !accepted {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": {
                    "type": "authentication_error",
                    "code": "token_expired",
                    "message": "the scripted access token was rejected"
                }
            })),
        )
            .into_response();
    }

    let connection_id = state.connection_count.fetch_add(1, Ordering::SeqCst);
    websocket
        .on_upgrade(move |socket| run_fake_upstream(socket, state, connection_id))
        .into_response()
}

async fn run_fake_upstream(mut socket: WebSocket, state: FakeUpstreamState, connection_id: usize) {
    let (commands_tx, mut commands_rx) = mpsc::unbounded_channel();
    if state
        .events
        .send(UpstreamEvent::Connected(UpstreamConnection {
            id: connection_id,
            commands: commands_tx,
        }))
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(AxumMessage::Text(text))) => {
                        if state.events.send(UpstreamEvent::Text {
                            connection_id,
                            text: text.to_string(),
                        }).is_err() {
                            return;
                        }
                    }
                    Some(Ok(AxumMessage::Close(frame))) => {
                        let code = frame.map(|frame| frame.code);
                        let _ = state.events.send(UpstreamEvent::Close { connection_id, code });
                        return;
                    }
                    Some(Ok(AxumMessage::Ping(payload))) => {
                        if state.events.send(UpstreamEvent::Ping {
                            connection_id,
                            payload: payload.to_vec(),
                        }).is_err() {
                            return;
                        }
                        if socket.send(AxumMessage::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(AxumMessage::Pong(payload))) => {
                        if state.events.send(UpstreamEvent::Pong {
                            connection_id,
                            payload: payload.to_vec(),
                        }).is_err() {
                            return;
                        }
                    }
                    Some(Ok(AxumMessage::Binary(_))) | Some(Err(_)) | None => return,
                }
            }
            command = commands_rx.recv() => {
                match command {
                    Some(UpstreamCommand::Text(text)) => {
                        if socket.send(AxumMessage::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    Some(UpstreamCommand::Ping(payload)) => {
                        if socket.send(AxumMessage::Ping(payload.into())).await.is_err() {
                            return;
                        }
                    }
                    Some(UpstreamCommand::Pong(payload)) => {
                        if socket.send(AxumMessage::Pong(payload.into())).await.is_err() {
                            return;
                        }
                    }
                    Some(UpstreamCommand::Close { code, reason }) => {
                        if socket
                            .send(AxumMessage::Close(Some(AxumCloseFrame {
                                code,
                                reason: reason.into(),
                            })))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Some(UpstreamCommand::Abort) | None => return,
                }
            }
        }
    }
}

async fn fake_oauth_token(
    State(state): State<FakeUpstreamState>,
    request: Request<Body>,
) -> Response {
    let headers = request.headers().clone();
    let bytes = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    let body = match serde_json::from_slice::<Value>(&bytes) {
        Ok(body) => body,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    if state
        .events
        .send(UpstreamEvent::OAuth { headers, body })
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    Json(json!({
        "access_token": state.refreshed_access_token,
        "refresh_token": "rotated-refresh-token"
    }))
    .into_response()
}

#[derive(Clone)]
struct RelayOptions {
    enable_websockets: bool,
    supports_websockets: bool,
    weekly_limit_usd: Option<&'static str>,
    hard_limit_usd: Option<&'static str>,
    fallback_model: Option<&'static str>,
    access_token: String,
    refresh_token: &'static str,
}

impl RelayOptions {
    fn enabled() -> Self {
        Self {
            enable_websockets: true,
            supports_websockets: true,
            weekly_limit_usd: None,
            hard_limit_usd: None,
            fallback_model: None,
            access_token: jwt(json!({"exp": 4_102_444_800_u64})),
            refresh_token: "upstream-refresh-token",
        }
    }
}

struct RelayFiles {
    _temp_dir: TempDir,
    config_path: PathBuf,
    database_path: PathBuf,
    listen_addr: SocketAddr,
}

struct RelayProcess {
    child: Child,
    addr: SocketAddr,
    database_path: PathBuf,
    _temp_dir: TempDir,
}

impl RelayProcess {
    async fn start(upstream: &FakeUpstream, options: RelayOptions) -> Self {
        let files = write_relay_files(upstream, &options);
        let RelayFiles {
            _temp_dir,
            config_path,
            database_path,
            listen_addr,
        } = files;
        let mut child = relay_command(&config_path)
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn codex-api binary");
        wait_until_listening(&mut child, listen_addr).await;
        Self {
            child,
            addr: listen_addr,
            database_path,
            _temp_dir,
        }
    }

    fn websocket_url(&self) -> String {
        format!("ws://{}/v1/responses", self.addr)
    }

    async fn stop(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn write_relay_files(upstream: &FakeUpstream, options: &RelayOptions) -> RelayFiles {
    let temp_dir = tempfile::tempdir().expect("create relay temp directory");
    let auth_path = temp_dir.path().join("auth.json");
    let config_path = temp_dir.path().join("config.toml");
    let database_path = temp_dir.path().join("state.sqlite3");
    let listen_addr = unused_loopback_addr();
    let id_token = jwt(json!({
        "https://api.openai.com/auth": {
            "chatgpt_account_id": ACCOUNT_ID,
            "chatgpt_plan_type": "pro"
        }
    }));
    std::fs::write(
        &auth_path,
        serde_json::to_vec_pretty(&json!({
            "OPENAI_API_KEY": null,
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": id_token,
                "access_token": options.access_token,
                "refresh_token": options.refresh_token,
                "account_id": ACCOUNT_ID
            },
            "last_refresh": "2099-12-31T00:00:00Z"
        }))
        .expect("serialize auth seed"),
    )
    .expect("write auth seed");
    let weekly_limit = options
        .weekly_limit_usd
        .map(|limit| format!("weekly_limit_usd = \"{limit}\"\n"))
        .unwrap_or_default();
    let hard_limit = options
        .hard_limit_usd
        .map(|limit| format!("hard_limit_usd = \"{limit}\"\n"))
        .unwrap_or_default();
    let fallback = options
        .fallback_model
        .map(|model| format!("fallback_model = \"{model}\"\n"))
        .unwrap_or_default();
    let fallback_prices = options
        .fallback_model
        .map(|model| {
            format!(
                r#"
[model_prices."{model}"]
input_usd_per_million = "0.10"
cached_input_usd_per_million = "0.01"
output_usd_per_million = "0.20"
"#
            )
        })
        .unwrap_or_default();
    let config = format!(
        r#"{fallback}[server]
listen = "{listen_addr}"
enable_websockets = {enable_websockets}

[state]
path = "{database_path}"

[upstream]
base_url = "{base_url}"
oauth_token_url = "{oauth_url}"
auth_file = "{auth_path}"
supports_websockets = {supports_websockets}

[[api_keys]]
id = "{api_key_id}"
secret = "{client_key}"
{weekly_limit}{hard_limit}
[model_prices."{model}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
{fallback_prices}"#,
        enable_websockets = options.enable_websockets,
        database_path = database_path.display(),
        base_url = upstream.base_url(),
        oauth_url = upstream.oauth_url(),
        auth_path = auth_path.display(),
        supports_websockets = options.supports_websockets,
        api_key_id = API_KEY_ID,
        client_key = CLIENT_KEY,
        model = MODEL,
    );
    std::fs::write(&config_path, config).expect("write relay configuration");

    RelayFiles {
        _temp_dir: temp_dir,
        config_path,
        database_path,
        listen_addr,
    }
}

fn relay_command(config_path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_codex-api"));
    command.arg("--config").arg(config_path).arg("serve");
    command
}

fn unused_loopback_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve relay port");
    listener.local_addr().expect("reserved relay address")
}

async fn wait_until_listening(child: &mut Child, addr: SocketAddr) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("inspect relay process") {
            panic!("codex-api exited before listening: {status}");
        }
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "codex-api did not start listening"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

fn jwt(payload: Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(payload.to_string());
    format!("{header}.{payload}.test-signature")
}

async fn connect_downstream(
    url: &str,
    authorization: Option<&str>,
) -> Result<ClientSocket, WsError> {
    connect_downstream_with_headers(url, authorization, &[]).await
}

async fn connect_downstream_with_headers(
    url: &str,
    authorization: Option<&str>,
    headers: &[(&'static str, &'static str)],
) -> Result<ClientSocket, WsError> {
    let mut request = url
        .into_client_request()
        .expect("build downstream WebSocket request");
    if let Some(authorization) = authorization {
        request.headers_mut().insert(
            "authorization",
            authorization
                .parse()
                .expect("valid downstream authorization header"),
        );
    }
    for &(name, value) in headers {
        request.headers_mut().insert(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    timeout(TEST_TIMEOUT, connect_async(request))
        .await
        .expect("timed out waiting for downstream WebSocket handshake")
        .map(|(socket, _)| socket)
}

fn assert_handshake_status(result: Result<ClientSocket, WsError>, expected: StatusCode) {
    match result {
        Err(WsError::Http(response)) => assert_eq!(response.status(), expected),
        Err(error) => panic!("expected HTTP {expected} handshake response, got {error}"),
        Ok(_) => panic!("expected HTTP {expected} handshake response, upgrade succeeded"),
    }
}

fn response_create(input: &str, previous_response_id: Option<&str>) -> Value {
    let mut request = json!({
        "type": "response.create",
        "model": MODEL,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": input}]
        }],
        "reasoning": {"effort": "low"}
    });
    if let Some(previous_response_id) = previous_response_id {
        request["previous_response_id"] = json!(previous_response_id);
    }
    request
}

fn response_completed(
    id: &str,
    input_tokens: u64,
    cached_tokens: u64,
    output_tokens: u64,
) -> Value {
    json!({
        "type": "response.completed",
        "sequence_number": 20,
        "response": {
            "id": id,
            "object": "response",
            "created_at": 1_700_000_000,
            "status": "completed",
            "background": false,
            "error": null,
            "incomplete_details": null,
            "instructions": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "model": MODEL,
            "output": [],
            "parallel_tool_calls": true,
            "previous_response_id": null,
            "prompt_cache_key": null,
            "reasoning": {"effort": "low", "summary": null},
            "safety_identifier": null,
            "service_tier": "default",
            "store": false,
            "temperature": 1.0,
            "text": {"format": {"type": "text"}},
            "tool_choice": "auto",
            "tools": [],
            "top_logprobs": 0,
            "top_p": 1.0,
            "truncation": "disabled",
            "usage": {
                "input_tokens": input_tokens,
                "input_tokens_details": {"cached_tokens": cached_tokens},
                "output_tokens": output_tokens,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": input_tokens + output_tokens
            },
            "user": null,
            "metadata": {}
        }
    })
}

async fn send_json(socket: &mut ClientSocket, value: &Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("send downstream WebSocket JSON");
}

async fn receive_json(socket: &mut ClientSocket) -> Value {
    loop {
        let message = timeout(TEST_TIMEOUT, socket.next())
            .await
            .expect("timed out waiting for downstream WebSocket event")
            .expect("downstream WebSocket ended before an event")
            .expect("read downstream WebSocket event");
        match message {
            Message::Text(text) => {
                return serde_json::from_str(text.as_str())
                    .expect("downstream text event should be JSON");
            }
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .expect("reply to downstream ping"),
            Message::Pong(_) => {}
            Message::Close(frame) => panic!("downstream closed before JSON event: {frame:?}"),
            Message::Binary(_) | Message::Frame(_) => {
                panic!("unexpected non-text downstream WebSocket frame")
            }
        }
    }
}

async fn receive_close(socket: &mut ClientSocket) -> CloseFrame {
    loop {
        let message = timeout(TEST_TIMEOUT, socket.next())
            .await
            .expect("timed out waiting for downstream WebSocket close")
            .expect("downstream WebSocket ended without a close frame")
            .expect("read downstream WebSocket close");
        match message {
            Message::Close(Some(frame)) => return frame,
            Message::Close(None) => panic!("downstream close omitted its status code"),
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .expect("reply to downstream ping"),
            Message::Text(_) | Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

fn assert_responses_error(event: &Value, code: &str, param: Option<&str>) {
    assert_eq!(event["type"], "error");
    assert_eq!(event["code"], code);
    match param {
        Some(param) => assert_eq!(event["param"], param),
        None => assert!(event.get("param").is_none_or(Value::is_null)),
    }
    assert!(
        event["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "Responses error event must include a message: {event}"
    );
}

async fn request_log_rows(
    database_path: &Path,
    minimum_rows: usize,
) -> Vec<(String, String, bool)> {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .read_only(true);
    let pool = timeout(
        TEST_TIMEOUT,
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options),
    )
    .await
    .expect("timed out opening request log database")
    .expect("open request log database");
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let rows =
            sqlx::query("SELECT status, transport, http_status FROM request_logs ORDER BY id ASC")
                .fetch_all(&pool)
                .await
                .expect("query public request_logs view");
        if rows.len() >= minimum_rows {
            return rows
                .into_iter()
                .map(|row| {
                    let status = row.get::<String, _>("status");
                    let transport = row.get::<String, _>("transport");
                    let http_status_is_null = row
                        .try_get::<Option<i64>, _>("http_status")
                        .expect("decode nullable HTTP status")
                        .is_none();
                    (status, transport, http_status_is_null)
                })
                .collect();
        }
        assert!(
            Instant::now() < deadline,
            "request_logs did not reach {minimum_rows} rows"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

async fn assert_only_request_log_has_no_usage_or_cost(database_path: &Path) {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open request log database");
    let row = sqlx::query(
        "SELECT input_tokens, cached_input_tokens, output_tokens, cost_usd \
         FROM request_logs",
    )
    .fetch_one(&pool)
    .await
    .expect("query failed operation accounting");
    for column in ["input_tokens", "cached_input_tokens", "output_tokens"] {
        assert!(
            row.try_get::<Option<i64>, _>(column)
                .expect("decode nullable token count")
                .is_none(),
            "{column} must remain null"
        );
    }
    assert!(
        row.try_get::<Option<String>, _>("cost_usd")
            .expect("decode nullable request cost")
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_upgrade_authenticates_before_contacting_upstream() {
    let access_token = jwt(json!({"exp": 4_102_444_800_u64}));
    let mut upstream = FakeUpstream::start(Some(&access_token), "unused-refresh-token").await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            access_token,
            ..RelayOptions::enabled()
        },
    )
    .await;

    for authorization in [None, Some("Basic abc"), Some("Bearer wrong-key")] {
        let result = connect_downstream(&relay.websocket_url(), authorization).await;
        assert_handshake_status(result, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(upstream.handshake_count(), 0);

    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("valid downstream key should upgrade");
    let (_, accepted) = upstream.expect_handshake().await;
    assert!(accepted);
    let connection = upstream.expect_connection().await;
    assert_eq!(connection.id, 0);
    assert_eq!(upstream.connection_count(), 1);

    socket
        .close(None)
        .await
        .expect("close authenticated downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_configuration_gate_is_enforced() {
    let upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let disabled = RelayProcess::start(
        &upstream,
        RelayOptions {
            enable_websockets: false,
            ..RelayOptions::enabled()
        },
    )
    .await;
    let result = connect_downstream(
        &disabled.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await;
    assert_handshake_status(result, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(upstream.handshake_count(), 0);
    disabled.stop().await;

    let files = write_relay_files(
        &upstream,
        &RelayOptions {
            enable_websockets: true,
            supports_websockets: false,
            ..RelayOptions::enabled()
        },
    );
    let output = timeout(
        TEST_TIMEOUT,
        relay_command(&files.config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .expect("invalid WebSocket configuration should exit")
    .expect("run codex-api with invalid WebSocket configuration");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("supports_websockets"),
        "stderr was: {stderr}"
    );
    assert!(!stderr.contains(CLIENT_KEY));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_downstream_socket_uses_one_authenticated_codex_upstream_and_normalizes_create() {
    let access_token = jwt(json!({"exp": 4_102_444_800_u64}));
    let mut upstream = FakeUpstream::start(Some(&access_token), "unused-refresh-token").await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            access_token: access_token.clone(),
            ..RelayOptions::enabled()
        },
    )
    .await;
    let mut socket = connect_downstream_with_headers(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
        &[
            ("thread_id", "thread-123"),
            ("x-codex-beta-features", "beta-one"),
            ("x-oai-attestation", "must-not-pass"),
        ],
    )
    .await
    .expect("upgrade downstream WebSocket");
    let (headers, accepted) = upstream.expect_handshake().await;
    assert!(accepted);
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some(format!("Bearer {access_token}").as_str())
    );
    assert_eq!(
        headers
            .get("chatgpt-account-id")
            .and_then(|value| value.to_str().ok()),
        Some(ACCOUNT_ID)
    );
    assert_eq!(
        headers
            .get("openai-beta")
            .and_then(|value| value.to_str().ok()),
        Some(WS_BETA)
    );
    assert_eq!(
        headers
            .get("originator")
            .and_then(|value| value.to_str().ok()),
        Some("codex_cli_rs")
    );
    assert_eq!(
        headers.get("version").and_then(|value| value.to_str().ok()),
        Some("0.147.0")
    );
    assert!(
        headers
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("codex_cli_rs/0.147.0 "))
    );
    assert_eq!(
        headers
            .get("thread_id")
            .and_then(|value| value.to_str().ok()),
        Some("thread-123")
    );
    assert_eq!(
        headers
            .get("x-codex-beta-features")
            .and_then(|value| value.to_str().ok()),
        Some("beta-one")
    );
    assert!(!headers.contains_key("x-oai-attestation"));
    assert_ne!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some(format!("Bearer {CLIENT_KEY}").as_str())
    );
    let connection = upstream.expect_connection().await;

    let create = response_create("preserve this input", None);
    send_json(&mut socket, &create).await;
    let upstream_create = upstream.expect_text(connection.id).await;
    assert_eq!(upstream_create["type"], "response.create");
    assert_eq!(upstream_create["model"], MODEL);
    assert_eq!(upstream_create["input"], create["input"]);
    assert_eq!(upstream_create["reasoning"], create["reasoning"]);
    assert_eq!(upstream_create["store"], false);
    assert_eq!(upstream_create["stream"], true);
    assert!(upstream_create.get("background").is_none());
    assert_eq!(upstream.handshake_count(), 1);
    assert_eq!(upstream.connection_count(), 1);

    let terminal = response_completed("resp-normalized", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);
    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_null_is_accepted_and_forced_false_upstream() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    let mut create = response_create("nullable store", None);
    create["store"] = Value::Null;
    send_json(&mut socket, &create).await;
    let forwarded = upstream.expect_text(connection.id).await;
    assert_eq!(forwarded["store"], false);

    let terminal = response_completed("resp-null-store", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);
    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_text_events_are_forwarded_unchanged_and_in_order() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let (_, accepted) = upstream.expect_handshake().await;
    assert!(accepted);
    let connection = upstream.expect_connection().await;
    send_json(&mut socket, &response_create("ordered events", None)).await;
    let _ = upstream.expect_text(connection.id).await;

    let events = vec![
        json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": {"id": "resp-order", "status": "in_progress"}
        }),
        json!({
            "type": "response.output_text.delta",
            "sequence_number": 1,
            "item_id": "msg-1",
            "output_index": 0,
            "content_index": 0,
            "delta": "first"
        }),
        json!({
            "type": "codex.opaque_informational_event",
            "sequence_number": 2,
            "opaque": {"preserved": true}
        }),
        response_completed("resp-order", 3, 1, 2),
    ];
    for event in &events {
        connection.send_json(event.clone());
    }
    for expected in events {
        assert_eq!(receive_json(&mut socket).await, expected);
    }

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_and_pong_frames_are_forwarded_both_ways_and_leave_the_connection_usable() {
    const DOWNSTREAM_PING: &[u8] = b"downstream-ping";
    const UPSTREAM_PING: &[u8] = b"upstream-ping";
    const DOWNSTREAM_PONG: &[u8] = b"downstream-pong";
    const UPSTREAM_PONG: &[u8] = b"upstream-pong";

    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    timeout(TEST_TIMEOUT, async {
        socket
            .send(Message::Ping(DOWNSTREAM_PING.to_vec().into()))
            .await
            .expect("send downstream ping");
        match upstream
            .events
            .recv()
            .await
            .expect("upstream event channel")
        {
            UpstreamEvent::Ping {
                connection_id,
                payload,
            } => {
                assert_eq!(connection_id, connection.id);
                assert_eq!(payload, DOWNSTREAM_PING);
            }
            event => panic!("expected forwarded downstream ping, got {event:?}"),
        }

        connection
            .commands
            .send(UpstreamCommand::Pong(UPSTREAM_PONG.to_vec()))
            .expect("fake upstream connection should still be open");
        loop {
            match socket
                .next()
                .await
                .expect("downstream socket ended during pong forwarding")
                .expect("read downstream pong")
            {
                Message::Pong(payload) if payload.as_ref() == UPSTREAM_PONG => break,
                Message::Pong(_) => {}
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("reply to unrelated downstream ping"),
                message => panic!("expected forwarded upstream pong, got {message:?}"),
            }
        }

        connection
            .commands
            .send(UpstreamCommand::Ping(UPSTREAM_PING.to_vec()))
            .expect("fake upstream connection should still be open");
        loop {
            match socket
                .next()
                .await
                .expect("downstream socket ended during ping forwarding")
                .expect("read downstream ping")
            {
                Message::Ping(payload) if payload.as_ref() == UPSTREAM_PING => break,
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("reply to unrelated downstream ping"),
                Message::Pong(_) => {}
                message => panic!("expected forwarded upstream ping, got {message:?}"),
            }
        }
        socket
            .send(Message::Pong(DOWNSTREAM_PONG.to_vec().into()))
            .await
            .expect("send downstream pong");
        loop {
            match upstream
                .events
                .recv()
                .await
                .expect("upstream event channel")
            {
                UpstreamEvent::Pong {
                    connection_id,
                    payload,
                } if payload == DOWNSTREAM_PONG => {
                    assert_eq!(connection_id, connection.id);
                    break;
                }
                UpstreamEvent::Pong {
                    connection_id,
                    payload,
                } if payload == UPSTREAM_PING => {
                    assert_eq!(connection_id, connection.id);
                }
                event => panic!("expected forwarded downstream pong, got {event:?}"),
            }
        }
    })
    .await
    .expect("timed out exercising bidirectional ping/pong forwarding");

    send_json(
        &mut socket,
        &response_create("usable after control frames", None),
    )
    .await;
    let forwarded = upstream.expect_text(connection.id).await;
    assert_eq!(
        forwarded["input"][0]["content"][0]["text"],
        "usable after control frames"
    );
    let terminal = response_completed("resp-after-controls", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_response_without_usage_is_not_forwarded_or_billed() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(
        &mut socket,
        &response_create("successful response missing usage", None),
    )
    .await;
    let _ = upstream.expect_text(connection.id).await;

    let mut terminal = response_completed("resp-missing-usage", 3, 1, 2);
    terminal["response"]
        .as_object_mut()
        .expect("response object")
        .remove("usage");
    connection.send_json(terminal);

    let message = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("timed out waiting for downstream accounting close")
        .expect("downstream ended without an accounting close")
        .expect("read downstream accounting close");
    let close = match message {
        Message::Close(Some(close)) => close,
        Message::Text(text) => panic!("unaccountable terminal was forwarded downstream: {text}"),
        message => panic!("expected downstream accounting close, got {message:?}"),
    };
    assert_eq!(close.code, CloseCode::Error);

    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    assert_only_request_log_has_no_usage_or_cost(&relay.database_path).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_present_cached_token_details_close_with_1011_before_terminal_forwarding() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(&mut socket, &response_create("malformed usage", None)).await;
    let _ = upstream.expect_text(connection.id).await;

    let mut terminal = response_completed("resp-malformed-usage", 3, 1, 2);
    terminal["response"]["usage"]["input_tokens_details"] =
        json!({"cached_tokens": "not-an-integer"});
    connection.send_json(terminal);

    let close = receive_close(&mut socket).await;
    assert_eq!(close.code, CloseCode::Error);
    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_total_tokens_closes_1011_without_terminal_forwarding_or_billing() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(&mut socket, &response_create("malformed total usage", None)).await;
    let _ = upstream.expect_text(connection.id).await;

    let mut terminal = response_completed("resp-malformed-total", 3, 1, 2);
    terminal["response"]["usage"]["total_tokens"] = json!("not-an-integer");
    connection.send_json(terminal);

    let message = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("timed out waiting for downstream protocol close")
        .expect("downstream ended without a protocol close")
        .expect("read downstream protocol close");
    let close = match message {
        Message::Close(Some(close)) => close,
        Message::Text(text) => panic!("malformed terminal was forwarded downstream: {text}"),
        message => panic!("expected downstream protocol close, got {message:?}"),
    };
    assert_eq!(close.code, CloseCode::Error);

    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    assert_only_request_log_has_no_usage_or_cost(&relay.database_path).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_tokens_above_input_tokens_fail_before_accounting_and_finalize_upstream_error() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(
        &mut socket,
        &response_create("invalid cached token count", None),
    )
    .await;
    let _ = upstream.expect_text(connection.id).await;

    connection.send_json(response_completed("resp-invalid-cached", 1, 2, 1));
    let message = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("timed out waiting for downstream protocol close")
        .expect("downstream ended without a protocol close")
        .expect("read downstream protocol close");
    let close = match message {
        Message::Close(Some(close)) => close,
        Message::Text(text) => panic!("malformed terminal was forwarded downstream: {text}"),
        message => panic!("expected downstream protocol close, got {message:?}"),
    };
    assert_eq!(close.code, CloseCode::Error);

    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    assert_only_request_log_has_no_usage_or_cost(&relay.database_path).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_above_sqlite_range_closes_without_forwarding_and_finalizes_upstream_error() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(
        &mut socket,
        &response_create("usage outside SQLite range", None),
    )
    .await;
    let _ = upstream.expect_text(connection.id).await;

    connection.send_json(response_completed(
        "resp-usage-out-of-range",
        i64::MAX as u64 + 1,
        0,
        1,
    ));
    let message = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("timed out waiting for downstream accounting close")
        .expect("downstream ended without an accounting close")
        .expect("read downstream accounting close");
    let close = match message {
        Message::Close(Some(close)) => close,
        Message::Text(text) => panic!("unaccountable terminal was forwarded downstream: {text}"),
        message => panic!("expected downstream accounting close, got {message:?}"),
    };
    assert_eq!(close.code, CloseCode::Error);

    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    assert_only_request_log_has_no_usage_or_cost(&relay.database_path).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_event_type_must_match_response_status() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(&mut socket, &response_create("mismatched terminal", None)).await;
    let _ = upstream.expect_text(connection.id).await;

    let mut terminal = response_completed("resp-status-mismatch", 1, 0, 1);
    terminal["response"]["status"] = json!("incomplete");
    connection.send_json(terminal);

    let close = receive_close(&mut socket).await;
    assert_eq!(close.code, CloseCode::Error);
    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_turns_and_previous_response_id_reuse_the_same_upstream_connection() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    send_json(&mut socket, &response_create("first turn", None)).await;
    let first = upstream.expect_text(connection.id).await;
    assert!(first.get("previous_response_id").is_none());
    let first_terminal = response_completed("resp-first", 1, 0, 1);
    connection.send_json(first_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, first_terminal);

    send_json(
        &mut socket,
        &response_create("second turn", Some("resp-first")),
    )
    .await;
    let second = upstream.expect_text(connection.id).await;
    assert_eq!(second["previous_response_id"], "resp-first");
    assert_eq!(second["input"][0]["content"][0]["text"], "second turn");
    let second_terminal = response_completed("resp-second", 2, 0, 1);
    connection.send_json(second_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, second_terminal);
    assert_eq!(upstream.handshake_count(), 1);
    assert_eq!(upstream.connection_count(), 1);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_in_flight_create_is_rejected_locally_then_a_later_turn_is_allowed() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    send_json(&mut socket, &response_create("still running", None)).await;
    let first = upstream.expect_text(connection.id).await;
    assert_eq!(first["input"][0]["content"][0]["text"], "still running");
    send_json(&mut socket, &response_create("must be rejected", None)).await;
    let error = receive_json(&mut socket).await;
    assert_responses_error(&error, "response_in_progress", None);

    let first_terminal = response_completed("resp-running", 1, 0, 1);
    connection.send_json(first_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, first_terminal);
    send_json(
        &mut socket,
        &response_create("now allowed", Some("resp-running")),
    )
    .await;
    let later = upstream.expect_text(connection.id).await;
    assert_eq!(later["input"][0]["content"][0]["text"], "now allowed");
    assert_eq!(later["previous_response_id"], "resp-running");
    let later_terminal = response_completed("resp-later", 1, 0, 1);
    connection.send_json(later_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, later_terminal);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_is_checked_before_rejecting_a_second_in_flight_create() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            weekly_limit_usd: Some("0.000001"),
            ..RelayOptions::enabled()
        },
    )
    .await;

    let mut first_socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade first downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let first_connection = upstream.expect_connection().await;
    send_json(
        &mut first_socket,
        &response_create("keep this response in flight", None),
    )
    .await;
    let _ = upstream.expect_text(first_connection.id).await;

    let mut spending_socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade spending downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let spending_connection = upstream.expect_connection().await;
    send_json(
        &mut spending_socket,
        &response_create("commit spend on another connection", None),
    )
    .await;
    let _ = upstream.expect_text(spending_connection.id).await;
    let spending_terminal = response_completed("resp-spend", 1, 0, 1);
    spending_connection.send_json(spending_terminal.clone());
    assert_eq!(receive_json(&mut spending_socket).await, spending_terminal);

    send_json(
        &mut first_socket,
        &response_create("quota takes precedence", None),
    )
    .await;
    let error = receive_json(&mut first_socket).await;
    assert_responses_error(&error, "weekly_quota_exceeded", None);

    let rows = request_log_rows(&relay.database_path, 3).await;
    assert_eq!(
        rows,
        vec![
            ("started".to_string(), "websocket".to_string(), true),
            ("completed".to_string(), "websocket".to_string(), true),
            ("rejected".to_string(), "websocket".to_string(), true),
        ]
    );
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_errors_are_not_forwarded_and_leave_the_connection_usable() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    let invalid_requests = [
        (json!({"type": "session.update", "model": MODEL}), "type"),
        (
            json!({"type": "response.create", "model": "not-allowed", "input": []}),
            "model",
        ),
        (
            json!({"type": "response.create", "model": MODEL, "input": [], "stream": true}),
            "stream",
        ),
        (
            json!({"type": "response.create", "model": MODEL, "input": [], "background": true}),
            "background",
        ),
        (
            json!({"type": "response.create", "model": MODEL, "input": [], "store": true}),
            "store",
        ),
        (
            json!({"type": "response.create", "model": MODEL, "input": [], "max_output_tokens": 64}),
            "max_output_tokens",
        ),
    ];
    for (request, param) in invalid_requests {
        send_json(&mut socket, &request).await;
        let error = receive_json(&mut socket).await;
        assert_responses_error(&error, "invalid_request_error", Some(param));
    }

    let valid = response_create("connection survived validation", None);
    send_json(&mut socket, &valid).await;
    let forwarded = upstream.expect_text(connection.id).await;
    assert_eq!(
        forwarded["input"][0]["content"][0]["text"],
        "connection survived validation"
    );
    let terminal = response_completed("resp-after-validation", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_is_checked_per_operation_and_rejections_keep_the_socket_open() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            weekly_limit_usd: Some("0.000001"),
            ..RelayOptions::enabled()
        },
    )
    .await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    send_json(&mut socket, &response_create("cross quota", None)).await;
    let _ = upstream.expect_text(connection.id).await;
    let terminal = response_completed("resp-costly", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);

    for prompt in ["over quota", "still connected"] {
        send_json(&mut socket, &response_create(prompt, Some("resp-costly"))).await;
        let error = receive_json(&mut socket).await;
        assert_responses_error(&error, "weekly_quota_exceeded", None);
    }

    let rows = request_log_rows(&relay.database_path, 3).await;
    assert_eq!(
        rows,
        vec![
            ("completed".to_string(), "websocket".to_string(), true),
            ("rejected".to_string(), "websocket".to_string(), true),
            ("rejected".to_string(), "websocket".to_string(), true),
        ]
    );
    assert_eq!(upstream.connection_count(), 1);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_quota_exhaustion_rewrites_websocket_creates_to_the_fallback_model() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            weekly_limit_usd: Some("0.000001"),
            hard_limit_usd: Some("600.00"),
            fallback_model: Some(FALLBACK_MODEL),
            ..RelayOptions::enabled()
        },
    )
    .await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    send_json(&mut socket, &response_create("primary model", None)).await;
    let first = upstream.expect_text(connection.id).await;
    assert_eq!(first["model"], MODEL);
    let first_terminal = response_completed("resp-primary", 1, 0, 0);
    connection.send_json(first_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, first_terminal);

    send_json(&mut socket, &response_create("should fall back", None)).await;
    let second = upstream.expect_text(connection.id).await;
    assert_eq!(second["model"], FALLBACK_MODEL);
    let second_terminal = response_completed("resp-fallback", 1, 0, 0);
    connection.send_json(second_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, second_terminal);

    let options = SqliteConnectOptions::new().filename(&relay.database_path);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open websocket fallback logs");
    let models = sqlx::query("SELECT model FROM request_logs ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read fallback models")
        .into_iter()
        .map(|row| row.get::<String, _>("model"))
        .collect::<Vec<_>>();
    assert_eq!(models, [MODEL.to_owned(), FALLBACK_MODEL.to_owned()]);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_quota_exhaustion_rejects_websocket_creates_after_fallback() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            weekly_limit_usd: Some("0.000001"),
            hard_limit_usd: Some("0.00000105"),
            fallback_model: Some(FALLBACK_MODEL),
            ..RelayOptions::enabled()
        },
    )
    .await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    send_json(&mut socket, &response_create("primary model", None)).await;
    let first = upstream.expect_text(connection.id).await;
    assert_eq!(first["model"], MODEL);
    let first_terminal = response_completed("resp-primary", 1, 0, 0);
    connection.send_json(first_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, first_terminal);

    send_json(&mut socket, &response_create("fallback once", None)).await;
    let second = upstream.expect_text(connection.id).await;
    assert_eq!(second["model"], FALLBACK_MODEL);
    let second_terminal = response_completed("resp-fallback", 1, 0, 0);
    connection.send_json(second_terminal.clone());
    assert_eq!(receive_json(&mut socket).await, second_terminal);

    send_json(&mut socket, &response_create("hard stop", None)).await;
    let error = receive_json(&mut socket).await;
    assert_responses_error(&error, "weekly_quota_exceeded", None);

    let rows = request_log_rows(&relay.database_path, 3).await;
    assert_eq!(
        rows,
        vec![
            ("completed".to_string(), "websocket".to_string(), true),
            ("completed".to_string(), "websocket".to_string(), true),
            ("rejected".to_string(), "websocket".to_string(), true),
        ]
    );

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_text_is_rejected_without_closing_or_contacting_upstream() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade malformed-text socket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    socket
        .send(Message::Text("{not-json".into()))
        .await
        .expect("send malformed text frame");
    let error = receive_json(&mut socket).await;
    assert_responses_error(&error, "invalid_request_error", None);
    assert!(error["param"].is_null());
    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("rejected".to_string(), "websocket".to_string(), true)]
    );
    assert!(
        timeout(Duration::from_millis(100), upstream.events.recv())
            .await
            .is_err(),
        "malformed downstream text reached the upstream WebSocket"
    );

    send_json(
        &mut socket,
        &response_create("valid after malformed text", None),
    )
    .await;
    let forwarded = upstream.expect_text(connection.id).await;
    assert_eq!(
        forwarded["input"][0]["content"][0]["text"],
        "valid after malformed text"
    );
    let terminal = response_completed("resp-after-malformed-text", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);
    let rows = request_log_rows(&relay.database_path, 2).await;
    assert_eq!(
        rows,
        vec![
            ("rejected".to_string(), "websocket".to_string(), true),
            ("completed".to_string(), "websocket".to_string(), true),
        ]
    );
    assert_eq!(upstream.connection_count(), 1);

    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_application_frames_close_with_1003() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut binary = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade binary socket");
    let _ = upstream.expect_handshake().await;
    let binary_upstream = upstream.expect_connection().await;
    binary
        .send(Message::Binary(vec![0, 159, 146, 150].into()))
        .await
        .expect("send binary application frame");
    let close = receive_close(&mut binary).await;
    assert_eq!(close.code, CloseCode::Unsupported);
    let _ = upstream.expect_close(binary_upstream.id).await;
    assert_eq!(upstream.connection_count(), 1);
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_downstream_cancels_the_in_flight_upstream_operation() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(&mut socket, &response_create("cancel me", None)).await;
    let _ = upstream.expect_text(connection.id).await;
    connection.send_json(json!({
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp-cancel", "status": "in_progress"}
    }));
    let _ = receive_json(&mut socket).await;

    socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "client cancellation".into(),
        })))
        .await
        .expect("send downstream cancellation close");
    let _ = upstream.expect_close(connection.id).await;
    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("canceled".to_string(), "websocket".to_string(), true)]
    );
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_close_handshakes_are_completed_in_both_directions() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;

    let mut downstream_initiated = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream-initiated close socket");
    let _ = upstream.expect_handshake().await;
    let first_connection = upstream.expect_connection().await;
    downstream_initiated
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "downstream complete".into(),
        })))
        .await
        .expect("send downstream close");
    let downstream_reply = receive_close(&mut downstream_initiated).await;
    assert_eq!(downstream_reply.code, CloseCode::Normal);
    assert_eq!(downstream_reply.reason, "downstream complete");
    assert_eq!(upstream.expect_close(first_connection.id).await, Some(1000));

    let mut upstream_initiated = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade upstream-initiated close socket");
    let _ = upstream.expect_handshake().await;
    let second_connection = upstream.expect_connection().await;
    second_connection.close(1000, "upstream complete");
    let upstream_close = receive_close(&mut upstream_initiated).await;
    assert_eq!(upstream_close.code, CloseCode::Normal);
    assert_eq!(upstream_close.reason, "upstream complete");
    assert_eq!(
        upstream.expect_close(second_connection.id).await,
        Some(1000)
    );

    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_abnormal_upstream_close_is_mapped_to_1011() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;

    connection.close(1002, "upstream protocol violation");
    let close = receive_close(&mut socket).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(upstream.expect_close(connection.id).await, Some(1002));
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abnormal_upstream_disconnect_closes_downstream_with_1011() {
    let mut upstream = FakeUpstream::start(None, "unused-refresh-token").await;
    let relay = RelayProcess::start(&upstream, RelayOptions::enabled()).await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("upgrade downstream WebSocket");
    let _ = upstream.expect_handshake().await;
    let connection = upstream.expect_connection().await;
    send_json(&mut socket, &response_create("upstream disappears", None)).await;
    let _ = upstream.expect_text(connection.id).await;

    connection.abort();
    let close = receive_close(&mut socket).await;
    assert_eq!(close.code, CloseCode::Error);
    let rows = request_log_rows(&relay.database_path, 1).await;
    assert_eq!(
        rows,
        vec![("upstream_error".to_string(), "websocket".to_string(), true)]
    );
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_handshake_401_refreshes_credentials_and_retries_once() {
    let stale_access_token = jwt(json!({"exp": 4_102_444_800_u64, "token": "stale"}));
    let fresh_access_token = jwt(json!({"exp": 4_102_444_800_u64, "token": "fresh"}));
    let mut upstream = FakeUpstream::start(Some(&fresh_access_token), &fresh_access_token).await;
    let relay = RelayProcess::start(
        &upstream,
        RelayOptions {
            access_token: stale_access_token.clone(),
            ..RelayOptions::enabled()
        },
    )
    .await;
    let mut socket = connect_downstream(
        &relay.websocket_url(),
        Some(&format!("Bearer {CLIENT_KEY}")),
    )
    .await
    .expect("downstream upgrade should survive one upstream 401");

    let (first_headers, accepted) = upstream.expect_handshake().await;
    assert!(!accepted);
    assert_eq!(
        first_headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some(format!("Bearer {stale_access_token}").as_str())
    );
    match upstream.next_event().await {
        UpstreamEvent::OAuth { headers, body } => {
            assert_eq!(
                headers
                    .get("content-type")
                    .and_then(|value| value.to_str().ok()),
                Some("application/json")
            );
            assert_eq!(body["client_id"], OAUTH_CLIENT_ID);
            assert_eq!(body["grant_type"], "refresh_token");
            assert_eq!(body["refresh_token"], "upstream-refresh-token");
        }
        event => panic!("expected OAuth refresh after upstream 401, got {event:?}"),
    }
    let (second_headers, accepted) = upstream.expect_handshake().await;
    assert!(accepted);
    assert_eq!(
        second_headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some(format!("Bearer {fresh_access_token}").as_str())
    );
    let connection = upstream.expect_connection().await;
    assert_eq!(upstream.handshake_count(), 2);
    assert_eq!(upstream.connection_count(), 1);

    send_json(&mut socket, &response_create("after refresh", None)).await;
    let forwarded = upstream.expect_text(connection.id).await;
    assert_eq!(forwarded["model"], MODEL);
    let terminal = response_completed("resp-refreshed", 1, 0, 1);
    connection.send_json(terminal.clone());
    assert_eq!(receive_json(&mut socket).await, terminal);
    socket.close(None).await.expect("close downstream socket");
    let _ = upstream.expect_close(connection.id).await;
    relay.stop().await;
}
