use std::{convert::Infallible, net::SocketAddr, process::Stdio, time::Duration};

use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    routing::get,
};
use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::mpsc,
    time::{Instant, sleep},
};
use tokio_stream::iter;

const KEY: &str = "sk-test";
const MODEL: &str = "gpt-5.6-luna";

#[derive(Clone)]
struct UpstreamState(mpsc::UnboundedSender<Value>);

struct Upstream {
    addr: SocketAddr,
    requests: mpsc::UnboundedReceiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

impl Upstream {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let app = Router::new()
            .route("/models", get(models))
            .route("/responses", get(responses))
            .with_state(UpstreamState(tx));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            addr,
            requests: rx,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn models() -> Json<Value> {
    Json(json!({"models": [{
        "slug": MODEL, "visibility": "list", "use_responses_lite": true,
        "base_instructions": "Follow the user's instructions."
    }]}))
}

async fn responses(
    State(state): State<UpstreamState>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| serve(socket, state))
}

async fn serve(mut socket: WebSocket, state: UpstreamState) {
    let Message::Text(text) = socket.recv().await.unwrap().unwrap() else {
        return;
    };
    state.0.send(serde_json::from_str(&text).unwrap()).unwrap();
    socket
        .send(Message::Text(
            json!({"type":"response.completed","response":{
        "id":"resp-prewarm","status":"completed","output":[],"usage":{
            "input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":0,
            "output_tokens_details":{"reasoning_tokens":0},"total_tokens":1}}})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let Message::Text(text) = socket.recv().await.unwrap().unwrap() else {
        return;
    };
    state.0.send(serde_json::from_str(&text).unwrap()).unwrap();
    for event in [
        json!({"type":"response.output_text.done","text":"OK"}),
        json!({"type":"response.completed","response":{"id":"resp-turn","status":"completed","output":[],"usage":{
            "input_tokens":2,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,
            "output_tokens_details":{"reasoning_tokens":0},"total_tokens":3}}}),
    ] {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .unwrap();
    }
}

struct Relay {
    addr: SocketAddr,
    child: Child,
    _temp: TempDir,
}

impl Relay {
    async fn start(upstream: &Upstream) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let auth = temp.path().join("auth.json");
        let state = temp.path().join("state.sqlite3");
        let config = temp.path().join("config.toml");
        std::fs::write(&auth, json!({"auth_mode":"chatgpt","tokens":{
            "id_token":"id","access_token":"access","refresh_token":"refresh","account_id":"account"},
            "last_refresh":"2099-01-01T00:00:00Z"}).to_string()).unwrap();
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        std::fs::write(
            &config,
            format!(
                r#"[server]
listen = "{addr}"
[state]
path = "{}"
[upstream]
base_url = "{}"
auth_file = "{}"
[[api_keys]]
id = "test"
secret = "{KEY}"
[model_prices."{MODEL}"]
input_usd_per_million = "1"
cached_input_usd_per_million = "0.1"
output_usd_per_million = "6"
"#,
                state.display(),
                upstream.base_url(),
                auth.display()
            ),
        )
        .unwrap();
        drop(reservation);
        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
            .args(["--config", config.to_str().unwrap(), "serve"])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(addr).await.is_err() {
            assert!(child.try_wait().unwrap().is_none());
            assert!(Instant::now() < deadline);
            sleep(Duration::from_millis(20)).await;
        }
        Self {
            addr,
            child,
            _temp: temp,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/v1/responses", self.addr)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[tokio::test]
async fn http_responses_use_websocket_lite_prewarm_and_return_sse() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let response = reqwest::Client::new()
        .post(relay.url())
        .bearer_auth(KEY)
        .json(&json!({"model":MODEL,"input":"Reply with OK.","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut events =
        Box::pin(iter([Ok::<Bytes, Infallible>(response.bytes().await.unwrap())]).eventsource());
    let mut data = Vec::new();
    while let Some(event) = events.next().await {
        data.push(event.unwrap().data);
    }
    assert!(data.iter().any(|event| event.contains("\"text\":\"OK\"")));
    assert!(data.last().unwrap().contains("response.completed"));
    let prewarm = upstream.requests.recv().await.unwrap();
    assert_eq!(prewarm["generate"], false);
    assert_eq!(prewarm["input"][0]["type"], "additional_tools");
    let turn = upstream.requests.recv().await.unwrap();
    assert_eq!(turn["previous_response_id"], "resp-prewarm");
    assert_eq!(turn["reasoning"]["context"], "all_turns");
    assert_eq!(turn["input"][0]["content"][0]["text"], "Reply with OK.");
}

#[tokio::test]
async fn stream_true_is_validated_before_contacting_upstream() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let response = reqwest::Client::new()
        .post(relay.url())
        .bearer_auth(KEY)
        .json(&json!({"model":MODEL,"input":"hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(upstream.requests.try_recv().is_err());
}
