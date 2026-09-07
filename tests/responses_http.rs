use std::{
    convert::Infallible,
    net::SocketAddr,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::HeaderMap,
    routing::get,
};
use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    sync::mpsc,
    time::{Instant, sleep, timeout},
};
use tokio_stream::iter;

const KEY: &str = "sk-test";
const MODEL: &str = "gpt-5.6-luna";

#[derive(Clone)]
struct UpstreamState {
    connections: mpsc::UnboundedSender<Connection>,
    count: Arc<AtomicUsize>,
}

struct Upstream {
    addr: SocketAddr,
    connections: mpsc::UnboundedReceiver<Connection>,
    count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Upstream {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/models", get(models))
            .route("/responses", get(responses))
            .with_state(UpstreamState {
                connections: tx,
                count: Arc::clone(&count),
            });
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            addr,
            connections: rx,
            count,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn accept(&mut self) -> Connection {
        timeout(Duration::from_secs(5), self.connections.recv())
            .await
            .unwrap()
            .unwrap()
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
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| serve(socket, state, headers))
}

struct Connection {
    headers: HeaderMap,
    incoming: mpsc::UnboundedReceiver<Message>,
    outgoing: mpsc::UnboundedSender<Message>,
}

impl Connection {
    async fn request(&mut self) -> Value {
        let message = timeout(Duration::from_secs(5), self.incoming.recv())
            .await
            .unwrap()
            .unwrap();
        let Message::Text(text) = message else {
            panic!("expected response.create, received {message:?}")
        };
        serde_json::from_str(&text).unwrap()
    }

    fn send(&self, value: Value) {
        self.outgoing
            .send(Message::Text(value.to_string().into()))
            .unwrap();
    }

    fn complete(&self, id: &str, output: Value) {
        self.send(json!({"type":"response.completed","response":{"id":id,"status":"completed","output":output,"usage":{
            "input_tokens":2,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,
            "output_tokens_details":{"reasoning_tokens":0},"total_tokens":3}}}));
    }

    async fn prewarm(&mut self) -> Value {
        let prewarm = self.request().await;
        assert_eq!(prewarm["generate"], false);
        assert!(prewarm.get("previous_response_id").is_none());
        self.complete("resp-prewarm", json!([]));
        prewarm
    }

    async fn closed(&mut self) {
        let next = timeout(Duration::from_secs(5), self.incoming.recv())
            .await
            .unwrap();
        assert!(
            matches!(next, None | Some(Message::Close(_))),
            "unexpected message: {next:?}"
        );
    }
}

async fn serve(mut socket: WebSocket, state: UpstreamState, headers: HeaderMap) {
    let (incoming, received) = mpsc::unbounded_channel();
    let (outgoing, mut commands) = mpsc::unbounded_channel();
    state.count.fetch_add(1, Ordering::SeqCst);
    state
        .connections
        .send(Connection {
            headers,
            incoming: received,
            outgoing,
        })
        .unwrap();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if socket.send(command).await.is_err() { break; }
            }
            message = socket.recv() => {
                let Some(Ok(message)) = message else { break };
                let closed = matches!(message, Message::Close(_));
                if incoming.send(message).is_err() || closed { break; }
            }
        }
    }
}

struct Relay {
    addr: SocketAddr,
    child: Child,
    _temp: TempDir,
}

impl Relay {
    fn configuration(upstream: &Upstream) -> (TempDir, SocketAddr) {
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
[[api_keys]]
id = "other"
secret = "sk-other"
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
        (temp, addr)
    }

    async fn start(upstream: &Upstream) -> Self {
        let (temp, addr) = Self::configuration(upstream);
        let config = temp.path().join("config.toml");
        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
            .args(["--config", config.to_str().unwrap(), "serve"])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if client
                .get(format!("http://{addr}/v1/responses"))
                .send()
                .await
                .is_ok_and(|response| response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED)
            {
                break;
            }
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

    fn post(&self, session: Option<&str>, body: Value) -> reqwest::RequestBuilder {
        let request = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
            .post(self.url())
            .bearer_auth(KEY)
            .json(&body);
        match session {
            Some(session) => request.header("thread_id", session),
            None => request,
        }
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
    let response = spawn_response(
        relay
            .post(
                None,
                json!({"model":MODEL,"input":"Reply with OK.","stream":true}),
            )
            .send(),
    );
    let mut connection = upstream.accept().await;
    let prewarm = connection.prewarm().await;
    let turn = connection.request().await;
    connection.send(json!({"type":"response.output_text.done","text":"OK"}));
    connection.complete("resp-turn", json!([]));
    let response = response.await.unwrap().unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut events =
        Box::pin(iter([Ok::<Bytes, Infallible>(response.bytes().await.unwrap())]).eventsource());
    let mut data = Vec::new();
    while let Some(event) = events.next().await {
        data.push(event.unwrap().data);
    }
    assert!(data.iter().any(|event| event.contains("\"text\":\"OK\"")));
    assert!(data.last().unwrap().contains("response.completed"));
    assert_eq!(prewarm["generate"], false);
    assert_eq!(prewarm["input"][0]["type"], "additional_tools");
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
    assert!(upstream.connections.try_recv().is_err());
}

fn spawn_response(
    response: impl std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>
    + Send
    + 'static,
) -> tokio::task::JoinHandle<Result<reqwest::Response, reqwest::Error>> {
    tokio::spawn(async move {
        let response = response
            .await
            .inspect_err(|error| eprintln!("HTTP request failed: {error:?}"))?;
        let status = response.status();
        assert!(
            status.is_success(),
            "unexpected HTTP {status}: {}",
            response.text().await.unwrap()
        );
        Ok(response)
    })
}

fn user(text: &str) -> Value {
    json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]})
}

fn assistant(text: &str) -> Value {
    json!({"type":"message","id":format!("msg-{text}"),"status":"completed","role":"assistant",
        "content":[{"type":"output_text","text":text,"annotations":[]}]})
}

fn body(input: Value) -> Value {
    json!({"model":MODEL,"input":input,"stream":true})
}

async fn finish(
    response: tokio::task::JoinHandle<Result<reqwest::Response, reqwest::Error>>,
) -> Value {
    let response = response.await.unwrap().unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut events =
        Box::pin(iter([Ok::<Bytes, Infallible>(response.bytes().await.unwrap())]).eventsource());
    let mut last = None;
    while let Some(event) = events.next().await {
        last = Some(serde_json::from_str(&event.unwrap().data).unwrap());
    }
    last.unwrap()
}

#[tokio::test]
async fn full_history_and_explicit_continuations_share_one_connection_and_one_prewarm() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .header("x-codex-turn-state", "state-one")
            .send(),
    );
    let mut connection = upstream.accept().await;
    assert!(!connection.headers.contains_key("x-codex-turn-state"));
    connection.prewarm().await;
    let initial = connection.request().await;
    assert_eq!(
        initial["client_metadata"]["x-codex-turn-state"],
        "state-one"
    );
    connection.complete("resp-one", json!([assistant("one")]));
    finish(first).await;

    let second = spawn_response(
        relay
            .post(
                Some("thread"),
                body(json!([user("one"), assistant("one"), user("two")])),
            )
            .header("x-codex-turn-metadata", "metadata-two")
            .send(),
    );
    let delta = connection.request().await;
    assert_eq!(delta["previous_response_id"], "resp-one");
    assert_eq!(delta["input"], json!([user("two")]));
    assert!(delta.get("generate").is_none());
    assert!(delta["client_metadata"].get("x-codex-turn-state").is_none());
    assert_eq!(
        delta["client_metadata"]["x-codex-turn-metadata"],
        "metadata-two"
    );
    assert_eq!(delta["client_metadata"]["thread_id"], "thread");
    assert_eq!(delta["prompt_cache_key"], initial["prompt_cache_key"]);
    connection.complete("resp-two", json!([assistant("two")]));
    finish(second).await;

    let mut third_body = body(json!([user("three")]));
    third_body["previous_response_id"] = json!("resp-two");
    let third = spawn_response(relay.post(Some("thread"), third_body).send());
    let delta = connection.request().await;
    assert_eq!(delta["previous_response_id"], "resp-two");
    assert_eq!(delta["input"], json!([user("three")]));
    connection.complete("resp-three", json!([assistant("three")]));
    finish(third).await;

    let fourth = spawn_response(
        relay
            .post(
                Some("thread"),
                body(json!([
                    user("one"),
                    assistant("one"),
                    user("two"),
                    assistant("two"),
                    user("three"),
                    assistant("three"),
                    user("four")
                ])),
            )
            .send(),
    );
    let delta = connection.request().await;
    assert_eq!(delta["previous_response_id"], "resp-three");
    assert_eq!(delta["input"], json!([user("four")]));
    connection.complete("resp-four", json!([]));
    finish(fourth).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn changed_parameters_and_edited_history_start_new_context_on_the_same_socket() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([assistant("one")]));
    finish(first).await;

    let mut changed = body(json!([user("two")]));
    changed["previous_response_id"] = json!("resp-one");
    changed["instructions"] = json!("Answer in Chinese.");
    let second = spawn_response(relay.post(Some("thread"), changed).send());
    let prewarm = connection.prewarm().await;
    assert_eq!(
        prewarm["input"][2]["content"][0]["text"],
        "Answer in Chinese."
    );
    let rebuilt = connection.request().await;
    assert_eq!(
        rebuilt["input"],
        json!([user("one"), assistant("one"), user("two")])
    );
    assert_eq!(rebuilt["previous_response_id"], "resp-prewarm");
    connection.complete("resp-two", json!([assistant("two")]));
    finish(second).await;

    let mut edited = body(json!([user("edited")]));
    edited["instructions"] = json!("Answer in Chinese.");
    let third = spawn_response(relay.post(Some("thread"), edited).send());
    connection.prewarm().await;
    let fresh = connection.request().await;
    assert_eq!(fresh["input"], json!([user("edited")]));
    connection.complete("resp-three", json!([]));
    finish(third).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn disconnected_session_rebuilds_tool_context_for_an_explicit_continuation() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let tools =
        json!([{"type":"function","name":"lookup","parameters":{"type":"object","properties":{}}}]);
    let call = json!({"type":"function_call","id":"fc-one","call_id":"call-one","name":"lookup","arguments":"{}","status":"completed"});
    let output = json!({"type":"function_call_output","call_id":"call-one","output":"found"});
    let mut first_body = body(json!([user("lookup")]));
    first_body["tools"] = tools.clone();
    let first = spawn_response(relay.post(Some("thread"), first_body).send());
    let mut connection = upstream.accept().await;
    let prewarm = connection.prewarm().await;
    assert_eq!(prewarm["input"][0]["tools"], tools);
    connection.request().await;
    connection.complete("resp-one", json!([call]));
    finish(first).await;
    connection.outgoing.send(Message::Close(None)).unwrap();
    connection.closed().await;

    let mut second_body = body(json!([output]));
    second_body["previous_response_id"] = json!("resp-one");
    second_body["tools"] = tools;
    let second = spawn_response(relay.post(Some("thread"), second_body).send());
    let mut replacement = upstream.accept().await;
    replacement.prewarm().await;
    let rebuilt = replacement.request().await;
    assert_eq!(rebuilt["input"], json!([user("lookup"), call, output]));
    assert_eq!(rebuilt["previous_response_id"], "resp-prewarm");
    replacement.complete("resp-two", json!([assistant("found")]));
    finish(second).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn sessions_are_isolated_by_identity_and_header_and_missing_ids_do_not_discard_context() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("secret")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([assistant("secret")]));
    finish(first).await;

    let mut continuation = body(json!([user("next")]));
    continuation["previous_response_id"] = json!("resp-one");
    for request in [
        reqwest::Client::new()
            .post(relay.url())
            .bearer_auth("sk-other")
            .header("thread_id", "thread")
            .json(&continuation),
        relay.post(Some("different"), continuation.clone()),
        relay.post(None, continuation.clone()),
        relay
            .post(None, continuation.clone())
            .header("session_id", "thread"),
    ] {
        let rejected = request.send().await.unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(
            rejected.json::<Value>().await.unwrap()["error"]["param"],
            "previous_response_id"
        );
    }
    let mut unknown = continuation.clone();
    unknown["previous_response_id"] = json!("unknown");
    assert_eq!(
        relay
            .post(Some("thread"), unknown)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let next = spawn_response(relay.post(Some("thread"), continuation).send());
    let delta = connection.request().await;
    assert_eq!(delta["previous_response_id"], "resp-one");
    assert_eq!(delta["input"], json!([user("next")]));
    connection.complete("resp-two", json!([]));
    finish(next).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_session_requests_are_rejected_and_other_sessions_remain_independent() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    let rejected = relay
        .post(Some("thread"), body(json!([user("overlap")])))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        rejected.json::<Value>().await.unwrap()["error"]["code"],
        "session_busy"
    );
    let other = spawn_response(
        relay
            .post(Some("other"), body(json!([user("other")])))
            .send(),
    );
    let mut other_connection = upstream.accept().await;
    other_connection.prewarm().await;
    other_connection.request().await;
    other_connection.complete("resp-other", json!([]));
    finish(other).await;
    connection.complete("resp-one", json!([assistant("one")]));
    finish(first).await;
    let next = spawn_response(
        relay
            .post(
                Some("thread"),
                body(json!([user("one"), assistant("one"), user("two")])),
            )
            .send(),
    );
    assert_eq!(
        connection.request().await["previous_response_id"],
        "resp-one"
    );
    connection.complete("resp-two", json!([]));
    finish(next).await;
}

#[tokio::test]
async fn failed_generation_preserves_the_last_successful_snapshot() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([assistant("one")]));
    finish(first).await;
    let second = spawn_response(
        relay
            .post(
                Some("thread"),
                body(json!([user("one"), assistant("one"), user("failed")])),
            )
            .send(),
    );
    connection.request().await;
    connection.send(json!({"type":"response.failed","response":{"id":"resp-failed","status":"failed","error":{"code":"server_error","message":"failed"}}}));
    assert_eq!(finish(second).await["type"], "response.failed");
    connection.closed().await;
    let mut retry = body(json!([user("retry")]));
    retry["previous_response_id"] = json!("resp-one");
    let third = spawn_response(relay.post(Some("thread"), retry).send());
    let mut replacement = upstream.accept().await;
    replacement.prewarm().await;
    assert_eq!(
        replacement.request().await["input"],
        json!([user("one"), assistant("one"), user("retry")])
    );
    replacement.complete("resp-three", json!([]));
    finish(third).await;
}

#[tokio::test]
async fn canceling_a_stream_discards_its_socket_but_keeps_successful_history() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([assistant("one")]));
    finish(first).await;
    let second = spawn_response(
        relay
            .post(
                Some("thread"),
                body(json!([user("one"), assistant("one"), user("canceled")])),
            )
            .send(),
    );
    connection.request().await;
    connection.send(json!({"type":"response.output_text.delta","delta":"partial"}));
    let mut response = second.await.unwrap().unwrap();
    assert!(response.chunk().await.unwrap().is_some());
    drop(response);
    connection.closed().await;
    let mut retry = body(json!([user("retry")]));
    retry["previous_response_id"] = json!("resp-one");
    let third = spawn_response(relay.post(Some("thread"), retry).send());
    let mut replacement = upstream.accept().await;
    replacement.prewarm().await;
    assert_eq!(
        replacement.request().await["input"],
        json!([user("one"), assistant("one"), user("retry")])
    );
    replacement.complete("resp-three", json!([]));
    finish(third).await;
}

#[tokio::test]
async fn idle_connection_answers_ping_and_session_id_also_enables_reuse() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(None, body(json!([user("one")])))
            .header("session_id", "session")
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([]));
    finish(first).await;
    connection
        .outgoing
        .send(Message::Ping(Bytes::from_static(b"idle")))
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), connection.incoming.recv())
            .await
            .unwrap(),
        Some(Message::Pong(Bytes::from_static(b"idle")))
    );
    let next = spawn_response(
        relay
            .post(None, body(json!([user("one"), user("two")])))
            .header("session_id", "session")
            .send(),
    );
    assert_eq!(
        connection.request().await["previous_response_id"],
        "resp-one"
    );
    connection.complete("resp-two", json!([]));
    finish(next).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idle_sessions_expire_and_cannot_continue_an_evicted_response() {
    let mut upstream = Upstream::start().await;
    let (temp, addr) = Relay::configuration(&upstream);
    let config = temp.path().join("config.toml");
    let service = tokio::spawn(async move { codex_api::run(&config).await.unwrap() });
    let deadline = Instant::now() + Duration::from_secs(5);
    let readiness_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    while !readiness_client
        .get(format!("http://{addr}/v1/responses"))
        .send()
        .await
        .is_ok_and(|response| response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED)
    {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    }
    let url = format!("http://{addr}/v1/responses");
    let client = reqwest::Client::new();
    let first = spawn_response(
        client
            .post(&url)
            .bearer_auth(KEY)
            .header("thread_id", "thread")
            .json(&body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([]));
    finish(first).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(601)).await;
    tokio::time::resume();
    connection.closed().await;
    let mut continuation = body(json!([user("next")]));
    continuation["previous_response_id"] = json!("resp-one");
    let response = client
        .post(&url)
        .bearer_auth(KEY)
        .header("thread_id", "thread")
        .json(&continuation)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(upstream.count.load(Ordering::SeqCst), 1);
    service.abort();
}

#[tokio::test]
async fn cache_capacity_evicts_the_oldest_idle_connection() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let mut connections = Vec::new();
    for index in 0..65 {
        let response = spawn_response(
            relay
                .post(Some(&format!("thread-{index}")), body(json!([user("one")])))
                .send(),
        );
        let mut connection = upstream.accept().await;
        connection.prewarm().await;
        connection.request().await;
        connection.complete(&format!("resp-{index}"), json!([]));
        finish(response).await;
        connections.push(connection);
    }
    connections[0].closed().await;
    let mut continuation = body(json!([user("next")]));
    continuation["previous_response_id"] = json!("resp-64");
    let next = spawn_response(relay.post(Some("thread-64"), continuation).send());
    assert_eq!(
        connections[64].request().await["previous_response_id"],
        "resp-64"
    );
    connections[64].complete("resp-next", json!([]));
    finish(next).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 65);
}

#[tokio::test]
async fn requests_without_session_headers_close_their_connections_after_completion() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    for _ in 0..2 {
        let response = spawn_response(relay.post(None, body(json!([user("one")]))).send());
        let mut connection = upstream.accept().await;
        connection.prewarm().await;
        connection.request().await;
        connection.complete("resp-one", json!([]));
        finish(response).await;
        connection.closed().await;
    }
    assert_eq!(upstream.count.load(Ordering::SeqCst), 2);
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_closes_idle_sessions_and_finishes_their_ledger_rows() {
    let mut upstream = Upstream::start().await;
    let mut relay = Relay::start(&upstream).await;
    let response = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection.complete("resp-one", json!([]));
    finish(response).await;
    let status = Command::new("kill")
        .args(["-TERM", &relay.child.id().unwrap().to_string()])
        .status()
        .await
        .unwrap();
    assert!(status.success());
    assert!(
        timeout(Duration::from_secs(5), relay.child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    connection.closed().await;
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(relay._temp.path().join("state.sqlite3"))
        .read_only(true);
    let mut database = <sqlx::SqliteConnection as sqlx::Connection>::connect_with(&options)
        .await
        .unwrap();
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT status, input_tokens, output_tokens FROM request_logs ORDER BY id")
            .fetch_all(&mut database)
            .await
            .unwrap();
    assert_eq!(rows, vec![("completed".to_owned(), 2, 1)]);
}

#[tokio::test]
async fn private_completed_items_are_cached_without_rewriting_the_terminal_sse_event() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let first = spawn_response(
        relay
            .post(Some("thread"), body(json!([user("one")])))
            .send(),
    );
    let mut connection = upstream.accept().await;
    connection.prewarm().await;
    connection.request().await;
    connection
        .send(json!({"type":"response.output_item.done","output_index":0,"item":assistant("one")}));
    connection.complete("resp-one", json!([]));
    assert_eq!(finish(first).await["response"]["output"], json!([]));
    let second = spawn_response(
        relay
            .post(
                Some("thread"),
                body(json!([user("one"), assistant("one"), user("two")])),
            )
            .send(),
    );
    let delta = connection.request().await;
    assert_eq!(delta["previous_response_id"], "resp-one");
    assert_eq!(delta["input"], json!([user("two")]));
    connection.complete("resp-two", json!([]));
    finish(second).await;
}

#[tokio::test]
async fn hermes_prompt_cache_affinity_survives_http_to_websocket_conversion() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    for turn in 0..2 {
        // Hermes's custom Responses provider sends its cache scope in the body,
        // without Codex-specific session headers. Each HTTP request is independent.
        let mut request = body(json!([user(&format!("turn {turn}"))]));
        request["instructions"] = json!("A stable Hermes task prompt.");
        request["prompt_cache_key"] = json!("hermes-task-cache");
        let response = spawn_response(relay.post(None, request).send());
        let mut connection = upstream.accept().await;
        assert_eq!(
            connection
                .headers
                .get("session_id")
                .and_then(|value| value.to_str().ok()),
            Some("hermes-task-cache"),
            "body cache affinity must reach the upstream WebSocket handshake"
        );
        let prewarm = connection.prewarm().await;
        assert_eq!(prewarm["prompt_cache_key"], "hermes-task-cache");
        let generation = connection.request().await;
        assert_eq!(generation["prompt_cache_key"], "hermes-task-cache");
        connection.complete(&format!("resp-{turn}"), json!([]));
        finish(response).await;
    }
    assert_eq!(upstream.count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn explicit_session_affinity_takes_precedence_over_the_body_cache_key() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    for header in ["session_id", "session-id"] {
        let mut request = body(json!([user("hello")]));
        request["prompt_cache_key"] = json!("body-cache-key");
        let response = spawn_response(
            relay
                .post(None, request)
                .header(header, "explicit-session")
                .send(),
        );
        let mut connection = upstream.accept().await;
        assert_eq!(connection.headers[header], "explicit-session");
        if header == "session-id" {
            assert!(!connection.headers.contains_key("session_id"));
        }
        connection.prewarm().await;
        assert_eq!(
            connection.request().await["prompt_cache_key"],
            "body-cache-key"
        );
        connection.complete("resp-done", json!([]));
        finish(response).await;
    }
}

#[tokio::test]
async fn shared_prompt_cache_affinity_does_not_merge_or_block_parallel_tasks() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let mut first_body = body(json!([user("first task")]));
    first_body["prompt_cache_key"] = json!("shared-cache");
    let first = spawn_response(relay.post(None, first_body).send());
    let mut first_connection = upstream.accept().await;
    first_connection.prewarm().await;
    first_connection.request().await;
    // Keep the first generation active while an independent task uses the same cache scope.
    let mut second_body = body(json!([user("second task")]));
    second_body["prompt_cache_key"] = json!("shared-cache");
    let second = spawn_response(relay.post(None, second_body).send());
    let mut second_connection = upstream.accept().await;
    assert_eq!(second_connection.headers["session_id"], "shared-cache");
    second_connection.prewarm().await;
    let second_generation = second_connection.request().await;
    assert_eq!(second_generation["input"], json!([user("second task")]));
    second_connection.complete("resp-second", json!([]));
    finish(second).await;
    first_connection.complete("resp-first", json!([]));
    finish(first).await;
    assert_eq!(upstream.count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn body_cache_key_remains_valid_json_text_when_copied_to_ws_metadata() {
    let mut upstream = Upstream::start().await;
    let relay = Relay::start(&upstream).await;
    let mut request = body(json!([user("hello")]));
    request["prompt_cache_key"] = json!("任务缓存");
    let response = spawn_response(relay.post(None, request).send());
    let mut connection = upstream.accept().await;
    assert_eq!(
        connection.headers["session_id"].as_bytes(),
        "任务缓存".as_bytes()
    );
    let prewarm = connection.prewarm().await;
    assert_eq!(prewarm["client_metadata"]["session_id"], "任务缓存");
    connection.request().await;
    connection.complete("resp-done", json!([]));
    finish(response).await;
}
