use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use codex_api::{Clock, run_with_clock};
use serde_json::{Value, json};
use sqlx::{Connection, sqlite::SqliteConnectOptions};
use tempfile::TempDir;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, sleep},
};

const API_KEY: &str = "sk-models-test";
const API_KEY_ID: &str = "models-client";
const ACCESS_TOKEN: &str = "upstream-models-token";
const ACCOUNT_ID: &str = "upstream-models-account";
const ROTATED_ACCESS_TOKEN: &str = "rotated-upstream-models-token";
const MODEL: &str = "gpt-test";
const FALLBACK_MODEL: &str = "fallback-test";
const HIDDEN_MODEL: &str = "hidden-test";
const TEST_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone)]
struct FixedClock(Arc<StdMutex<OffsetDateTime>>);

impl FixedClock {
    fn at(value: &str) -> Self {
        Self(Arc::new(StdMutex::new(
            OffsetDateTime::parse(value, &Rfc3339).expect("valid fixed time"),
        )))
    }

    fn set(&self, value: &str) {
        *self.0.lock().expect("clock lock") =
            OffsetDateTime::parse(value, &Rfc3339).expect("valid fixed time");
    }
}

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        *self.0.lock().expect("clock lock")
    }
}

enum ModelsReply {
    Json(Value),
    Status(StatusCode),
    Raw(&'static str),
}

#[derive(Clone)]
struct UpstreamState {
    replies: Arc<Mutex<VecDeque<ModelsReply>>>,
    requests: Arc<Mutex<Vec<(Uri, HeaderMap)>>>,
    oauth_requests: Arc<Mutex<usize>>,
}

struct FakeUpstream {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<(Uri, HeaderMap)>>>,
    oauth_requests: Arc<Mutex<usize>>,
    task: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start(replies: Vec<ModelsReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let address = listener.local_addr().expect("fake upstream address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let oauth_requests = Arc::new(Mutex::new(0));
        let state = UpstreamState {
            replies: Arc::new(Mutex::new(replies.into())),
            requests: Arc::clone(&requests),
            oauth_requests: Arc::clone(&oauth_requests),
        };
        let app = Router::new()
            .route("/models", get(upstream_models))
            .route("/oauth/token", post(upstream_oauth))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve fake upstream");
        });
        Self {
            address,
            requests,
            oauth_requests,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    async fn requests(&self) -> Vec<(Uri, HeaderMap)> {
        self.requests.lock().await.clone()
    }

    async fn oauth_requests(&self) -> usize {
        *self.oauth_requests.lock().await
    }
}

async fn upstream_oauth(State(state): State<UpstreamState>) -> Json<Value> {
    *state.oauth_requests.lock().await += 1;
    Json(json!({
        "access_token": ROTATED_ACCESS_TOKEN,
        "refresh_token": "rotated-refresh-token"
    }))
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream_models(
    State(state): State<UpstreamState>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    state.requests.lock().await.push((uri, headers));
    match state
        .replies
        .lock()
        .await
        .pop_front()
        .expect("fake models reply exhausted")
    {
        ModelsReply::Json(value) => Json(value).into_response(),
        ModelsReply::Status(status) => status.into_response(),
        ModelsReply::Raw(body) => Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("build raw upstream response"),
    }
}

struct Fixture {
    _directory: TempDir,
    config_path: PathBuf,
    database_path: PathBuf,
}

impl Fixture {
    fn new(upstream: &FakeUpstream) -> Self {
        Self::with_fallback(upstream, true)
    }

    fn without_fallback(upstream: &FakeUpstream) -> Self {
        Self::with_fallback(upstream, false)
    }

    fn with_fallback(upstream: &FakeUpstream, fallback_configured: bool) -> Self {
        let directory = tempfile::tempdir().expect("create models fixture");
        let config_path = directory.path().join("config.toml");
        let auth_path = directory.path().join("auth.json");
        let database_path = directory.path().join("state.sqlite3");
        std::fs::write(
            &auth_path,
            json!({
                "auth_mode": "chatgpt",
                "OPENAI_API_KEY": null,
                "tokens": {
                    "id_token": "unused-id-token",
                    "access_token": ACCESS_TOKEN,
                    "refresh_token": "unused-refresh-token",
                    "account_id": ACCOUNT_ID
                },
                "last_refresh": "2026-08-10T00:00:00Z"
            })
            .to_string(),
        )
        .expect("write auth seed");
        let listen = unused_address();
        let fallback = if fallback_configured {
            format!("fallback_model = \"{FALLBACK_MODEL}\"\n\n")
        } else {
            String::new()
        };
        std::fs::write(
            &config_path,
            format!(
                r#"{fallback}[server]
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
id = "{API_KEY_ID}"
secret = "{API_KEY}"
weekly_limit_usd = "1.00"
hard_limit_usd = "2.00"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"

[model_prices."{FALLBACK_MODEL}"]
input_usd_per_million = "0.10"
cached_input_usd_per_million = "0.01"
output_usd_per_million = "0.20"

[model_prices."{HIDDEN_MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
                database_path.display(),
                upstream.base_url(),
                upstream.base_url(),
                auth_path.display(),
            ),
        )
        .expect("write models config");
        Self {
            _directory: directory,
            config_path,
            database_path,
        }
    }

    async fn start(&self, clock: FixedClock) -> Relay {
        let contents = std::fs::read_to_string(&self.config_path).expect("read models config");
        let listen = contents
            .lines()
            .find_map(|line| line.strip_prefix("listen = \"")?.strip_suffix('"'))
            .expect("configured listen")
            .parse()
            .expect("valid listen address");
        let config_path = self.config_path.clone();
        let task = tokio::spawn(async move { run_with_clock(&config_path, Arc::new(clock)).await });
        wait_until_listening(&task, listen).await;
        Relay { listen, task }
    }
}

struct Relay {
    listen: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Relay {
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.listen, path)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn catalog() -> Value {
    json!({
        "models": [
            {
                "slug": MODEL,
                "display_name": "GPT Test",
                "description": "primary model",
                "default_reasoning_level": "medium",
                "supported_reasoning_levels": [{"effort": "medium", "description": "Medium"}],
                "visibility": "list",
                "supported_in_api": false,
                "priority": 1,
                "context_window": 272000,
                "id": "must-be-overwritten",
                "owned_by": "must-be-overwritten"
            },
            {
                "slug": FALLBACK_MODEL,
                "display_name": "Fallback Test",
                "visibility": "list",
                "supported_in_api": true,
                "priority": 2,
                "context_window": 128000
            },
            {
                "slug": HIDDEN_MODEL,
                "display_name": "Hidden Test",
                "visibility": "hide",
                "supported_in_api": true,
                "priority": 3
            },
            {
                "slug": "unpriced-test",
                "display_name": "Unpriced Test",
                "visibility": "list",
                "supported_in_api": true,
                "priority": 4
            }
        ]
    })
}

fn unused_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    listener.local_addr().expect("reserved address")
}

async fn wait_until_listening(task: &JoinHandle<anyhow::Result<()>>, listen: SocketAddr) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        assert!(!task.is_finished(), "codex-api exited before listening");
        if TcpStream::connect(listen).await.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "codex-api did not start in time");
        sleep(Duration::from_millis(20)).await;
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(TEST_TIMEOUT)
        .build()
        .expect("build test client")
}

async fn get_models(relay: &Relay, path: &str, api_key: &str) -> reqwest::Response {
    client()
        .get(relay.url(path))
        .bearer_auth(api_key)
        .send()
        .await
        .expect("send models request")
}

async fn record_spend(path: &Path, cost_nano_usd: i64) {
    let mut database = sqlx::SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false),
    )
    .await
    .expect("open state database");
    sqlx::query(
        "INSERT INTO request_ledger (requested_at_ms, finished_at_ms, api_key_id, model, \
         api_protocol, transport, cost_nano_usd, duration_ms, status, http_status) \
         VALUES (1786356000000, 1786356000000, ?, ?, 'responses', 'http_sse', ?, 0, \
         'completed', 200)",
    )
    .bind(API_KEY_ID)
    .bind(MODEL)
    .bind(cost_nano_usd)
    .execute(&mut database)
    .await
    .expect("insert accounted spend");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn models_follow_upstream_visibility_local_pricing_and_live_quota_state() {
    let upstream = FakeUpstream::start(vec![ModelsReply::Json(catalog())]).await;
    let fixture = Fixture::new(&upstream);
    let clock = FixedClock::at("2026-08-10T12:00:00Z");
    let relay = fixture.start(clock).await;

    let unauthorized = get_models(&relay, "/v1/models", "wrong-key").await;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let (first, second) = tokio::join!(
        get_models(&relay, "/v1/models", API_KEY),
        get_models(&relay, "/v1/models", API_KEY),
    );
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    let body: Value = first.json().await.expect("decode models response");
    assert_eq!(body["object"], "list");
    assert_eq!(body["data"].as_array().expect("models array").len(), 2);
    assert_eq!(body["data"][0]["id"], MODEL);
    assert_eq!(body["data"][0]["object"], "model");
    assert_eq!(body["data"][0]["created"], 0);
    assert_eq!(body["data"][0]["owned_by"], "openai");
    assert_eq!(body["data"][0]["context_window"], 272000);
    assert_eq!(body["data"][0]["supported_in_api"], false);
    assert_eq!(body["data"][0]["description"], "primary model");
    assert_eq!(body["data"][1]["id"], FALLBACK_MODEL);

    let detail = get_models(&relay, &format!("/v1/models/{MODEL}"), API_KEY).await;
    assert_eq!(detail.status(), StatusCode::OK);
    assert_eq!(detail.json::<Value>().await.unwrap()["id"], MODEL);
    let missing = get_models(&relay, "/v1/models/unpriced-test", API_KEY).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let missing_body: Value = missing.json().await.expect("decode missing error");
    assert_eq!(missing_body["error"]["code"], "model_not_found");
    assert_eq!(missing_body["error"]["param"], "model");

    record_spend(&fixture.database_path, 1_000_000_000).await;
    let fallback_body: Value = get_models(&relay, "/v1/models", API_KEY)
        .await
        .json()
        .await
        .expect("decode fallback models");
    assert_eq!(fallback_body["data"].as_array().unwrap().len(), 1);
    assert_eq!(fallback_body["data"][0]["id"], FALLBACK_MODEL);
    assert_eq!(
        get_models(&relay, &format!("/v1/models/{MODEL}"), API_KEY)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    record_spend(&fixture.database_path, 1_000_000_000).await;
    let blocked_body: Value = get_models(&relay, "/v1/models", API_KEY)
        .await
        .json()
        .await
        .expect("decode blocked models");
    assert_eq!(blocked_body, json!({"object": "list", "data": []}));

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1, "catalog requests should share the cache");
    assert_eq!(requests[0].0.query(), Some("client_version=0.147.0"));
    assert_eq!(
        requests[0].1["authorization"],
        format!("Bearer {ACCESS_TOKEN}")
    );
    assert_eq!(requests[0].1["chatgpt-account-id"], ACCOUNT_ID);
    assert_eq!(requests[0].1["accept"], "application/json");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_models_cache_returns_502_instead_of_stale_data() {
    let upstream = FakeUpstream::start(vec![
        ModelsReply::Json(catalog()),
        ModelsReply::Status(StatusCode::SERVICE_UNAVAILABLE),
    ])
    .await;
    let fixture = Fixture::new(&upstream);
    let clock = FixedClock::at("2026-08-10T12:00:00Z");
    let relay = fixture.start(clock.clone()).await;

    assert_eq!(
        get_models(&relay, "/v1/models", API_KEY).await.status(),
        StatusCode::OK
    );
    clock.set("2026-08-10T12:59:59Z");
    assert_eq!(
        get_models(&relay, "/v1/models", API_KEY).await.status(),
        StatusCode::OK
    );
    assert_eq!(upstream.requests().await.len(), 1);

    clock.set("2026-08-10T13:00:00Z");
    let failed = get_models(&relay, "/v1/models", API_KEY).await;
    assert_eq!(failed.status(), StatusCode::BAD_GATEWAY);
    let body: Value = failed.json().await.expect("decode gateway error");
    assert_eq!(body["error"]["code"], "upstream_error");
    assert_eq!(upstream.requests().await.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_exhaustion_without_fallback_returns_an_empty_list_without_contacting_upstream() {
    let upstream = FakeUpstream::start(Vec::new()).await;
    let fixture = Fixture::without_fallback(&upstream);
    let relay = fixture.start(FixedClock::at("2026-08-10T12:00:00Z")).await;
    record_spend(&fixture.database_path, 1_000_000_000).await;

    let response = get_models(&relay, "/v1/models", API_KEY).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"object": "list", "data": []})
    );
    assert!(upstream.requests().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn models_retry_once_with_refreshed_credentials_after_upstream_401() {
    let upstream = FakeUpstream::start(vec![
        ModelsReply::Status(StatusCode::UNAUTHORIZED),
        ModelsReply::Json(catalog()),
    ])
    .await;
    let fixture = Fixture::new(&upstream);
    let relay = fixture.start(FixedClock::at("2026-08-10T12:00:00Z")).await;

    assert_eq!(
        get_models(&relay, "/v1/models", API_KEY).await.status(),
        StatusCode::OK
    );
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].1["authorization"],
        format!("Bearer {ACCESS_TOKEN}")
    );
    assert_eq!(
        requests[1].1["authorization"],
        format!("Bearer {ROTATED_ACCESS_TOKEN}")
    );
    assert_eq!(upstream.oauth_requests().await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_upstream_models_payload_returns_502() {
    let upstream = FakeUpstream::start(vec![ModelsReply::Raw("not-json")]).await;
    let fixture = Fixture::new(&upstream);
    let relay = fixture.start(FixedClock::at("2026-08-10T12:00:00Z")).await;

    let response = get_models(&relay, "/v1/models", API_KEY).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        response.json::<Value>().await.unwrap()["error"]["code"],
        "upstream_error"
    );
}
