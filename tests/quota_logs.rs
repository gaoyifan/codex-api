use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
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
use codex_api::{Clock, run_with_clock};
use serde_json::{Value, json};
use sqlx::{Connection, Row, sqlite::SqliteConnectOptions};
use tempfile::TempDir;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Barrier, Mutex},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const API_KEY: &str = "sk-ledger-secret-must-not-leak";
const API_KEY_ID: &str = "ledger-client";
const ACCESS_TOKEN: &str = "upstream-access-secret-must-not-leak";
const ACCOUNT_ID: &str = "account-secret-must-not-leak";
const PROMPT: &str = "prompt-content-must-not-be-stored";
const MODEL: &str = "gpt-test";
const ROUND_MODEL: &str = "round-test";
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

#[derive(Clone)]
struct Usage {
    input: u64,
    cached: u64,
    output: u64,
}

enum Reply {
    Terminal {
        event: &'static str,
        status: &'static str,
        usage: Usage,
    },
    BarrierTerminal {
        barrier: Arc<Barrier>,
        usage: Usage,
    },
    Http(StatusCode),
}

#[derive(Clone)]
struct UpstreamState {
    replies: Arc<Mutex<VecDeque<Reply>>>,
}

struct FakeUpstream {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let addr = listener.local_addr().expect("fake upstream address");
        let app = Router::new()
            .route("/responses", post(upstream_response))
            .with_state(UpstreamState {
                replies: Arc::new(Mutex::new(replies.into())),
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve fake upstream");
        });
        Self { addr, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream_response(State(state): State<UpstreamState>, body: Bytes) -> Response<Body> {
    let request: Value = serde_json::from_slice(&body).expect("relay sent upstream JSON");
    let model = request["model"].as_str().unwrap_or(MODEL);
    let reply = state
        .replies
        .lock()
        .await
        .pop_front()
        .expect("fake upstream reply exhausted");
    match reply {
        Reply::Terminal {
            event,
            status,
            usage,
        } => terminal_response(model, event, status, &usage),
        Reply::BarrierTerminal { barrier, usage } => {
            timeout(TEST_TIMEOUT, barrier.wait())
                .await
                .expect("two admitted requests did not reach upstream");
            terminal_response(model, "response.completed", "completed", &usage)
        }
        Reply::Http(status) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "error": {
                        "message": "scripted upstream failure must not be logged",
                        "type": "server_error",
                        "code": "server_error"
                    }
                })
                .to_string(),
            ))
            .expect("build HTTP failure"),
    }
}

fn terminal_response(model: &str, event: &str, status: &str, usage: &Usage) -> Response<Body> {
    let terminal = json!({
        "type": event,
        "sequence_number": 1,
        "response": {
            "id": format!("resp_{status}"),
            "object": "response",
            "created_at": 1_700_000_000,
            "status": status,
            "model": model,
            "output": [],
            "usage": {
                "input_tokens": usage.input,
                "input_tokens_details": {"cached_tokens": usage.cached},
                "output_tokens": usage.output,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": usage.input + usage.output
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from(format!("event: {event}\ndata: {terminal}\n\n")))
        .expect("build terminal SSE")
}

struct Fixture {
    _directory: TempDir,
    config_path: PathBuf,
    auth_path: PathBuf,
    database_path: PathBuf,
    upstream_base_url: String,
    weekly_limit: Option<String>,
}

impl Fixture {
    fn new(upstream: &FakeUpstream, weekly_limit: Option<&str>) -> Self {
        let directory = tempfile::tempdir().expect("create ledger fixture");
        let config_path = directory.path().join("config.toml");
        let auth_path = directory.path().join("auth.json");
        let database_path = directory.path().join("state.sqlite3");
        std::fs::write(
            &auth_path,
            serde_json::to_vec_pretty(&json!({
                "auth_mode": "chatgpt",
                "OPENAI_API_KEY": null,
                "tokens": {
                    "id_token": "unused-id-token",
                    "access_token": ACCESS_TOKEN,
                    "refresh_token": "refresh-secret-must-not-leak",
                    "account_id": ACCOUNT_ID
                },
                "last_refresh": "2026-08-01T00:00:00Z"
            }))
            .expect("serialize auth seed"),
        )
        .expect("write auth seed");
        Self {
            _directory: directory,
            config_path,
            auth_path,
            database_path,
            upstream_base_url: upstream.base_url(),
            weekly_limit: weekly_limit.map(str::to_owned),
        }
    }

    fn write_config(&self, listen: SocketAddr) {
        let limit = self
            .weekly_limit
            .as_ref()
            .map(|value| format!("weekly_limit_usd = \"{value}\"\n"))
            .unwrap_or_default();
        let config = format!(
            r#"[server]
listen = "{listen}"
enable_websockets = false

[state]
path = "{database_path}"

[upstream]
base_url = "{upstream_base_url}"
oauth_token_url = "{upstream_base_url}/oauth/token"
auth_file = "{auth_path}"
supports_websockets = false

[[api_keys]]
id = "{API_KEY_ID}"
secret = "{API_KEY}"
{limit}
[model_prices."{MODEL}"]
input_usd_per_million = "2.00"
cached_input_usd_per_million = "0.50"
output_usd_per_million = "4.00"

[model_prices."{ROUND_MODEL}"]
input_usd_per_million = "0.0005"
cached_input_usd_per_million = "0"
output_usd_per_million = "0"
"#,
            database_path = self.database_path.display(),
            upstream_base_url = self.upstream_base_url,
            auth_path = self.auth_path.display(),
        );
        std::fs::write(&self.config_path, config).expect("write relay config");
    }

    async fn start(&self, clock: FixedClock) -> Relay {
        let listen = unused_address();
        self.write_config(listen);
        let config_path = self.config_path.clone();
        let task = tokio::spawn(async move { run_with_clock(&config_path, Arc::new(clock)).await });
        wait_until_listening(&task, listen).await;
        Relay {
            listen,
            task,
            database_path: self.database_path.clone(),
        }
    }
}

struct Relay {
    listen: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
    database_path: PathBuf,
}

impl Relay {
    fn url(&self) -> String {
        format!("http://{}/v1/responses", self.listen)
    }

    async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
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

async fn post_response(relay: &Relay, key: &str, model: &str, stream: Value) -> StatusCode {
    let response = client()
        .post(relay.url())
        .bearer_auth(key)
        .json(&json!({
            "model": model,
            "input": PROMPT,
            "reasoning": {"effort": "low"},
            "stream": stream
        }))
        .send()
        .await
        .expect("send downstream request");
    let status = response.status();
    let _ = response.bytes().await.expect("consume downstream response");
    status
}

async fn open_database(path: &Path) -> sqlx::SqliteConnection {
    timeout(
        TEST_TIMEOUT,
        sqlx::SqliteConnection::connect_with(
            &SqliteConnectOptions::new().filename(path).read_only(true),
        ),
    )
    .await
    .expect("open database timeout")
    .expect("open request log database")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_view_commits_exact_pricing_rounding_and_safe_columns_before_terminal_delivery() {
    let upstream = FakeUpstream::start(vec![
        Reply::Terminal {
            event: "response.completed",
            status: "completed",
            usage: Usage {
                input: 10,
                cached: 4,
                output: 3,
            },
        },
        Reply::Terminal {
            event: "response.completed",
            status: "completed",
            usage: Usage {
                input: 1,
                cached: 0,
                output: 0,
            },
        },
    ])
    .await;
    let fixture = Fixture::new(&upstream, None);
    let clock = FixedClock::at("2026-08-09T12:34:56Z");
    let relay = fixture.start(clock).await;

    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, ROUND_MODEL, json!(true)).await,
        StatusCode::OK
    );

    let mut database = open_database(&relay.database_path).await;
    let columns = sqlx::query("PRAGMA table_info(request_logs)")
        .fetch_all(&mut database)
        .await
        .expect("read view columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert_eq!(
        columns,
        [
            "id",
            "requested_at",
            "api_key_id",
            "model",
            "reasoning_effort",
            "api_protocol",
            "transport",
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "cost_usd",
            "duration_ms",
            "status",
            "http_status",
        ]
    );

    let rows = sqlx::query(
        "SELECT *, printf('%.9f', cost_usd) AS exact_cost FROM request_logs ORDER BY id",
    )
    .fetch_all(&mut database)
    .await
    .expect("read committed request logs");
    assert_eq!(rows.len(), 2, "terminal delivery raced ahead of accounting");
    assert_eq!(
        rows[0].get::<String, _>("requested_at"),
        "2026-08-09T12:34:56Z"
    );
    assert_eq!(rows[0].get::<String, _>("api_key_id"), API_KEY_ID);
    assert_eq!(rows[0].get::<String, _>("model"), MODEL);
    assert_eq!(rows[0].get::<String, _>("reasoning_effort"), "low");
    assert_eq!(rows[0].get::<String, _>("api_protocol"), "responses");
    assert_eq!(rows[0].get::<String, _>("transport"), "http_sse");
    assert_eq!(rows[0].get::<i64, _>("input_tokens"), 10);
    assert_eq!(rows[0].get::<i64, _>("cached_input_tokens"), 4);
    assert_eq!(rows[0].get::<i64, _>("output_tokens"), 3);
    assert_eq!(rows[0].get::<String, _>("exact_cost"), "0.000026000");
    assert_eq!(rows[0].get::<i64, _>("duration_ms"), 0);
    assert_eq!(rows[0].get::<String, _>("status"), "completed");
    assert_eq!(rows[0].get::<i64, _>("http_status"), 200);
    assert_eq!(rows[1].get::<String, _>("exact_cost"), "0.000000001");

    let rendered = format!("{rows:?}");
    for forbidden in [
        API_KEY,
        ACCESS_TOKEN,
        ACCOUNT_ID,
        PROMPT,
        "scripted upstream failure",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "request_logs leaked {forbidden:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_may_cross_the_limit_then_monday_rollover_and_restart_define_admission() {
    let usage = Usage {
        input: 1,
        cached: 0,
        output: 0,
    };
    let upstream = FakeUpstream::start(vec![
        Reply::Terminal {
            event: "response.completed",
            status: "completed",
            usage: usage.clone(),
        },
        Reply::Terminal {
            event: "response.completed",
            status: "completed",
            usage,
        },
    ])
    .await;
    let fixture = Fixture::new(&upstream, Some("0.000001"));
    let clock = FixedClock::at("2026-08-09T23:59:59Z");
    let relay = fixture.start(clock.clone()).await;

    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS
    );

    clock.set("2026-08-10T00:00:00Z");
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    relay.stop().await;

    let restarted = fixture.start(clock).await;
    assert_eq!(
        post_response(&restarted, API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS,
        "restart lost committed current-week spend"
    );

    let mut database = open_database(&restarted.database_path).await;
    let statuses = sqlx::query("SELECT status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read quota statuses")
        .into_iter()
        .map(|row| row.get::<String, _>("status"))
        .collect::<Vec<_>>();
    assert_eq!(statuses, ["completed", "rejected", "completed", "rejected"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_keys_are_not_logged_while_valid_rejections_and_terminal_failures_are() {
    let upstream = FakeUpstream::start(vec![
        Reply::Http(StatusCode::SERVICE_UNAVAILABLE),
        Reply::Terminal {
            event: "response.incomplete",
            status: "incomplete",
            usage: Usage {
                input: 2,
                cached: 1,
                output: 3,
            },
        },
        Reply::Terminal {
            event: "response.failed",
            status: "failed",
            usage: Usage {
                input: 1,
                cached: 0,
                output: 1,
            },
        },
    ])
    .await;
    let fixture = Fixture::new(&upstream, None);
    let relay = fixture.start(FixedClock::at("2026-08-09T10:00:00Z")).await;

    assert_eq!(
        post_response(&relay, "wrong-key", MODEL, json!(true)).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(false)).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );

    let mut database = open_database(&relay.database_path).await;
    let rows = sqlx::query(
        "SELECT status, http_status, cost_usd IS NOT NULL AS charged FROM request_logs ORDER BY id",
    )
    .fetch_all(&mut database)
    .await
    .expect("read semantic statuses");
    assert_eq!(rows.len(), 4, "invalid API key attempt must not be logged");
    let observed = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("status"),
                row.try_get::<i64, _>("http_status").ok(),
                row.get::<bool, _>("charged"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        [
            ("rejected".to_owned(), Some(400), false),
            ("upstream_error".to_owned(), Some(503), false),
            ("incomplete".to_owned(), Some(200), true),
            ("upstream_error".to_owned(), Some(200), true),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admission_observes_only_committed_spend_without_reservations() {
    let barrier = Arc::new(Barrier::new(2));
    let usage = Usage {
        input: 1,
        cached: 0,
        output: 0,
    };
    let upstream = FakeUpstream::start(vec![
        Reply::BarrierTerminal {
            barrier: Arc::clone(&barrier),
            usage: usage.clone(),
        },
        Reply::BarrierTerminal { barrier, usage },
    ])
    .await;
    let fixture = Fixture::new(&upstream, Some("0.000001"));
    let relay = Arc::new(fixture.start(FixedClock::at("2026-08-09T10:00:00Z")).await);

    let first = {
        let relay = Arc::clone(&relay);
        tokio::spawn(async move { post_response(&relay, API_KEY, MODEL, json!(true)).await })
    };
    let second = {
        let relay = Arc::clone(&relay);
        tokio::spawn(async move { post_response(&relay, API_KEY, MODEL, json!(true)).await })
    };
    assert_eq!(
        timeout(TEST_TIMEOUT, first).await.unwrap().unwrap(),
        StatusCode::OK
    );
    assert_eq!(
        timeout(TEST_TIMEOUT, second).await.unwrap().unwrap(),
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS
    );

    let mut database = open_database(&relay.database_path).await;
    let statuses = sqlx::query("SELECT status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read concurrent quota rows")
        .into_iter()
        .map(|row| row.get::<String, _>("status"))
        .collect::<Vec<_>>();
    assert_eq!(statuses, ["completed", "completed", "rejected"]);
}
