use std::{
    collections::VecDeque,
    convert::Infallible,
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
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::{Value, json};
use sqlx::{Column, Connection, Row, sqlite::SqliteConnectOptions};
use tempfile::TempDir;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Barrier, Mutex, Notify},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const API_KEY: &str = "sk-ledger-secret-must-not-leak";
const API_KEY_ID: &str = "ledger-client";
const ACCESS_TOKEN: &str = "upstream-access-secret-must-not-leak";
const ACCOUNT_ID: &str = "account-secret-must-not-leak";
const PROMPT: &str = "prompt-content-must-not-be-stored";
const OUTPUT_SENTINEL: &str = "output-content-must-not-be-stored";
const RAW_ERROR_SENTINEL: &str = "raw-upstream-error-must-not-be-stored";
const MODEL: &str = "gpt-test";
const FALLBACK_MODEL: &str = "fallback-test";
const ROUND_MODEL: &str = "round-test";
const HIGH_VALUE_MODEL: &str = "high-value-test";
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
    GatedTerminal {
        reached: Arc<Notify>,
        release: Arc<Notify>,
        usage: Usage,
    },
    Http(StatusCode),
}

#[derive(Clone)]
struct UpstreamState {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    received_models: Arc<Mutex<Vec<String>>>,
}

struct FakeUpstream {
    addr: SocketAddr,
    task: JoinHandle<()>,
    received_models: Arc<Mutex<Vec<String>>>,
}

impl FakeUpstream {
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let addr = listener.local_addr().expect("fake upstream address");
        let received_models = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/responses", post(upstream_response))
            .with_state(UpstreamState {
                replies: Arc::new(Mutex::new(replies.into())),
                received_models: Arc::clone(&received_models),
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve fake upstream");
        });
        Self {
            addr,
            task,
            received_models,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn received_models(&self) -> Vec<String> {
        self.received_models.lock().await.clone()
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream_response(State(state): State<UpstreamState>, body: Bytes) -> Response<Body> {
    let request: Value = serde_json::from_slice(&body).expect("relay sent upstream JSON");
    let model = request["model"].as_str().unwrap_or(MODEL).to_owned();
    state.received_models.lock().await.push(model.clone());
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
        } => terminal_response(&model, event, status, &usage),
        Reply::BarrierTerminal { barrier, usage } => {
            timeout(TEST_TIMEOUT, barrier.wait())
                .await
                .expect("two admitted requests did not reach upstream");
            terminal_response(&model, "response.completed", "completed", &usage)
        }
        Reply::GatedTerminal {
            reached,
            release,
            usage,
        } => {
            let created = json!({
                "type": "response.created",
                "sequence_number": 0,
                "response": {"id": "resp_gated", "status": "in_progress"}
            });
            let terminal = terminal_sse(&model, "response.completed", "completed", &usage);
            let stream = async_stream::stream! {
                yield Ok::<Bytes, Infallible>(Bytes::from(format!(
                    "event: response.created\ndata: {created}\n\n"
                )));
                reached.notify_one();
                release.notified().await;
                yield Ok::<Bytes, Infallible>(Bytes::from(terminal));
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(stream))
                .expect("build gated terminal SSE")
        }
        Reply::Http(status) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "error": {
                        "message": RAW_ERROR_SENTINEL,
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
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from(terminal_sse(model, event, status, usage)))
        .expect("build terminal SSE")
}

fn terminal_sse(model: &str, event: &str, status: &str, usage: &Usage) -> String {
    let error = (status == "failed").then(|| {
        json!({
            "message": RAW_ERROR_SENTINEL,
            "type": "server_error",
            "code": "server_error"
        })
    });
    let terminal = json!({
        "type": event,
        "sequence_number": 1,
        "response": {
            "id": format!("resp_{status}"),
            "object": "response",
            "created_at": 1_700_000_000,
            "status": status,
            "model": model,
            "error": error,
            "output": [{
                "type": "message",
                "id": "msg_sentinel",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": OUTPUT_SENTINEL,
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": usage.input,
                "input_tokens_details": {"cached_tokens": usage.cached},
                "output_tokens": usage.output,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": usage.input + usage.output
            }
        }
    });
    format!("event: {event}\ndata: {terminal}\n\n")
}

struct Fixture {
    _directory: TempDir,
    config_path: PathBuf,
    auth_path: PathBuf,
    database_path: PathBuf,
    upstream_base_url: String,
    weekly_limit: Option<String>,
    hard_limit: Option<String>,
    fallback_model: Option<String>,
}

impl Fixture {
    fn new(upstream: &FakeUpstream, weekly_limit: Option<&str>) -> Self {
        Self::with_limits(upstream, weekly_limit, None, None)
    }

    fn with_limits(
        upstream: &FakeUpstream,
        weekly_limit: Option<&str>,
        hard_limit: Option<&str>,
        fallback_model: Option<&str>,
    ) -> Self {
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
                "last_refresh": "2026-08-09T00:00:00Z"
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
            hard_limit: hard_limit.map(str::to_owned),
            fallback_model: fallback_model.map(str::to_owned),
        }
    }

    fn write_config(&self, listen: SocketAddr, api_key: &str) {
        let limit = self
            .weekly_limit
            .as_ref()
            .map(|value| format!("weekly_limit_usd = \"{value}\"\n"))
            .unwrap_or_default();
        let hard = self
            .hard_limit
            .as_ref()
            .map(|value| format!("hard_limit_usd = \"{value}\"\n"))
            .unwrap_or_default();
        let fallback = self
            .fallback_model
            .as_ref()
            .map(|value| format!("fallback_model = \"{value}\"\n"))
            .unwrap_or_default();
        let config = format!(
            r#"{fallback}[server]
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
secret = "{api_key}"
{limit}{hard}
[model_prices."{MODEL}"]
input_usd_per_million = "2.00"
cached_input_usd_per_million = "0.50"
output_usd_per_million = "4.00"

[model_prices."{FALLBACK_MODEL}"]
input_usd_per_million = "0.10"
cached_input_usd_per_million = "0.01"
output_usd_per_million = "0.20"

[model_prices."{ROUND_MODEL}"]
input_usd_per_million = "0.0005"
cached_input_usd_per_million = "0"
output_usd_per_million = "0"

[model_prices."{HIGH_VALUE_MODEL}"]
input_usd_per_million = "9007199254740.993"
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
        self.start_with_key(clock, API_KEY).await
    }

    async fn start_with_key(&self, clock: FixedClock, api_key: &str) -> Relay {
        let listen = unused_address();
        self.write_config(listen, api_key);
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
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let upstream = FakeUpstream::start(vec![
        Reply::GatedTerminal {
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
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
        Reply::Terminal {
            event: "response.completed",
            status: "completed",
            usage: Usage {
                input: 1,
                cached: 0,
                output: 0,
            },
        },
        Reply::Http(StatusCode::SERVICE_UNAVAILABLE),
    ])
    .await;
    let fixture = Fixture::new(&upstream, None);
    let clock = FixedClock::at("2026-08-09T12:34:56Z");
    let relay = fixture.start(clock).await;

    let response = client()
        .post(relay.url())
        .bearer_auth(API_KEY)
        .json(&json!({
            "model": MODEL,
            "input": PROMPT,
            "reasoning": {"effort": "low"},
            "stream": true
        }))
        .send()
        .await
        .expect("send gated downstream request");
    assert_eq!(response.status(), StatusCode::OK);
    let mut events = Box::pin(response.bytes_stream().eventsource());
    let created = timeout(TEST_TIMEOUT, events.next())
        .await
        .expect("created event timed out")
        .expect("stream ended before created")
        .expect("created event framing");
    assert_eq!(created.event, "response.created");
    timeout(TEST_TIMEOUT, reached.notified())
        .await
        .expect("gated upstream stream was not observed");
    release.notify_one();
    let terminal = timeout(TEST_TIMEOUT, events.next())
        .await
        .expect("terminal event timed out")
        .expect("stream ended before terminal")
        .expect("terminal event framing");
    assert_eq!(terminal.event, "response.completed");

    let mut database = open_database(&relay.database_path).await;
    let status: String = sqlx::query_scalar("SELECT status FROM request_logs WHERE id = 1")
        .fetch_one(&mut database)
        .await
        .expect("terminal was delivered before its accounting commit");
    assert_eq!(status, "completed");

    assert_eq!(
        post_response(&relay, API_KEY, ROUND_MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, HIGH_VALUE_MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let rows = sqlx::query("SELECT * FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read committed request logs");
    let columns = rows[0]
        .columns()
        .iter()
        .map(|column| column.name())
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

    assert_eq!(rows.len(), 4);
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
    assert_eq!(rows[0].get::<String, _>("cost_usd"), "0.000026000");
    assert_eq!(rows[0].get::<i64, _>("duration_ms"), 0);
    assert_eq!(rows[0].get::<String, _>("status"), "completed");
    assert_eq!(rows[0].get::<i64, _>("http_status"), 200);
    assert_eq!(rows[1].get::<String, _>("cost_usd"), "0.000000001");
    assert_eq!(rows[2].get::<String, _>("cost_usd"), "9007199.254740993");
    assert_eq!(
        rows[3].try_get::<Option<String>, _>("cost_usd").unwrap(),
        None
    );

    let rendered = format!("{rows:?}");
    for forbidden in [
        API_KEY,
        ACCESS_TOKEN,
        "refresh-secret-must-not-leak",
        ACCOUNT_ID,
        PROMPT,
        OUTPUT_SENTINEL,
        RAW_ERROR_SENTINEL,
    ] {
        assert!(
            !rendered.contains(forbidden),
            "request_logs leaked {forbidden:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_may_cross_the_limit_then_monday_rollover_and_restart_define_admission() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let usage = Usage {
        input: 1,
        cached: 0,
        output: 0,
    };
    let upstream = FakeUpstream::start(vec![
        Reply::GatedTerminal {
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
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

    let response = client()
        .post(relay.url())
        .bearer_auth(API_KEY)
        .json(&json!({
            "model": MODEL,
            "input": PROMPT,
            "reasoning": {"effort": "low"},
            "stream": true
        }))
        .send()
        .await
        .expect("send cross-boundary request");
    assert_eq!(response.status(), StatusCode::OK);
    timeout(TEST_TIMEOUT, reached.notified())
        .await
        .expect("cross-boundary request did not reach upstream");
    clock.set("2026-08-10T00:00:00Z");
    release.notify_one();
    let body = response
        .bytes()
        .await
        .expect("consume cross-boundary response");
    assert!(String::from_utf8_lossy(&body).contains("response.completed"));

    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS,
        "the Monday request may cross the limit but the next one must be rejected"
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
    assert_eq!(statuses, ["completed", "completed", "rejected", "rejected"]);

    let requested_at = sqlx::query("SELECT requested_at FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read request attribution timestamps")
        .into_iter()
        .map(|row| row.get::<String, _>("requested_at"))
        .collect::<Vec<_>>();
    assert_eq!(requested_at[0], "2026-08-09T23:59:59Z");
    assert_eq!(requested_at[1], "2026-08-10T00:00:00Z");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotating_a_secret_preserves_weekly_spend_for_the_stable_key_id() {
    const ROTATED_API_KEY: &str = "sk-rotated-ledger-secret-must-not-leak";

    let upstream = FakeUpstream::start(vec![Reply::Terminal {
        event: "response.completed",
        status: "completed",
        usage: Usage {
            input: 1,
            cached: 0,
            output: 0,
        },
    }])
    .await;
    let fixture = Fixture::new(&upstream, Some("0.000001"));
    let clock = FixedClock::at("2026-08-09T10:00:00Z");
    let first = fixture.start(clock.clone()).await;

    assert_eq!(
        post_response(&first, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    first.stop().await;

    let restarted = fixture.start_with_key(clock, ROTATED_API_KEY).await;
    assert_eq!(
        post_response(&restarted, API_KEY, MODEL, json!(true)).await,
        StatusCode::UNAUTHORIZED,
        "the old secret remained valid after rotation"
    );
    assert_eq!(
        post_response(&restarted, ROTATED_API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS,
        "the rotated secret did not inherit spend recorded under its stable key ID"
    );

    let mut database = open_database(&restarted.database_path).await;
    let rows = sqlx::query("SELECT api_key_id, status, http_status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read stable-identity request logs");
    assert_eq!(rows.len(), 2, "the invalid old secret created a ledger row");
    assert_eq!(rows[0].get::<String, _>("api_key_id"), API_KEY_ID);
    assert_eq!(rows[0].get::<String, _>("status"), "completed");
    assert_eq!(rows[0].get::<i64, _>("http_status"), 200);
    assert_eq!(rows[1].get::<String, _>("api_key_id"), API_KEY_ID);
    assert_eq!(rows[1].get::<String, _>("status"), "rejected");
    assert_eq!(rows[1].get::<i64, _>("http_status"), 429);

    let rendered = format!("{rows:?}");
    assert!(!rendered.contains(API_KEY));
    assert!(!rendered.contains(ROTATED_API_KEY));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backward_wall_clock_does_not_break_terminal_accounting_or_make_duration_negative() {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let upstream = FakeUpstream::start(vec![Reply::GatedTerminal {
        reached: Arc::clone(&reached),
        release: Arc::clone(&release),
        usage: Usage {
            input: 1,
            cached: 0,
            output: 0,
        },
    }])
    .await;
    let fixture = Fixture::new(&upstream, None);
    let clock = FixedClock::at("2026-08-09T10:00:01Z");
    let relay = fixture.start(clock.clone()).await;

    let url = relay.url();
    let request = tokio::spawn(async move {
        client()
            .post(url)
            .bearer_auth(API_KEY)
            .json(&json!({
                "model": MODEL,
                "input": PROMPT,
                "reasoning": {"effort": "low"},
                "stream": true
            }))
            .send()
            .await
            .expect("send downstream request")
    });
    timeout(TEST_TIMEOUT, reached.notified())
        .await
        .expect("request did not reach the gated upstream");
    clock.set("2026-08-09T10:00:00Z");
    release.notify_one();

    let response = timeout(TEST_TIMEOUT, request)
        .await
        .expect("downstream response timed out")
        .expect("request task failed");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.expect("consume downstream response");
    assert!(
        String::from_utf8_lossy(&body).contains("response.completed"),
        "a backward wall-clock adjustment suppressed the terminal event"
    );

    let mut database = open_database(&relay.database_path).await;
    let row = sqlx::query("SELECT status, duration_ms FROM request_logs")
        .fetch_one(&mut database)
        .await
        .expect("read finalized request log");
    assert_eq!(row.get::<String, _>("status"), "completed");
    assert_eq!(row.get::<i64, _>("duration_ms"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_quota_exhaustion_rewrites_requests_to_the_fallback_model() {
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
    let fixture = Fixture::with_limits(
        &upstream,
        Some("0.000001"),
        Some("600.00"),
        Some(FALLBACK_MODEL),
    );
    let relay = fixture.start(FixedClock::at("2026-08-10T12:00:00Z")).await;

    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        upstream.received_models().await,
        vec![MODEL.to_owned(), FALLBACK_MODEL.to_owned()]
    );

    let mut database = open_database(&relay.database_path).await;
    let rows = sqlx::query("SELECT model, cost_usd, status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read fallback request logs");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("model"), MODEL);
    assert_eq!(rows[0].get::<String, _>("cost_usd"), "0.000002000");
    assert_eq!(rows[0].get::<String, _>("status"), "completed");
    assert_eq!(rows[1].get::<String, _>("model"), FALLBACK_MODEL);
    assert_eq!(rows[1].get::<String, _>("cost_usd"), "0.000000100");
    assert_eq!(rows[1].get::<String, _>("status"), "completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_quota_exhaustion_rejects_after_fallback_spend() {
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
    let fixture = Fixture::with_limits(
        &upstream,
        Some("0.000001"),
        Some("0.00000205"),
        Some(FALLBACK_MODEL),
    );
    let relay = fixture.start(FixedClock::at("2026-08-10T12:00:00Z")).await;

    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        upstream.received_models().await,
        vec![MODEL.to_owned(), FALLBACK_MODEL.to_owned()]
    );

    let mut database = open_database(&relay.database_path).await;
    let statuses = sqlx::query("SELECT status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read hard-quota statuses")
        .into_iter()
        .map(|row| row.get::<String, _>("status"))
        .collect::<Vec<_>>();
    assert_eq!(statuses, ["completed", "completed", "rejected"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_quota_without_fallback_model_rejects_like_before() {
    let usage = Usage {
        input: 1,
        cached: 0,
        output: 0,
    };
    let upstream = FakeUpstream::start(vec![Reply::Terminal {
        event: "response.completed",
        status: "completed",
        usage,
    }])
    .await;
    let fixture = Fixture::with_limits(&upstream, Some("0.000001"), Some("600.00"), None);
    let relay = fixture.start(FixedClock::at("2026-08-10T12:00:00Z")).await;

    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::OK
    );
    assert_eq!(
        post_response(&relay, API_KEY, MODEL, json!(true)).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(upstream.received_models().await, vec![MODEL.to_owned()]);

    let mut database = open_database(&relay.database_path).await;
    let statuses = sqlx::query("SELECT status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read soft-without-fallback statuses")
        .into_iter()
        .map(|row| row.get::<String, _>("status"))
        .collect::<Vec<_>>();
    assert_eq!(statuses, ["completed", "rejected"]);
}
