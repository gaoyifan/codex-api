#![cfg(unix)]

use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{StatusCode, header::CONTENT_TYPE};
use axum::response::Response;
use axum::{Router, routing::post};
use codex_api::{Clock, run_with_clock};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::json;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};
use tempfile::TempDir;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

const API_KEY: &str = "sk-state-contract";
const API_KEY_ID: &str = "state-contract-client";
const MODEL: &str = "gpt-state-contract";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

enum UpstreamBehavior {
    Hold,
    GatedTerminal {
        reached: Arc<Notify>,
        release: Arc<Notify>,
    },
}

#[derive(Clone)]
struct UpstreamState {
    behavior: Arc<Mutex<Option<UpstreamBehavior>>>,
}

struct FakeUpstream {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start(behavior: UpstreamBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind state-contract upstream");
        let addr = listener
            .local_addr()
            .expect("read state-contract upstream address");
        let app = Router::new()
            .route("/responses", post(upstream_response))
            .with_state(UpstreamState {
                behavior: Arc::new(Mutex::new(Some(behavior))),
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve state-contract upstream");
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

async fn upstream_response(State(state): State<UpstreamState>) -> Response<Body> {
    let behavior = state
        .behavior
        .lock()
        .await
        .take()
        .expect("state-contract upstream received an unexpected second request");
    let created = json!({
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_state_contract", "status": "in_progress"}
    });
    let created = format!("event: response.created\ndata: {created}\n\n");

    let body = match behavior {
        UpstreamBehavior::Hold => Body::from_stream(stream! {
            yield Ok::<Bytes, Infallible>(Bytes::from(created));
            std::future::pending::<()>().await;
        }),
        UpstreamBehavior::GatedTerminal { reached, release } => Body::from_stream(stream! {
            yield Ok::<Bytes, Infallible>(Bytes::from(created));
            reached.notify_one();
            release.notified().await;
            yield Ok::<Bytes, Infallible>(Bytes::from(completed_sse()));
        }),
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(body)
        .expect("build state-contract upstream response")
}

fn completed_sse() -> String {
    let completed = json!({
        "type": "response.completed",
        "sequence_number": 1,
        "response": {
            "id": "resp_state_contract",
            "object": "response",
            "created_at": 1_700_000_000,
            "status": "completed",
            "model": MODEL,
            "output": [],
            "usage": {
                "input_tokens": 1,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 1,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 2
            }
        }
    });
    format!("event: response.completed\ndata: {completed}\n\n")
}

struct Fixture {
    _directory: TempDir,
    config_path: PathBuf,
    database_path: PathBuf,
    listen: SocketAddr,
}

impl Fixture {
    fn new(upstream: &FakeUpstream) -> Self {
        let directory = tempfile::tempdir().expect("create state-contract fixture directory");
        let auth_path = directory.path().join("auth.json");
        let database_path = directory.path().join("state.sqlite3");
        let config_path = directory.path().join("config.toml");
        let reservation =
            StdTcpListener::bind("127.0.0.1:0").expect("reserve state-contract listen address");
        let listen = reservation
            .local_addr()
            .expect("read state-contract listen address");

        std::fs::write(
            &auth_path,
            serde_json::to_vec(&json!({
                "auth_mode": "chatgpt",
                "tokens": {
                    "id_token": "state-contract-id-token",
                    "access_token": "state-contract-access-token",
                    "refresh_token": "state-contract-refresh-token",
                    "account_id": "state-contract-account"
                },
                "last_refresh": "2099-01-01T00:00:00Z"
            }))
            .expect("serialize state-contract auth seed"),
        )
        .expect("write state-contract auth seed");

        let upstream_base_url = upstream.base_url();
        let config = format!(
            r#"[server]
listen = "{listen}"
enable_websockets = false

[state]
path = "{}"

[upstream]
base_url = "{upstream_base_url}"
oauth_token_url = "{upstream_base_url}/oauth/token"
auth_file = "{}"
supports_websockets = false

[[api_keys]]
id = "{API_KEY_ID}"
secret = "{API_KEY}"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
            database_path.display(),
            auth_path.display(),
        );
        std::fs::write(&config_path, config).expect("write state-contract configuration");
        drop(reservation);

        Self {
            _directory: directory,
            config_path,
            database_path,
            listen,
        }
    }

    fn responses_url(&self) -> String {
        format!("http://{}/v1/responses", self.listen)
    }
}

struct RelayProcess {
    child: Child,
}

impl RelayProcess {
    async fn start(fixture: &Fixture) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
            .arg("--config")
            .arg(&fixture.config_path)
            .arg("serve")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn state-contract relay");
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait().expect("poll state-contract relay") {
                panic!("codex-api exited before listening with {status}");
            }
            if TcpStream::connect(fixture.listen).await.is_ok() {
                return Self { child };
            }
            assert!(
                Instant::now() < deadline,
                "codex-api did not start listening"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn kill_abruptly(mut self) {
        self.child
            .kill()
            .await
            .expect("SIGKILL state-contract relay");
    }
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

async fn startup_is_rejected(fixture: &Fixture) -> bool {
    let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
        .arg("--config")
        .arg(&fixture.config_path)
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn state-path startup attempt");
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("poll state-path startup attempt") {
            return !status.success();
        }
        if TcpStream::connect(fixture.listen).await.is_ok() {
            child
                .kill()
                .await
                .expect("stop relay that accepted an invalid state path");
            return false;
        }
        assert!(
            Instant::now() < deadline,
            "state-path startup attempt neither exited nor listened"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

struct InProcessRelay {
    task: JoinHandle<anyhow::Result<()>>,
}

impl InProcessRelay {
    async fn start(fixture: &Fixture, clock: FixedClock) -> Self {
        let config_path = fixture.config_path.clone();
        let task = tokio::spawn(async move { run_with_clock(&config_path, Arc::new(clock)).await });
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            assert!(!task.is_finished(), "codex-api exited before listening");
            if TcpStream::connect(fixture.listen).await.is_ok() {
                return Self { task };
            }
            assert!(
                Instant::now() < deadline,
                "codex-api did not start listening"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for InProcessRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct FixedClock(Arc<StdMutex<OffsetDateTime>>);

impl FixedClock {
    fn new(now: OffsetDateTime) -> Self {
        Self(Arc::new(StdMutex::new(now)))
    }

    fn advance(&self, duration: TimeDuration) {
        let mut now = self.0.lock().expect("fixed clock lock");
        *now += duration;
    }
}

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        *self.0.lock().expect("fixed clock lock")
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(TEST_TIMEOUT)
        .build()
        .expect("build state-contract client")
}

async fn open_database(path: &Path, read_only: bool) -> SqliteConnection {
    timeout(
        TEST_TIMEOUT,
        SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(path)
                .read_only(read_only),
        ),
    )
    .await
    .expect("open state database in time")
    .expect("open state database")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_sqlite_main_file_is_owner_read_write_only() {
    let _test_guard = TEST_LOCK.lock().await;
    let upstream = FakeUpstream::start(UpstreamBehavior::Hold).await;
    let fixture = Fixture::new(&upstream);
    assert!(!fixture.database_path.exists());

    let _relay = RelayProcess::start(&fixture).await;

    let mode = std::fs::metadata(&fixture.database_path)
        .expect("read new SQLite main-file metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_directory_state_path_is_rejected_without_changing_its_mode() {
    let _test_guard = TEST_LOCK.lock().await;
    let upstream = FakeUpstream::start(UpstreamBehavior::Hold).await;
    let fixture = Fixture::new(&upstream);
    std::fs::create_dir(&fixture.database_path).expect("create directory at configured state path");
    std::fs::set_permissions(
        &fixture.database_path,
        std::fs::Permissions::from_mode(0o750),
    )
    .expect("set original state-directory mode");

    let rejected = startup_is_rejected(&fixture).await;
    let metadata = std::fs::symlink_metadata(&fixture.database_path)
        .expect("read state-directory metadata after startup attempt");
    let is_directory = metadata.file_type().is_dir();
    let observed_mode = metadata.permissions().mode() & 0o777;
    std::fs::set_permissions(
        &fixture.database_path,
        std::fs::Permissions::from_mode(0o750),
    )
    .expect("restore state-directory mode for fixture cleanup");

    assert!(is_directory);
    assert_eq!(observed_mode, 0o750);
    assert!(rejected, "codex-api accepted a directory as state.path");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_symlink_state_path_is_rejected_without_changing_its_target_mode() {
    let _test_guard = TEST_LOCK.lock().await;
    let upstream = FakeUpstream::start(UpstreamBehavior::Hold).await;
    let fixture = Fixture::new(&upstream);
    let target = fixture
        .database_path
        .parent()
        .expect("state path has a parent")
        .join("symlink-target.sqlite3");
    std::fs::write(&target, b"not opened as SQLite").expect("create state symlink target");
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640))
        .expect("set original symlink-target mode");
    std::os::unix::fs::symlink(&target, &fixture.database_path)
        .expect("create symlink at configured state path");

    let rejected = startup_is_rejected(&fixture).await;
    let link_metadata = std::fs::symlink_metadata(&fixture.database_path)
        .expect("read state symlink metadata after startup attempt");
    let target_mode = std::fs::metadata(&target)
        .expect("read state symlink target metadata after startup attempt")
        .permissions()
        .mode()
        & 0o777;
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640))
        .expect("restore state symlink target mode for fixture cleanup");

    assert!(link_metadata.file_type().is_symlink());
    assert_eq!(target_mode, 0o640);
    assert!(rejected, "codex-api accepted a symlink as state.path");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_logs_rejects_insert_update_and_delete() {
    let _test_guard = TEST_LOCK.lock().await;
    let upstream = FakeUpstream::start(UpstreamBehavior::Hold).await;
    let fixture = Fixture::new(&upstream);
    let _relay = RelayProcess::start(&fixture).await;
    let mut database = open_database(&fixture.database_path, false).await;

    let insert = sqlx::query("INSERT INTO request_logs (id) VALUES (999)")
        .execute(&mut database)
        .await;
    assert!(insert.is_err(), "request_logs accepted INSERT");

    let update = sqlx::query("UPDATE request_logs SET status = 'completed' WHERE id = 999")
        .execute(&mut database)
        .await;
    assert!(update.is_err(), "request_logs accepted UPDATE");

    let delete = sqlx::query("DELETE FROM request_logs WHERE id = 999")
        .execute(&mut database)
        .await;
    assert!(delete.is_err(), "request_logs accepted DELETE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abrupt_process_death_leaves_active_authenticated_request_started_after_restart() {
    let _test_guard = TEST_LOCK.lock().await;
    let upstream = FakeUpstream::start(UpstreamBehavior::Hold).await;
    let fixture = Fixture::new(&upstream);
    let first = RelayProcess::start(&fixture).await;
    let response = client()
        .post(fixture.responses_url())
        .bearer_auth(API_KEY)
        .json(&json!({"model": MODEL, "input": "hold", "stream": true}))
        .send()
        .await
        .expect("start authenticated active request");
    assert_eq!(response.status(), StatusCode::OK);
    let mut events = response.bytes_stream().eventsource();
    let created = timeout(TEST_TIMEOUT, events.next())
        .await
        .expect("receive active response event in time")
        .expect("active response stream ended")
        .expect("decode active response event");
    assert_eq!(created.event, "response.created");

    first.kill_abruptly().await;
    drop(events);
    let _restarted = RelayProcess::start(&fixture).await;
    let mut database = open_database(&fixture.database_path, true).await;
    let rows = sqlx::query("SELECT status, duration_ms, http_status FROM request_logs ORDER BY id")
        .fetch_all(&mut database)
        .await
        .expect("read request log after restart");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("status"), "started");
    assert_eq!(rows[0].get::<Option<i64>, _>("duration_ms"), None);
    assert_eq!(rows[0].get::<Option<i64>, _>("http_status"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controlled_clock_records_positive_request_duration_in_milliseconds() {
    let _test_guard = TEST_LOCK.lock().await;
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let upstream = FakeUpstream::start(UpstreamBehavior::GatedTerminal {
        reached: Arc::clone(&reached),
        release: Arc::clone(&release),
    })
    .await;
    let fixture = Fixture::new(&upstream);
    let clock = FixedClock::new(
        OffsetDateTime::from_unix_timestamp(1_786_363_200).expect("valid fixed timestamp"),
    );
    let _relay = InProcessRelay::start(&fixture, clock.clone()).await;
    let url = fixture.responses_url();
    let request = tokio::spawn(async move {
        let response = client()
            .post(url)
            .bearer_auth(API_KEY)
            .json(&json!({"model": MODEL, "input": "duration", "stream": true}))
            .send()
            .await
            .expect("send duration request");
        assert_eq!(response.status(), StatusCode::OK);
        response
            .bytes()
            .await
            .expect("consume completed duration response");
    });
    timeout(TEST_TIMEOUT, reached.notified())
        .await
        .expect("duration request did not reach gated upstream");
    clock.advance(TimeDuration::milliseconds(123));
    release.notify_one();
    timeout(TEST_TIMEOUT, request)
        .await
        .expect("duration request did not complete")
        .expect("duration request task failed");

    let mut database = open_database(&fixture.database_path, true).await;
    let row = sqlx::query("SELECT status, duration_ms FROM request_logs")
        .fetch_one(&mut database)
        .await
        .expect("read duration request log");
    assert_eq!(row.get::<String, _>("status"), "completed");
    assert_eq!(row.get::<i64, _>("duration_ms"), 123);
}
