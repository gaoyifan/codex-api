use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::routing::post;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};
use tempfile::TempDir;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::sync::{Barrier, Semaphore};

const CLIENT_KEY: &str = "sk-test-client";
const MODEL: &str = "gpt-5.6-luna";
const CODEX_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";

#[derive(Clone, Debug)]
struct ObservedUpstreamRequest {
    authorization: Option<String>,
    account_id: Option<String>,
}

#[derive(Clone, Debug)]
struct ObservedOauthRequest {
    body: Value,
}

#[derive(Clone)]
struct OauthReply {
    status: StatusCode,
    body: Value,
    delay: StdDuration,
    gate: Option<Arc<Semaphore>>,
}

impl Default for OauthReply {
    fn default() -> Self {
        Self {
            status: StatusCode::OK,
            body: json!({}),
            delay: StdDuration::ZERO,
            gate: None,
        }
    }
}

#[derive(Default)]
struct FakeState {
    upstream_requests: Mutex<Vec<ObservedUpstreamRequest>>,
    oauth_requests: Mutex<Vec<ObservedOauthRequest>>,
    unauthorized_remaining: AtomicUsize,
    rejected_authorization: Mutex<Option<(String, StdDuration)>>,
    oauth_reply: Mutex<OauthReply>,
}

struct FakeCodexServer {
    addr: SocketAddr,
    state: Arc<FakeState>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeCodexServer {
    async fn start() -> Self {
        let state = Arc::new(FakeState::default());
        let app = Router::new()
            .route("/backend-api/codex/responses", post(fake_responses))
            .route("/oauth/token", post(fake_oauth))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind fake Codex server");
        let addr = listener.local_addr().expect("fake server local address");
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("fake Codex server failed");
        });

        Self { addr, state, task }
    }

    fn upstream_base_url(&self) -> String {
        format!("http://{}/backend-api/codex", self.addr)
    }

    fn oauth_url(&self) -> String {
        format!("http://{}/oauth/token", self.addr)
    }

    fn reply_with(&self, body: Value) {
        *self.state.oauth_reply.lock().expect("OAuth reply lock") = OauthReply {
            status: StatusCode::OK,
            body,
            delay: StdDuration::ZERO,
            gate: None,
        };
    }

    fn reply_with_delay(&self, body: Value, delay: StdDuration) {
        *self.state.oauth_reply.lock().expect("OAuth reply lock") = OauthReply {
            status: StatusCode::OK,
            body,
            delay,
            gate: None,
        };
    }

    fn reply_after_release(&self, body: Value) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.state.oauth_reply.lock().expect("OAuth reply lock") = OauthReply {
            status: StatusCode::OK,
            body,
            delay: StdDuration::ZERO,
            gate: Some(Arc::clone(&gate)),
        };
        gate
    }

    fn fail_oauth_with(&self, status: StatusCode) {
        *self.state.oauth_reply.lock().expect("OAuth reply lock") = OauthReply {
            status,
            body: json!({
                "error": "temporarily_unavailable",
                "error_description": "scripted OAuth failure"
            }),
            delay: StdDuration::ZERO,
            gate: None,
        };
    }

    fn fail_oauth_with_delay(&self, status: StatusCode, delay: StdDuration) {
        *self.state.oauth_reply.lock().expect("OAuth reply lock") = OauthReply {
            status,
            body: json!({
                "error": "temporarily_unavailable",
                "error_description": "scripted delayed OAuth failure"
            }),
            delay,
            gate: None,
        };
    }

    fn reject_next_upstream_requests(&self, count: usize) {
        self.state
            .unauthorized_remaining
            .store(count, Ordering::SeqCst);
    }

    fn reject_authorization(&self, authorization: String, delay: StdDuration) {
        *self
            .state
            .rejected_authorization
            .lock()
            .expect("rejected authorization lock") = Some((authorization, delay));
    }

    fn upstream_requests(&self) -> Vec<ObservedUpstreamRequest> {
        self.state
            .upstream_requests
            .lock()
            .expect("upstream requests lock")
            .clone()
    }

    fn oauth_requests(&self) -> Vec<ObservedOauthRequest> {
        self.state
            .oauth_requests
            .lock()
            .expect("OAuth requests lock")
            .clone()
    }

    fn clear_observations(&self) {
        self.state
            .upstream_requests
            .lock()
            .expect("upstream requests lock")
            .clear();
        self.state
            .oauth_requests
            .lock()
            .expect("OAuth requests lock")
            .clear();
        self.state.unauthorized_remaining.store(0, Ordering::SeqCst);
        *self
            .state
            .rejected_authorization
            .lock()
            .expect("rejected authorization lock") = None;
    }
}

impl Drop for FakeCodexServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake_responses(
    State(state): State<Arc<FakeState>>,
    headers: HeaderMap,
    _body: Bytes,
) -> Response<Body> {
    let authorization = header_string(&headers, AUTHORIZATION.as_str());
    let account_id = header_string(&headers, "chatgpt-account-id");
    state
        .upstream_requests
        .lock()
        .expect("upstream requests lock")
        .push(ObservedUpstreamRequest {
            authorization: authorization.clone(),
            account_id,
        });

    let rejected = state
        .rejected_authorization
        .lock()
        .expect("rejected authorization lock")
        .clone()
        .filter(|(expected, _)| authorization.as_deref() == Some(expected.as_str()));
    if let Some((_, delay)) = rejected {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        return json_response(
            StatusCode::UNAUTHORIZED,
            json!({
                "error": {
                    "message": "scripted upstream authentication rejection",
                    "type": "invalid_request_error",
                    "code": "invalid_api_key"
                }
            }),
        );
    }

    if state
        .unauthorized_remaining
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return json_response(
            StatusCode::UNAUTHORIZED,
            json!({
                "error": {
                    "message": "scripted upstream authentication rejection",
                    "type": "invalid_request_error",
                    "code": "invalid_api_key"
                }
            }),
        );
    }

    let terminal = json!({
        "type": "response.completed",
        "sequence_number": 1,
        "response": {
            "id": "resp_oauth_test",
            "object": "response",
            "created_at": 1_700_000_000,
            "status": "completed",
            "model": MODEL,
            "output": [],
            "usage": {
                "input_tokens": 1,
                "input_tokens_details": { "cached_tokens": 0 },
                "output_tokens": 1,
                "output_tokens_details": { "reasoning_tokens": 0 },
                "total_tokens": 2
            }
        }
    });
    let body = format!("event: response.completed\ndata: {terminal}\n\n");
    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
}

async fn fake_oauth(State(state): State<Arc<FakeState>>, body: Bytes) -> Response<Body> {
    let parsed = serde_json::from_slice(&body).unwrap_or_else(|_| {
        json!({
            "unparseable_body": String::from_utf8_lossy(&body)
        })
    });
    state
        .oauth_requests
        .lock()
        .expect("OAuth requests lock")
        .push(ObservedOauthRequest { body: parsed });
    let reply = state.oauth_reply.lock().expect("OAuth reply lock").clone();
    if let Some(gate) = reply.gate {
        gate.acquire_owned()
            .await
            .expect("OAuth response gate was closed")
            .forget();
    }
    if !reply.delay.is_zero() {
        tokio::time::sleep(reply.delay).await;
    }
    json_response(reply.status, reply.body)
}

fn json_response(status: StatusCode, value: Value) -> Response<Body> {
    let mut response = Response::new(Body::from(value.to_string()));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

#[derive(Clone)]
struct AuthSeed {
    access_token: String,
    refresh_token: String,
    account_id: String,
    last_refresh: OffsetDateTime,
}

struct RelayFixture {
    _temp_dir: TempDir,
    config_path: PathBuf,
    auth_path: PathBuf,
    database_path: PathBuf,
    upstream_base_url: String,
    oauth_url: String,
}

impl RelayFixture {
    fn new(fake: &FakeCodexServer, seed: &AuthSeed) -> Self {
        let temp_dir = tempfile::tempdir().expect("temporary relay directory");
        let config_path = temp_dir.path().join("config.toml");
        let auth_path = temp_dir.path().join("auth.json");
        let database_path = temp_dir.path().join("state.sqlite3");
        write_auth_seed(&auth_path, seed);
        Self {
            _temp_dir: temp_dir,
            config_path,
            auth_path,
            database_path,
            upstream_base_url: fake.upstream_base_url(),
            oauth_url: fake.oauth_url(),
        }
    }

    fn write_seed(&self, seed: &AuthSeed) {
        write_auth_seed(&self.auth_path, seed);
    }

    async fn start(&self) -> RelayProcess {
        let listen = unused_local_addr();
        write_config(
            &self.config_path,
            listen,
            &self.database_path,
            &self.auth_path,
            &self.upstream_base_url,
            &self.oauth_url,
        );
        RelayProcess::start(&self.config_path, listen).await
    }
}

struct RelayProcess {
    child: Child,
    base_url: String,
}

impl RelayProcess {
    async fn start(config_path: &Path, listen: SocketAddr) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
            .arg("--config")
            .arg(config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start codex-api process");

        for _ in 0..150 {
            if let Some(status) = child.try_wait().expect("poll codex-api process") {
                let output = child
                    .wait_with_output()
                    .expect("collect failed codex-api output");
                panic!(
                    "codex-api exited before listening ({status}):\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            if tokio::net::TcpStream::connect(listen).await.is_ok() {
                return Self {
                    child,
                    base_url: format!("http://{listen}"),
                };
            }
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }

        child.kill().expect("kill unresponsive codex-api process");
        let output = child
            .wait_with_output()
            .expect("collect unresponsive codex-api output");
        panic!(
            "codex-api did not listen at {listen}:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn responses_url(&self) -> String {
        format!("{}/v1/responses", self.base_url)
    }

    fn stop(self) {
        drop(self);
    }
}

async fn expect_seed_startup_failure(fixture: &RelayFixture) -> std::process::Output {
    let listen = unused_local_addr();
    write_config(
        &fixture.config_path,
        listen,
        &fixture.database_path,
        &fixture.auth_path,
        &fixture.upstream_base_url,
        &fixture.oauth_url,
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_codex-api"))
        .arg("--config")
        .arg(&fixture.config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start codex-api process for rejected seed");

    for _ in 0..100 {
        if child
            .try_wait()
            .expect("poll rejected-seed process")
            .is_some()
        {
            return child
                .wait_with_output()
                .expect("collect rejected-seed process output");
        }
        if tokio::net::TcpStream::connect(listen).await.is_ok() {
            child
                .kill()
                .expect("kill process that accepted an invalid auth seed");
            let _ = child.wait();
            panic!("codex-api listened at {listen} with an invalid auth seed");
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }

    child
        .kill()
        .expect("kill process that did not reject invalid auth seed");
    let _ = child.wait();
    panic!("codex-api did not reject an invalid auth seed before listening");
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct PublicResponse {
    status: reqwest::StatusCode,
    body: String,
}

async fn post_streaming_response(url: &str) -> PublicResponse {
    let response = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(5))
        .build()
        .expect("build downstream client")
        .post(url)
        .bearer_auth(CLIENT_KEY)
        .json(&json!({
            "model": MODEL,
            "input": "Reply with OK.",
            "stream": true
        }))
        .send()
        .await
        .expect("send downstream Responses request");
    let status = response.status();
    let body = response.text().await.expect("read downstream response");
    PublicResponse { status, body }
}

fn assert_success(response: &PublicResponse) {
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(
        response.body.contains("response.completed"),
        "missing terminal Responses event: {}",
        response.body
    );
}

fn assert_oauth_grant(request: &ObservedOauthRequest, refresh_token: &str) {
    assert_eq!(
        request.body,
        json!({
            "client_id": CODEX_OAUTH_CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        })
    );
}

fn full_oauth_reply(access_token: &str, refresh_token: &str, account_id: &str) -> Value {
    json!({
        "id_token": id_token(account_id),
        "access_token": access_token,
        "refresh_token": refresh_token
    })
}

fn write_auth_seed(path: &Path, seed: &AuthSeed) {
    let last_refresh = seed
        .last_refresh
        .format(&Rfc3339)
        .expect("format auth last_refresh");
    let value = json!({
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": null,
        "tokens": {
            "id_token": id_token(&seed.account_id),
            "access_token": seed.access_token,
            "refresh_token": seed.refresh_token,
            "account_id": seed.account_id
        },
        "last_refresh": last_refresh
    });
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&value).expect("serialize auth seed"),
    )
    .expect("write auth seed");
}

fn write_config(
    path: &Path,
    listen: SocketAddr,
    database_path: &Path,
    auth_path: &Path,
    upstream_base_url: &str,
    oauth_url: &str,
) {
    fn quoted(value: &str) -> String {
        serde_json::to_string(value).expect("quote TOML string")
    }

    let config = format!(
        r#"[server]
listen = {}
enable_websockets = false

[state]
path = {}

[upstream]
base_url = {}
oauth_token_url = {}
auth_file = {}
supports_websockets = false

[[api_keys]]
id = "test-client"
secret = "{CLIENT_KEY}"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
        quoted(&listen.to_string()),
        quoted(&database_path.display().to_string()),
        quoted(upstream_base_url),
        quoted(oauth_url),
        quoted(&auth_path.display().to_string()),
    );
    std::fs::write(path, config).expect("write relay config");
}

fn unused_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .expect("reserve relay listen address");
    let port = listener
        .local_addr()
        .expect("reserved local address")
        .port();
    drop(listener);
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn access_token(label: &str, expires_at: OffsetDateTime) -> String {
    unsigned_jwt(json!({
        "exp": expires_at.unix_timestamp(),
        "test_label": label
    }))
}

fn id_token(account_id: &str) -> String {
    unsigned_jwt(json!({
        "email": "relay-test@example.invalid",
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id,
            "chatgpt_plan_type": "plus",
            "chatgpt_user_id": "user-test"
        }
    }))
}

fn unsigned_jwt(payload: Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("serialize JWT"));
    format!("{header}.{payload}.test-signature")
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

fn seed(
    access_token: impl Into<String>,
    refresh_token: impl Into<String>,
    account_id: impl Into<String>,
    last_refresh: OffsetDateTime,
) -> AuthSeed {
    AuthSeed {
        access_token: access_token.into(),
        refresh_token: refresh_token.into(),
        account_id: account_id.into(),
        last_refresh,
    }
}

#[tokio::test]
async fn auth_seed_rejects_header_control_characters_before_listening_without_leaking_them() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let cases = [
        seed(
            "access-header-injection\r\nx-leaked-access",
            "refresh-a",
            "account-a",
            now,
        ),
        seed(
            access_token("valid-header", now + Duration::hours(1)),
            "refresh-b",
            "account-header-injection\r\nx-leaked-account",
            now,
        ),
    ];

    for invalid_seed in cases {
        let leaked_access = invalid_seed.access_token.clone();
        let leaked_account = invalid_seed.account_id.clone();
        let fixture = RelayFixture::new(&fake, &invalid_seed);
        let output = expect_seed_startup_failure(&fixture).await;
        assert!(!output.status.success());
        let diagnostics = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(diagnostics.to_ascii_lowercase().contains("auth"));
        assert!(!diagnostics.contains(&leaked_access));
        assert!(!diagnostics.contains(&leaked_account));
        assert!(!diagnostics.contains("x-leaked-access"));
        assert!(!diagnostics.contains("x-leaked-account"));
    }
}

#[tokio::test]
async fn auth_seed_requires_explicit_chatgpt_mode_without_leaking_seed_contents() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();

    for replacement_mode in [None, Some("definitely-not-chatgpt-mode")] {
        let fixture = RelayFixture::new(
            &fake,
            &seed(
                access_token("mode-secret-access", now + Duration::hours(1)),
                "mode-secret-refresh",
                "mode-secret-account",
                now,
            ),
        );
        let mut auth: Value = serde_json::from_slice(
            &std::fs::read(&fixture.auth_path).expect("read auth seed for mode mutation"),
        )
        .expect("parse auth seed for mode mutation");
        let object = auth.as_object_mut().expect("auth seed object");
        match replacement_mode {
            Some(mode) => {
                object.insert("auth_mode".to_owned(), Value::String(mode.to_owned()));
            }
            None => {
                object.remove("auth_mode");
            }
        }
        std::fs::write(
            &fixture.auth_path,
            serde_json::to_vec_pretty(&auth).expect("serialize mutated auth mode"),
        )
        .expect("write mutated auth mode");

        let output = expect_seed_startup_failure(&fixture).await;
        assert!(!output.status.success());
        let diagnostics = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(diagnostics.to_ascii_lowercase().contains("auth"));
        assert!(!diagnostics.contains("definitely-not-chatgpt-mode"));
        assert!(!diagnostics.contains("mode-secret-access"));
        assert!(!diagnostics.contains("mode-secret-refresh"));
        assert!(!diagnostics.contains("mode-secret-account"));
    }
}

#[tokio::test]
async fn access_token_is_refreshed_when_jwt_expires_within_five_minutes() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let expiring_access = access_token("expiring", now + Duration::minutes(4));
    let refreshed_access = access_token("refreshed", now + Duration::hours(1));
    fake.reply_with(full_oauth_reply(
        &refreshed_access,
        "rotated-refresh",
        "account-a",
    ));
    let fixture = RelayFixture::new(
        &fake,
        &seed(expiring_access, "seed-refresh", "account-a", now),
    );

    let relay = fixture.start().await;
    let response = post_streaming_response(&relay.responses_url()).await;
    assert_success(&response);

    let oauth = fake.oauth_requests();
    assert_eq!(oauth.len(), 1);
    assert_oauth_grant(&oauth[0], "seed-refresh");
    assert_eq!(
        fake.upstream_requests()
            .iter()
            .map(|request| request.authorization.clone())
            .collect::<Vec<_>>(),
        vec![Some(bearer(&refreshed_access))]
    );
}

#[tokio::test]
async fn access_token_outside_the_five_minute_window_is_not_refreshed() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let valid_access = access_token("valid", now + Duration::minutes(6));
    fake.reply_with(full_oauth_reply(
        &access_token("unused", now + Duration::hours(1)),
        "unused-refresh",
        "account-a",
    ));
    let fixture = RelayFixture::new(
        &fake,
        &seed(&valid_access, "seed-refresh", "account-a", now),
    );

    let relay = fixture.start().await;
    let response = post_streaming_response(&relay.responses_url()).await;
    assert_success(&response);

    assert!(fake.oauth_requests().is_empty());
    assert_eq!(
        fake.upstream_requests()[0].authorization.as_deref(),
        Some(bearer(&valid_access).as_str())
    );
}

#[tokio::test]
async fn undecodable_access_token_refreshes_only_after_eight_days() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let refreshed_access = access_token("refreshed-opaque", now + Duration::hours(1));
    fake.reply_with(full_oauth_reply(
        &refreshed_access,
        "old-seed-rotated",
        "account-old",
    ));
    let old_fixture = RelayFixture::new(
        &fake,
        &seed(
            "opaque-old-access",
            "old-seed-refresh",
            "account-old",
            now - Duration::days(8) - Duration::minutes(1),
        ),
    );

    let old_relay = old_fixture.start().await;
    assert_success(&post_streaming_response(&old_relay.responses_url()).await);
    assert_eq!(fake.oauth_requests().len(), 1);
    assert_oauth_grant(&fake.oauth_requests()[0], "old-seed-refresh");
    assert_eq!(
        fake.upstream_requests()[0].authorization.as_deref(),
        Some(bearer(&refreshed_access).as_str())
    );
    old_relay.stop();

    fake.clear_observations();
    let recent_fixture = RelayFixture::new(
        &fake,
        &seed(
            "opaque-recent-access",
            "recent-seed-refresh",
            "account-recent",
            now - Duration::days(7),
        ),
    );
    let recent_relay = recent_fixture.start().await;
    assert_success(&post_streaming_response(&recent_relay.responses_url()).await);

    assert!(fake.oauth_requests().is_empty());
    assert_eq!(
        fake.upstream_requests()[0].authorization.as_deref(),
        Some("Bearer opaque-recent-access")
    );
}

#[tokio::test]
async fn one_pre_stream_401_refreshes_credentials_and_retries_once() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let original_access = access_token("original", now + Duration::hours(1));
    let refreshed_access = access_token("after-401", now + Duration::hours(2));
    fake.reject_next_upstream_requests(1);
    fake.reply_with(full_oauth_reply(
        &refreshed_access,
        "refresh-after-401",
        "account-a",
    ));
    let fixture = RelayFixture::new(
        &fake,
        &seed(&original_access, "seed-refresh", "account-a", now),
    );

    let relay = fixture.start().await;
    let response = post_streaming_response(&relay.responses_url()).await;
    assert_success(&response);

    assert_eq!(fake.oauth_requests().len(), 1);
    assert_oauth_grant(&fake.oauth_requests()[0], "seed-refresh");
    assert_eq!(
        fake.upstream_requests()
            .iter()
            .map(|request| request.authorization.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(bearer(&original_access)),
            Some(bearer(&refreshed_access))
        ]
    );
}

#[tokio::test]
async fn repeated_upstream_401_stops_after_one_refresh_without_looping() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let original_access = access_token("original", now + Duration::hours(1));
    let refreshed_access = access_token("still-rejected", now + Duration::hours(2));
    fake.reject_next_upstream_requests(10);
    fake.reply_with(full_oauth_reply(
        &refreshed_access,
        "rotated-refresh",
        "account-a",
    ));
    let fixture = RelayFixture::new(
        &fake,
        &seed(original_access, "seed-refresh", "account-a", now),
    );

    let relay = fixture.start().await;
    let response = post_streaming_response(&relay.responses_url()).await;

    assert_eq!(
        response.status,
        StatusCode::BAD_GATEWAY,
        "{}",
        response.body
    );
    assert_eq!(fake.oauth_requests().len(), 1, "refresh must not loop");
    assert_eq!(
        fake.upstream_requests().len(),
        2,
        "upstream request must be retried only once"
    );
}

#[tokio::test]
async fn oauth_failure_is_not_retried_or_reported_as_downstream_auth_failure() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    fake.reject_next_upstream_requests(1);
    fake.fail_oauth_with(StatusCode::INTERNAL_SERVER_ERROR);
    let fixture = RelayFixture::new(
        &fake,
        &seed(
            access_token("original", now + Duration::hours(1)),
            "seed-refresh",
            "account-a",
            now,
        ),
    );

    let relay = fixture.start().await;
    let response = post_streaming_response(&relay.responses_url()).await;

    assert_eq!(
        response.status,
        StatusCode::BAD_GATEWAY,
        "{}",
        response.body
    );
    assert_ne!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(fake.upstream_requests().len(), 1);
    assert_eq!(fake.oauth_requests().len(), 1);
    assert_oauth_grant(&fake.oauth_requests()[0], "seed-refresh");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_generation_callers_share_one_oauth_failure() {
    const REQUEST_COUNT: usize = 12;

    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    fake.fail_oauth_with_delay(
        StatusCode::INTERNAL_SERVER_ERROR,
        StdDuration::from_millis(100),
    );
    let fixture = RelayFixture::new(
        &fake,
        &seed(
            access_token("shared-failure", now + Duration::minutes(4)),
            "single-use-refresh",
            "account-a",
            now,
        ),
    );
    let relay = fixture.start().await;
    let responses_url = relay.responses_url();
    let barrier = Arc::new(Barrier::new(REQUEST_COUNT));
    let requests = (0..REQUEST_COUNT)
        .map(|_| {
            let barrier = barrier.clone();
            let responses_url = responses_url.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                post_streaming_response(&responses_url).await
            })
        })
        .collect::<Vec<_>>();

    tokio::time::timeout(StdDuration::from_secs(10), async {
        for request in requests {
            let response = request.await.expect("concurrent failed refresh task");
            assert_eq!(
                response.status,
                StatusCode::BAD_GATEWAY,
                "{}",
                response.body
            );
        }
    })
    .await
    .expect("concurrent failed refresh requests timed out");

    let oauth = fake.oauth_requests();
    assert_eq!(
        oauth.len(),
        1,
        "same-generation callers must share the failed exchange instead of reusing the refresh token"
    );
    assert_oauth_grant(&oauth[0], "single-use-refresh");
    assert!(fake.upstream_requests().is_empty());
}

#[tokio::test]
async fn refreshed_id_token_without_account_claim_preserves_account_and_rotated_tokens() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let rotated_access = access_token("claimless-rotation", now + Duration::hours(1));
    let final_access = access_token("after-claimless-restart", now + Duration::hours(2));
    fake.reply_with(json!({
        "id_token": unsigned_jwt(json!({
            "sub": "user-without-workspace-claim",
            "email": "relay-test@example.invalid"
        })),
        "access_token": rotated_access,
        "refresh_token": "rotated-after-claimless-id-token"
    }));
    let fixture = RelayFixture::new(
        &fake,
        &seed(
            access_token("claimless-seed", now + Duration::minutes(4)),
            "seed-refresh",
            "account-a",
            now,
        ),
    );

    let first_process = fixture.start().await;
    assert_success(&post_streaming_response(&first_process.responses_url()).await);
    assert_eq!(
        fake.upstream_requests()[0].account_id.as_deref(),
        Some("account-a")
    );
    first_process.stop();

    fake.clear_observations();
    fake.reject_next_upstream_requests(1);
    fake.reply_with(json!({ "access_token": final_access }));
    let restarted = fixture.start().await;
    assert_success(&post_streaming_response(&restarted.responses_url()).await);

    assert_oauth_grant(
        &fake.oauth_requests()[0],
        "rotated-after-claimless-id-token",
    );
    let upstream = fake.upstream_requests();
    assert_eq!(
        upstream
            .iter()
            .map(|request| request.authorization.clone())
            .collect::<Vec<_>>(),
        vec![Some(bearer(&rotated_access)), Some(bearer(&final_access))]
    );
    assert!(
        upstream
            .iter()
            .all(|request| request.account_id.as_deref() == Some("account-a"))
    );
}

#[tokio::test]
async fn oauth_output_with_header_control_characters_is_not_published_or_persisted() {
    let now = OffsetDateTime::now_utc();
    let invalid_responses = [
        json!({
            "id_token": id_token("account-a"),
            "access_token": "oauth-access-injection\r\nx-invalid-access",
            "refresh_token": "rotated-with-invalid-access"
        }),
        json!({
            "id_token": unsigned_jwt(json!({
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": "oauth-account-injection\r\nx-invalid-account"
                }
            })),
            "access_token": access_token("valid-access-invalid-account", now + Duration::hours(1)),
            "refresh_token": "rotated-with-invalid-account"
        }),
    ];

    for (case_index, invalid_response) in invalid_responses.into_iter().enumerate() {
        let fake = FakeCodexServer::start().await;
        fake.reply_with(invalid_response);
        let fixture = RelayFixture::new(
            &fake,
            &seed(
                access_token(
                    &format!("invalid-oauth-output-seed-{case_index}"),
                    now + Duration::minutes(4),
                ),
                "seed-refresh",
                "account-a",
                now,
            ),
        );

        let first_process = fixture.start().await;
        let rejected = post_streaming_response(&first_process.responses_url()).await;
        assert_eq!(
            rejected.status,
            StatusCode::BAD_GATEWAY,
            "{}",
            rejected.body
        );
        assert_eq!(fake.oauth_requests().len(), 1);
        assert_oauth_grant(&fake.oauth_requests()[0], "seed-refresh");
        assert!(
            fake.upstream_requests().is_empty(),
            "invalid refreshed header values must never reach the upstream"
        );
        first_process.stop();

        // If the failed refresh neither published nor persisted its generation,
        // restart still sees the expiring seed and exchanges its original token.
        fake.clear_observations();
        let recovered_access =
            access_token(&format!("recovered-{case_index}"), now + Duration::hours(2));
        fake.reply_with(full_oauth_reply(
            &recovered_access,
            "recovered-refresh",
            "account-a",
        ));
        let restarted = fixture.start().await;
        assert_success(&post_streaming_response(&restarted.responses_url()).await);
        assert_eq!(fake.oauth_requests().len(), 1);
        assert_oauth_grant(&fake.oauth_requests()[0], "seed-refresh");
        let upstream = fake.upstream_requests();
        assert_eq!(upstream.len(), 1);
        assert_eq!(
            upstream[0].authorization.as_deref(),
            Some(bearer(&recovered_access).as_str())
        );
        assert_eq!(upstream[0].account_id.as_deref(), Some("account-a"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceling_refresh_initiator_does_not_cancel_dispatched_rotation_or_duplicate_it() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let refreshed_access = access_token("cancellation-safe", now + Duration::hours(1));
    fake.reply_with_delay(
        full_oauth_reply(&refreshed_access, "cancellation-safe-refresh", "account-a"),
        StdDuration::from_millis(500),
    );
    let fixture = RelayFixture::new(
        &fake,
        &seed(
            access_token("cancel-initiator", now + Duration::minutes(4)),
            "seed-refresh",
            "account-a",
            now,
        ),
    );
    let relay = fixture.start().await;
    let responses_url = relay.responses_url();
    let initiating_url = responses_url.clone();
    let initiating = tokio::spawn(async move { post_streaming_response(&initiating_url).await });

    tokio::time::timeout(StdDuration::from_secs(2), async {
        while fake.oauth_requests().is_empty() {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("initiating OAuth exchange was not dispatched");
    initiating.abort();
    let initiating_result = initiating.await;
    assert!(
        initiating_result
            .as_ref()
            .is_err_and(tokio::task::JoinError::is_cancelled),
        "initiating request was not canceled"
    );

    let waiter = post_streaming_response(&responses_url).await;
    assert_success(&waiter);
    assert_eq!(
        fake.oauth_requests().len(),
        1,
        "canceling the initiating request must not cancel or duplicate a dispatched token rotation"
    );
    assert!(fake.upstream_requests().iter().all(
        |request| request.authorization.as_deref() == Some(bearer(&refreshed_access).as_str())
    ));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_waits_for_dispatched_rotation_to_persist_before_clean_exit() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let rotated_access = access_token("sigterm-rotated", now + Duration::hours(1));
    let release_oauth = fake.reply_after_release(full_oauth_reply(
        &rotated_access,
        "sigterm-rotated-refresh",
        "account-a",
    ));
    let fixture = RelayFixture::new(
        &fake,
        &seed(
            access_token("sigterm-expiring", now + Duration::minutes(4)),
            "stale-seed-refresh",
            "account-a",
            now,
        ),
    );
    let stale_auth_seed = std::fs::read(&fixture.auth_path).expect("read stale auth seed");
    let mut relay = fixture.start().await;
    let responses_url = relay.responses_url();
    let waiting_request = tokio::spawn(async move {
        let response = reqwest::Client::builder()
            .timeout(StdDuration::from_secs(5))
            .build()
            .expect("build shutdown-test client")
            .post(&responses_url)
            .bearer_auth(CLIENT_KEY)
            .json(&json!({
                "model": MODEL,
                "input": "Reply with OK.",
                "stream": true
            }))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        Ok::<PublicResponse, reqwest::Error>(PublicResponse { status, body })
    });

    tokio::time::timeout(StdDuration::from_secs(2), async {
        while fake.oauth_requests().is_empty() {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("OAuth provider never observed the dispatched rotation");
    assert_eq!(fake.oauth_requests().len(), 1);
    assert_oauth_grant(&fake.oauth_requests()[0], "stale-seed-refresh");

    let signal_status = Command::new("kill")
        .arg("-TERM")
        .arg(relay.child.id().to_string())
        .status()
        .expect("send SIGTERM to codex-api");
    assert!(signal_status.success(), "kill -TERM failed");
    tokio::time::sleep(StdDuration::from_millis(50)).await;
    assert!(
        relay
            .child
            .try_wait()
            .expect("poll relay after SIGTERM")
            .is_none(),
        "relay exited while an accepted OAuth rotation was still gated"
    );

    release_oauth.add_permits(1);
    let shutdown_response = tokio::time::timeout(StdDuration::from_secs(5), waiting_request)
        .await
        .expect("authenticated request did not finish during graceful shutdown")
        .expect("authenticated request task failed");
    if let Ok(response) = shutdown_response {
        assert_eq!(
            response.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            response.body
        );
        let body: Value = serde_json::from_str(&response.body)
            .expect("shutdown response must use the standard JSON error envelope");
        assert_eq!(
            body.pointer("/error/code").and_then(Value::as_str),
            Some("server_shutting_down")
        );
    }
    let exit_status = tokio::time::timeout(StdDuration::from_secs(5), async {
        loop {
            if let Some(status) = relay.child.try_wait().expect("poll graceful relay exit") {
                break status;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("relay did not exit within the graceful shutdown bound");
    assert!(exit_status.success(), "relay exited with {exit_status}");
    assert_eq!(
        std::fs::read(&fixture.auth_path).expect("read unchanged stale auth seed"),
        stale_auth_seed,
        "refresh must not rewrite the auth seed"
    );
    relay.stop();

    // The stale seed remains unchanged. Restart must therefore obtain both
    // rotated fields from SQLite and make no second OAuth exchange.
    fake.reply_with(full_oauth_reply(
        &access_token("unexpected-second-refresh", now + Duration::hours(2)),
        "unexpected-second-refresh-token",
        "account-a",
    ));
    let restarted = fixture.start().await;
    assert_success(&post_streaming_response(&restarted.responses_url()).await);
    assert_eq!(
        fake.oauth_requests().len(),
        1,
        "restart reused the stale seed instead of the persisted rotation"
    );
    let upstream = fake.upstream_requests();
    assert_eq!(upstream.len(), 1);
    assert!(upstream.iter().all(|request| {
        request.authorization.as_deref() == Some(bearer(&rotated_access).as_str())
            && request.account_id.as_deref() == Some("account-a")
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_proactive_expiry_checks_share_one_single_flight_refresh() {
    const REQUEST_COUNT: usize = 12;

    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let expiring_access = access_token("concurrent-expiring", now + Duration::minutes(4));
    let refreshed_access = access_token("proactively-refreshed", now + Duration::hours(2));
    fake.reply_with_delay(
        full_oauth_reply(&refreshed_access, "proactive-rotated-refresh", "account-a"),
        StdDuration::from_millis(150),
    );
    let fixture = RelayFixture::new(
        &fake,
        &seed(expiring_access, "seed-refresh", "account-a", now),
    );
    let relay = fixture.start().await;
    let responses_url = relay.responses_url();
    let barrier = Arc::new(Barrier::new(REQUEST_COUNT));
    let requests = (0..REQUEST_COUNT)
        .map(|_| {
            let barrier = barrier.clone();
            let responses_url = responses_url.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                post_streaming_response(&responses_url).await
            })
        })
        .collect::<Vec<_>>();

    tokio::time::timeout(StdDuration::from_secs(10), async {
        for request in requests {
            assert_success(&request.await.expect("concurrent proactive request task"));
        }
    })
    .await
    .expect("concurrent proactive refresh requests timed out");

    let oauth = fake.oauth_requests();
    assert_eq!(oauth.len(), 1, "proactive refresh must be single-flight");
    assert_oauth_grant(&oauth[0], "seed-refresh");
    let upstream = fake.upstream_requests();
    assert_eq!(upstream.len(), REQUEST_COUNT);
    assert!(
        upstream
            .iter()
            .all(|request| request.authorization.as_deref()
                == Some(bearer(&refreshed_access).as_str())),
        "all requests must wait for and use the refreshed token: {upstream:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_auth_failures_share_one_single_flight_refresh() {
    const REQUEST_COUNT: usize = 12;

    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let original_access = access_token("concurrent-original", now + Duration::hours(1));
    let refreshed_access = access_token("concurrent-refreshed", now + Duration::hours(2));
    fake.reject_authorization(bearer(&original_access), StdDuration::from_millis(100));
    fake.reply_with_delay(
        full_oauth_reply(&refreshed_access, "concurrent-rotated-refresh", "account-a"),
        StdDuration::from_millis(150),
    );
    let fixture = RelayFixture::new(
        &fake,
        &seed(original_access, "seed-refresh", "account-a", now),
    );
    let relay = fixture.start().await;
    let responses_url = relay.responses_url();
    let barrier = Arc::new(Barrier::new(REQUEST_COUNT));

    let mut requests = Vec::new();
    for _ in 0..REQUEST_COUNT {
        let barrier = barrier.clone();
        let responses_url = responses_url.clone();
        requests.push(tokio::spawn(async move {
            barrier.wait().await;
            post_streaming_response(&responses_url).await
        }));
    }
    for request in requests {
        assert_success(&request.await.expect("concurrent request task"));
    }

    let oauth = fake.oauth_requests();
    assert_eq!(oauth.len(), 1, "rotating refresh token must be used once");
    assert_oauth_grant(&oauth[0], "seed-refresh");
    let upstream = fake.upstream_requests();
    assert!(
        upstream
            .iter()
            .filter(|request| request.authorization.as_deref()
                == Some(bearer(&refreshed_access).as_str()))
            .count()
            >= REQUEST_COUNT,
        "every operation must eventually use the refreshed access token: {upstream:?}"
    );
    assert!(
        upstream
            .iter()
            .filter(|request| request.authorization.as_deref()
                == Some(
                    bearer(&access_token(
                        "concurrent-original",
                        now + Duration::hours(1)
                    ))
                    .as_str()
                ))
            .count()
            >= 2,
        "fixture must exercise concurrent 401 recovery: {upstream:?}"
    );
}

#[tokio::test]
async fn omitted_oauth_fields_preserve_each_existing_credential() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let original_access = access_token("optional-original", now + Duration::hours(1));
    let second_access = access_token("optional-second", now + Duration::hours(2));
    let third_access = access_token("optional-third", now + Duration::hours(3));
    let fixture = RelayFixture::new(
        &fake,
        &seed(&original_access, "refresh-1", "account-a", now),
    );
    let relay = fixture.start().await;

    // Omitting access_token and id_token keeps the access token and account while
    // accepting the rotated refresh token.
    fake.reject_next_upstream_requests(1);
    fake.reply_with(json!({ "refresh_token": "refresh-2" }));
    assert_success(&post_streaming_response(&relay.responses_url()).await);
    assert_eq!(
        fake.upstream_requests()
            .iter()
            .map(|request| request.authorization.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(bearer(&original_access)),
            Some(bearer(&original_access))
        ]
    );
    assert!(
        fake.upstream_requests()
            .iter()
            .all(|request| request.account_id.as_deref() == Some("account-a"))
    );

    // The next forced refresh must use the rotation saved above. Omitting
    // refresh_token and id_token now keeps both of those fields.
    fake.clear_observations();
    fake.reject_next_upstream_requests(1);
    fake.reply_with(json!({ "access_token": second_access }));
    assert_success(&post_streaming_response(&relay.responses_url()).await);
    assert_oauth_grant(&fake.oauth_requests()[0], "refresh-2");
    assert_eq!(
        fake.upstream_requests()[1].authorization.as_deref(),
        Some(bearer(&second_access).as_str())
    );
    assert!(
        fake.upstream_requests()
            .iter()
            .all(|request| request.account_id.as_deref() == Some("account-a"))
    );

    // A third recovery proves the omitted refresh_token remained refresh-2.
    fake.clear_observations();
    fake.reject_next_upstream_requests(1);
    fake.reply_with(json!({ "access_token": third_access }));
    assert_success(&post_streaming_response(&relay.responses_url()).await);
    assert_oauth_grant(&fake.oauth_requests()[0], "refresh-2");
}

#[tokio::test]
async fn rotated_access_and_refresh_tokens_persist_together_and_newer_db_wins_on_restart() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc();
    let seed_access = access_token("seed-expiring", now + Duration::minutes(4));
    let database_access = access_token("database-access", now + Duration::hours(1));
    let final_access = access_token("final-access", now + Duration::hours(2));
    fake.reply_with(full_oauth_reply(
        &database_access,
        "database-refresh",
        "account-a",
    ));
    let fixture = RelayFixture::new(&fake, &seed(seed_access, "seed-refresh", "account-a", now));
    let original_auth_file = std::fs::read(&fixture.auth_path).expect("read original auth seed");

    let first_process = fixture.start().await;
    assert_success(&post_streaming_response(&first_process.responses_url()).await);
    assert_oauth_grant(&fake.oauth_requests()[0], "seed-refresh");
    first_process.stop();

    assert!(
        fixture.database_path.is_file(),
        "SQLite state was not created"
    );
    assert_eq!(
        std::fs::read(&fixture.auth_path).expect("read unchanged auth seed"),
        original_auth_file,
        "the seed auth.json must never be rewritten"
    );

    // The unchanged seed is now older than the successful database refresh.
    // A 401 after restart exposes both halves of the persisted rotation: the
    // first attempt uses database_access, and OAuth receives database-refresh.
    fake.clear_observations();
    fake.reject_next_upstream_requests(1);
    fake.reply_with(full_oauth_reply(
        &final_access,
        "final-refresh",
        "account-a",
    ));
    let restarted = fixture.start().await;
    assert_success(&post_streaming_response(&restarted.responses_url()).await);

    let upstream = fake.upstream_requests();
    assert_eq!(
        upstream
            .iter()
            .map(|request| request.authorization.clone())
            .collect::<Vec<_>>(),
        vec![Some(bearer(&database_access)), Some(bearer(&final_access))]
    );
    assert_oauth_grant(&fake.oauth_requests()[0], "database-refresh");
}

#[tokio::test]
async fn auth_seed_replaces_database_state_only_when_strictly_newer() {
    let fake = FakeCodexServer::start().await;
    let now = OffsetDateTime::now_utc() - Duration::hours(1);
    let database_access = access_token("database-seed", now + Duration::hours(3));
    let equal_access = access_token("equal-seed", now + Duration::hours(3));
    let newer_access = access_token("newer-seed", now + Duration::hours(3));
    let fixture = RelayFixture::new(
        &fake,
        &seed(&database_access, "db-refresh", "account-db", now),
    );

    let first_process = fixture.start().await;
    assert_success(&post_streaming_response(&first_process.responses_url()).await);
    first_process.stop();
    assert!(
        fixture.database_path.is_file(),
        "SQLite state was not created"
    );

    fixture.write_seed(&seed(&equal_access, "equal-refresh", "account-equal", now));
    fake.clear_observations();
    let equal_restart = fixture.start().await;
    assert_success(&post_streaming_response(&equal_restart.responses_url()).await);
    assert_eq!(
        fake.upstream_requests()[0].authorization.as_deref(),
        Some(bearer(&database_access).as_str()),
        "equal last_refresh must not replace SQLite credentials"
    );
    assert_eq!(
        fake.upstream_requests()[0].account_id.as_deref(),
        Some("account-db")
    );
    assert!(fake.oauth_requests().is_empty());
    equal_restart.stop();

    fixture.write_seed(&seed(
        &newer_access,
        "newer-refresh",
        "account-newer",
        now + Duration::seconds(1),
    ));
    fake.clear_observations();
    let newer_restart = fixture.start().await;
    assert_success(&post_streaming_response(&newer_restart.responses_url()).await);
    assert_eq!(
        fake.upstream_requests()[0].authorization.as_deref(),
        Some(bearer(&newer_access).as_str()),
        "strictly newer auth seed must replace SQLite credentials"
    );
    assert_eq!(
        fake.upstream_requests()[0].account_id.as_deref(),
        Some("account-newer")
    );
    assert!(fake.oauth_requests().is_empty());
}
