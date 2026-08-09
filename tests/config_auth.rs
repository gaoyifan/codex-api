use std::{
    io::Read,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use tempfile::TempDir;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{net::TcpListener, task::JoinHandle, time::sleep};

const FIRST_KEY_ID: &str = "client-a";
const FIRST_KEY: &str = "sk-local-first-secret";
const SECOND_KEY_ID: &str = "client-b";
const SECOND_KEY: &str = "sk-local-second-secret";
const ACCOUNT_ID: &str = "account-id-must-not-leak";
const REFRESH_TOKEN: &str = "refresh-token-must-not-leak";
const MODEL: &str = "test-model";

#[derive(Debug)]
struct CapturedOutput {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl CapturedOutput {
    fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

#[derive(Debug)]
struct ServiceProcess {
    child: Option<Child>,
}

impl ServiceProcess {
    fn stop(mut self) -> CapturedOutput {
        let mut child = self.child.take().expect("running service process");
        let _ = child.kill();
        let status = child.wait().expect("wait for stopped service");
        collect_exited_child(child, status)
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Debug, Clone)]
struct ObservedRequest {
    path: String,
    authorization: Option<String>,
}

#[derive(Debug)]
struct FakeUpstream {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<ObservedRequest>>>,
    task: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let address = listener.local_addr().expect("fake upstream address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(any(fake_upstream_response))
            .with_state(Arc::clone(&requests));
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("fake upstream server failed");
        });

        Self {
            address,
            requests,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    fn requests(&self) -> Vec<ObservedRequest> {
        self.requests.lock().expect("request lock").clone()
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake_upstream_response(
    State(requests): State<Arc<Mutex<Vec<ObservedRequest>>>>,
    request: Request,
) -> Response {
    let authorization = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let path = request.uri().path().to_owned();
    requests
        .lock()
        .expect("request lock")
        .push(ObservedRequest {
            path: path.clone(),
            authorization,
        });

    if path != "/responses" {
        return (StatusCode::NOT_FOUND, "unexpected upstream path").into_response();
    }

    let event = json!({
        "type": "response.completed",
        "sequence_number": 1,
        "response": {
            "id": "resp_auth_test",
            "object": "response",
            "created_at": 1_700_000_000,
            "status": "completed",
            "model": MODEL,
            "output": [{
                "id": "msg_auth_test",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "authenticated",
                    "annotations": [],
                    "logprobs": []
                }]
            }],
            "usage": {
                "input_tokens": 2,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 1,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 3
            }
        }
    });
    let body = format!("event: response.completed\ndata: {event}\n\n");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .expect("build fake upstream response")
}

#[derive(Debug)]
struct Fixture {
    _directory: TempDir,
    config_path: PathBuf,
    auth_path: PathBuf,
    state_path: PathBuf,
    listen_address: SocketAddr,
    access_token: String,
}

impl Fixture {
    fn new(upstream_base_url: &str) -> Self {
        let directory = tempfile::tempdir().expect("create fixture directory");
        let config_path = directory.path().join("config.toml");
        let auth_path = directory.path().join("auth.json");
        let state_path = directory.path().join("state.sqlite3");
        let listen_address = unused_address();
        let access_token = future_access_token();
        write_auth_seed(&auth_path, &access_token);

        let fixture = Self {
            _directory: directory,
            config_path,
            auth_path,
            state_path,
            listen_address,
            access_token,
        };
        fixture.write_config(&fixture.valid_config(upstream_base_url));
        fixture
    }

    fn valid_config(&self, upstream_base_url: &str) -> String {
        format!(
            r#"[server]
listen = "{}"
enable_websockets = false

[state]
path = "{}"

[upstream]
base_url = "{}"
oauth_token_url = "{}/oauth/token"
auth_file = "{}"
supports_websockets = false

[[api_keys]]
id = "{FIRST_KEY_ID}"
secret = "{FIRST_KEY}"
weekly_limit_usd = "10.00"

[[api_keys]]
id = "{SECOND_KEY_ID}"
secret = "{SECOND_KEY}"

[model_prices."{MODEL}"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
            self.listen_address,
            self.state_path.display(),
            upstream_base_url,
            upstream_base_url,
            self.auth_path.display(),
        )
    }

    fn write_config(&self, contents: &str) {
        std::fs::write(&self.config_path, contents).expect("write test configuration");
    }
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_codex-api")
}

fn unused_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve test address");
    listener.local_addr().expect("reserved test address")
}

fn future_access_token() -> String {
    let expires_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_secs()
        + 86_400;
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(json!({"exp": expires_at}).to_string());
    format!("{header}.{payload}.test-signature")
}

fn write_auth_seed(path: &Path, access_token: &str) {
    let last_refresh = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("format current time");
    let auth = json!({
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": null,
        "tokens": {
            "id_token": "id-token-not-used",
            "access_token": access_token,
            "refresh_token": REFRESH_TOKEN,
            "account_id": ACCOUNT_ID
        },
        "last_refresh": last_refresh
    });
    std::fs::write(path, auth.to_string()).expect("write auth seed");
}

fn spawn_binary(args: &[&str]) -> Child {
    Command::new(binary())
        .args(args)
        .env_remove("RUST_LOG")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn codex-api")
}

fn collect_exited_child(mut child: Child, status: ExitStatus) -> CapturedOutput {
    let mut stdout = String::new();
    let mut stderr = String::new();
    child
        .stdout
        .take()
        .expect("captured stdout")
        .read_to_string(&mut stdout)
        .expect("read stdout");
    child
        .stderr
        .take()
        .expect("captured stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    CapturedOutput {
        status,
        stdout,
        stderr,
    }
}

async fn expect_startup_failure(args: &[&str]) -> CapturedOutput {
    let mut child = spawn_binary(args);
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        if let Some(status) = child.try_wait().expect("poll codex-api") {
            let output = collect_exited_child(child, status);
            assert!(
                !output.status.success(),
                "invalid startup unexpectedly succeeded: {}",
                output.combined()
            );
            return output;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().expect("wait after killing codex-api");
            let output = collect_exited_child(child, status);
            panic!(
                "invalid configuration was accepted and the process stayed running: {}",
                output.combined()
            );
        }
        sleep(Duration::from_millis(20)).await;
    }
}

async fn start_service(fixture: &Fixture) -> ServiceProcess {
    let config = fixture.config_path.to_str().expect("UTF-8 config path");
    let mut child = spawn_binary(&["--config", config]);
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        if tokio::net::TcpStream::connect(fixture.listen_address)
            .await
            .is_ok()
        {
            return ServiceProcess { child: Some(child) };
        }
        if let Some(status) = child.try_wait().expect("poll codex-api") {
            let output = collect_exited_child(child, status);
            panic!(
                "valid service exited before listening ({}): {}",
                output.status,
                output.combined()
            );
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().expect("wait after service startup timeout");
            let output = collect_exited_child(child, status);
            panic!(
                "valid service did not listen on {} within the deadline: {}",
                fixture.listen_address,
                output.combined()
            );
        }
        sleep(Duration::from_millis(25)).await;
    }
}

fn assert_error_mentions(output: &CapturedOutput, expected: &str) {
    let rendered = output.combined().to_lowercase();
    assert!(
        rendered.contains(expected),
        "startup error should mention {expected:?}, got: {rendered}"
    );
}

#[tokio::test]
async fn cli_requires_an_explicit_config_path() {
    let output = expect_startup_failure(&[]).await;
    assert_error_mentions(&output, "--config");
}

#[tokio::test]
async fn missing_configuration_file_fails_before_startup() {
    let directory = tempfile::tempdir().expect("create fixture directory");
    let missing = directory.path().join("does-not-exist.toml");
    let output =
        expect_startup_failure(&["--config", missing.to_str().expect("UTF-8 missing path")]).await;
    assert_error_mentions(&output, "config");
}

#[tokio::test]
async fn missing_required_configuration_fields_fail_before_listening() {
    let cases: [(&str, fn(&str) -> String, &str); 5] = [
        (
            "server listen",
            |config| config.replace(&format!("listen = \"{}\"\n", extract_listen(config)), ""),
            "listen",
        ),
        (
            "state path",
            |config| {
                let start = config.find("[state]\n").expect("state section");
                let value_start = start + "[state]\n".len();
                let value_end = config[value_start..]
                    .find('\n')
                    .map(|offset| value_start + offset + 1)
                    .expect("state path line");
                format!("{}{}", &config[..value_start], &config[value_end..])
            },
            "path",
        ),
        (
            "auth file",
            |config| {
                config
                    .lines()
                    .filter(|line| !line.starts_with("auth_file ="))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            "auth_file",
        ),
        (
            "API keys",
            |config| {
                let start = config.find("[[api_keys]]\n").expect("API key sections");
                let end = config.find("[model_prices.").expect("model prices section");
                format!("{}{}", &config[..start], &config[end..])
            },
            "api",
        ),
        (
            "model prices",
            |config| {
                let start = config.find("[model_prices.").expect("prices section");
                config[..start].to_owned()
            },
            "model",
        ),
    ];

    for (name, mutate, expected_error) in cases {
        let fixture = Fixture::new("http://127.0.0.1:9");
        let config = std::fs::read_to_string(&fixture.config_path).expect("read base config");
        fixture.write_config(&mutate(&config));
        let output = expect_startup_failure(&[
            "--config",
            fixture.config_path.to_str().expect("UTF-8 config path"),
        ])
        .await;
        assert_error_mentions(&output, expected_error);
        assert!(
            StdTcpListener::bind(fixture.listen_address).is_ok(),
            "{name} failure left the listen address occupied"
        );
    }
}

fn extract_listen(config: &str) -> String {
    config
        .lines()
        .find_map(|line| line.strip_prefix("listen = \"")?.strip_suffix('"'))
        .expect("listen setting")
        .to_owned()
}

#[tokio::test]
async fn unknown_fields_are_rejected_at_every_configuration_level() {
    let cases = [
        ("top-level", "\nunexpected = true\n", "unexpected"),
        ("server", "\nserver_typo = true\n", "server_typo"),
        ("state", "\nstate_typo = true\n", "state_typo"),
        ("upstream", "\nupstream_typo = true\n", "upstream_typo"),
        ("API key", "\nkey_typo = true\n", "key_typo"),
        ("model price", "\nprice_typo = true\n", "price_typo"),
    ];

    for (level, unknown_line, unknown_field) in cases {
        let fixture = Fixture::new("http://127.0.0.1:9");
        let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
        let mutated = match level {
            "top-level" => format!("{unknown_line}{base}"),
            "server" => base.replacen(
                "enable_websockets = false",
                &format!("enable_websockets = false{unknown_line}"),
                1,
            ),
            "state" => base.replacen(
                &format!("path = \"{}\"", fixture.state_path.display()),
                &format!("path = \"{}\"{unknown_line}", fixture.state_path.display()),
                1,
            ),
            "upstream" => base.replacen(
                "supports_websockets = false",
                &format!("supports_websockets = false{unknown_line}"),
                1,
            ),
            "API key" => base.replacen(
                "weekly_limit_usd = \"10.00\"",
                &format!("weekly_limit_usd = \"10.00\"{unknown_line}"),
                1,
            ),
            "model price" => format!("{base}{unknown_line}"),
            _ => unreachable!(),
        };
        fixture.write_config(&mutated);
        let output = expect_startup_failure(&[
            "--config",
            fixture.config_path.to_str().expect("UTF-8 config path"),
        ])
        .await;
        assert_error_mentions(&output, unknown_field);
    }
}

#[tokio::test]
async fn invalid_api_key_definitions_are_rejected() {
    let cases = [
        ("empty id", "id = \"client-a\"", "id = \"\"", "id"),
        (
            "empty secret",
            "secret = \"sk-local-first-secret\"",
            "secret = \"\"",
            "secret",
        ),
        (
            "duplicate id",
            "id = \"client-b\"",
            "id = \"client-a\"",
            "duplicate",
        ),
        (
            "duplicate secret",
            "secret = \"sk-local-second-secret\"",
            "secret = \"sk-local-first-secret\"",
            "duplicate",
        ),
    ];

    for (name, old, new, expected_error) in cases {
        let fixture = Fixture::new("http://127.0.0.1:9");
        let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
        fixture.write_config(&base.replacen(old, new, 1));
        let output = expect_startup_failure(&[
            "--config",
            fixture.config_path.to_str().expect("UTF-8 config path"),
        ])
        .await;
        assert_error_mentions(&output, expected_error);
        assert!(
            !output.status.success(),
            "invalid API key case {name} was accepted"
        );
    }
}

#[tokio::test]
async fn invalid_decimal_prices_and_limits_are_rejected() {
    let cases = [
        (
            "negative weekly limit",
            "weekly_limit_usd = \"10.00\"",
            "weekly_limit_usd = \"-0.01\"",
        ),
        (
            "non-decimal weekly limit",
            "weekly_limit_usd = \"10.00\"",
            "weekly_limit_usd = \"ten\"",
        ),
        (
            "negative input price",
            "input_usd_per_million = \"1.00\"",
            "input_usd_per_million = \"-1.00\"",
        ),
        (
            "invalid cached price",
            "cached_input_usd_per_million = \"0.10\"",
            "cached_input_usd_per_million = \"free\"",
        ),
        (
            "numeric values are not decimal strings",
            "output_usd_per_million = \"6.00\"",
            "output_usd_per_million = 6.00",
        ),
    ];

    for (name, old, new) in cases {
        let fixture = Fixture::new("http://127.0.0.1:9");
        let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
        fixture.write_config(&base.replacen(old, new, 1));
        let output = expect_startup_failure(&[
            "--config",
            fixture.config_path.to_str().expect("UTF-8 config path"),
        ])
        .await;
        assert!(
            !output.status.success(),
            "invalid money case {name} was accepted"
        );
    }
}

#[tokio::test]
async fn unreadable_or_invalid_auth_seed_fails_before_listening() {
    let fixture = Fixture::new("http://127.0.0.1:9");
    std::fs::remove_file(&fixture.auth_path).expect("remove auth seed");
    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    assert_error_mentions(&output, "auth");

    let fixture = Fixture::new("http://127.0.0.1:9");
    std::fs::write(&fixture.auth_path, "not JSON").expect("write malformed auth seed");
    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    assert_error_mentions(&output, "auth");

    let fixture = Fixture::new("http://127.0.0.1:9");
    std::fs::write(
        &fixture.auth_path,
        r#"{"auth_mode":"chatgpt","tokens":{"access_token":"auth-structure-secret-must-not-leak"}}"#,
    )
    .expect("write structurally invalid auth seed");
    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    assert_error_mentions(&output, "auth");
    assert!(
        !output
            .combined()
            .contains("auth-structure-secret-must-not-leak")
    );
}

#[tokio::test]
async fn malformed_listen_address_and_uncreatable_sqlite_path_fail_startup() {
    let fixture = Fixture::new("http://127.0.0.1:9");
    let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
    fixture.write_config(&base.replace(
        &format!("listen = \"{}\"", fixture.listen_address),
        "listen = \"not-an-address\"",
    ));
    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    assert_error_mentions(&output, "listen");

    let fixture = Fixture::new("http://127.0.0.1:9");
    let missing_parent_state = fixture
        ._directory
        .path()
        .join("missing-parent")
        .join("state.sqlite3");
    let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
    fixture.write_config(&base.replace(
        &format!("path = \"{}\"", fixture.state_path.display()),
        &format!("path = \"{}\"", missing_parent_state.display()),
    ));
    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    assert_error_mentions(&output, "sqlite");
}

#[tokio::test]
async fn downstream_websockets_cannot_be_enabled_when_upstream_lacks_support() {
    let fixture = Fixture::new("http://127.0.0.1:9");
    let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
    fixture.write_config(&base.replacen(
        "enable_websockets = false",
        "enable_websockets = true",
        1,
    ));
    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    assert_error_mentions(&output, "websocket");
}

#[tokio::test]
async fn startup_errors_and_logs_never_reveal_configured_secrets() {
    let fixture = Fixture::new("http://127.0.0.1:9");
    let base = std::fs::read_to_string(&fixture.config_path).expect("read base config");
    fixture.write_config(&base.replace(
        "output_usd_per_million = \"6.00\"",
        "output_usd_per_million = \"invalid\"",
    ));

    let output = expect_startup_failure(&[
        "--config",
        fixture.config_path.to_str().expect("UTF-8 config path"),
    ])
    .await;
    let rendered = output.combined();
    for secret in [
        FIRST_KEY,
        SECOND_KEY,
        fixture.access_token.as_str(),
        REFRESH_TOKEN,
        ACCOUNT_ID,
    ] {
        assert!(
            !rendered.contains(secret),
            "startup output exposed configured secret {secret:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_malformed_and_incorrect_bearer_credentials_are_rejected_locally() {
    let upstream = FakeUpstream::start().await;
    let fixture = Fixture::new(&upstream.base_url());
    let service = start_service(&fixture).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build HTTP client");

    let endpoints = [
        (
            "/v1/responses",
            json!({"model": MODEL, "input": "hello", "stream": true}),
        ),
        (
            "/v1/chat/completions",
            json!({
                "model": MODEL,
                "messages": [{"role": "user", "content": "hello"}]
            }),
        ),
    ];
    let credentials = [
        None,
        Some("Basic not-a-bearer-token"),
        Some("Bearer definitely-wrong"),
        Some("Bearer wrong extra-token"),
    ];

    for (path, body) in endpoints {
        for authorization in credentials {
            let mut request = client
                .post(format!("http://{}{path}", fixture.listen_address))
                .json(&body);
            if let Some(authorization) = authorization {
                request = request.header("authorization", authorization);
            }
            let response = request.send().await.expect("send unauthorized request");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            let response_body = response.text().await.expect("read auth error body");
            let error: Value = serde_json::from_str(&response_body)
                .unwrap_or_else(|_| panic!("auth error was not JSON: {response_body}"));
            assert_eq!(
                error.pointer("/error/type"),
                Some(&json!("invalid_request_error"))
            );
            assert_eq!(
                error.pointer("/error/code"),
                Some(&json!("invalid_api_key"))
            );
            assert!(!response_body.contains(FIRST_KEY));
            assert!(!response_body.contains(SECOND_KEY));
            if let Some(provided) = authorization {
                assert!(!response_body.contains(provided));
            }
        }
    }

    sleep(Duration::from_millis(50)).await;
    assert!(
        upstream.requests().is_empty(),
        "unauthenticated requests reached the upstream boundary"
    );

    let output = service.stop();
    let operational_output = output.combined();
    for secret in [FIRST_KEY, SECOND_KEY, ACCOUNT_ID, REFRESH_TOKEN] {
        assert!(
            !operational_output.contains(secret),
            "operational output exposed secret {secret:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_configured_key_can_use_the_responses_and_chat_http_apis() {
    let upstream = FakeUpstream::start().await;
    let fixture = Fixture::new(&upstream.base_url());
    let service = start_service(&fixture).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("build HTTP client");

    let responses = client
        .post(format!("http://{}/v1/responses", fixture.listen_address))
        .bearer_auth(FIRST_KEY)
        .json(&json!({"model": MODEL, "input": "hello", "stream": true}))
        .send()
        .await
        .expect("send Responses request");
    assert_eq!(responses.status(), StatusCode::OK);
    assert_eq!(
        responses
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value)),
        Some("text/event-stream")
    );
    let responses_body = responses.text().await.expect("read Responses stream");
    assert!(responses_body.contains("event: response.completed"));
    assert!(responses_body.contains("resp_auth_test"));

    let chat = client
        .post(format!(
            "http://{}/v1/chat/completions",
            fixture.listen_address
        ))
        .bearer_auth(SECOND_KEY)
        .json(&json!({
            "model": MODEL,
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .expect("send Chat Completions request");
    assert_eq!(chat.status(), StatusCode::OK);
    let chat_body: Value = chat.json().await.expect("read Chat Completion JSON");
    assert_eq!(chat_body["object"], "chat.completion");
    assert_eq!(chat_body["id"], "resp_auth_test");
    assert_eq!(
        chat_body["choices"][0]["message"]["content"],
        "authenticated"
    );

    let observed = upstream.requests();
    assert_eq!(
        observed.len(),
        2,
        "both valid requests should reach upstream"
    );
    let expected_upstream_authorization = format!("Bearer {}", fixture.access_token);
    let first_downstream_authorization = format!("Bearer {FIRST_KEY}");
    let second_downstream_authorization = format!("Bearer {SECOND_KEY}");
    for request in observed {
        assert_eq!(request.path, "/responses");
        assert_eq!(
            request.authorization.as_deref(),
            Some(expected_upstream_authorization.as_str())
        );
        assert_ne!(
            request.authorization.as_deref(),
            Some(first_downstream_authorization.as_str())
        );
        assert_ne!(
            request.authorization.as_deref(),
            Some(second_downstream_authorization.as_str())
        );
    }

    let output = service.stop();
    let operational_output = output.combined();
    for secret in [
        FIRST_KEY,
        SECOND_KEY,
        fixture.access_token.as_str(),
        REFRESH_TOKEN,
        ACCOUNT_ID,
    ] {
        assert!(
            !operational_output.contains(secret),
            "operational output exposed secret {secret:?}"
        );
    }
}
