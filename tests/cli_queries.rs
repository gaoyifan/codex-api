use std::process::Command;

use sqlx::{
    Connection, Executor,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use tempfile::TempDir;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_codex-api")
}

struct Fixture {
    _directory: TempDir,
    config_path: std::path::PathBuf,
    database_path: std::path::PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("create CLI fixture directory");
        let config_path = directory.path().join("config.toml");
        let database_path = directory.path().join("state.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(&database_path)
            .create_if_missing(true);
        let mut connection = sqlx::SqliteConnection::connect_with(&options)
            .await
            .expect("open CLI fixture database");
        sqlx::migrate!("./migrations")
            .run(&mut connection)
            .await
            .expect("migrate CLI fixture database");
        connection.close().await.expect("close fixture database");

        std::fs::write(
            &config_path,
            format!(
                r#"[server]
listen = "127.0.0.1:8080"
enable_websockets = false

[state]
path = "{}"

[upstream]
auth_file = "{}"
supports_websockets = false

[[api_keys]]
id = "client-a"
secret = "secret-must-not-be-printed"
weekly_limit_usd = "1.00"

[[api_keys]]
id = "client-unlimited"
secret = "second-secret-must-not-be-printed"

[[api_keys]]
id = "client-available"
secret = "third-secret-must-not-be-printed"
weekly_limit_usd = "0.50"

[[api_keys]]
id = "client-empty"
secret = "fourth-secret-must-not-be-printed"
weekly_limit_usd = "0.75"

[model_prices."gpt-query"]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
"#,
                database_path.display(),
                directory.path().join("auth.json").display()
            ),
        )
        .expect("write CLI fixture config");

        Self {
            _directory: directory,
            config_path,
            database_path,
        }
    }

    fn command(&self, command: &str) -> Command {
        let mut process = Command::new(binary());
        process.arg("--config").arg(&self.config_path).arg(command);
        process
    }

    async fn execute(&self, sql: &str) {
        let options = SqliteConnectOptions::new().filename(&self.database_path);
        let mut connection = sqlx::SqliteConnection::connect_with(&options)
            .await
            .expect("open fixture for insertion");
        connection
            .execute(sqlx::AssertSqlSafe(sql.to_owned()))
            .await
            .expect("insert fixture row");
        connection.close().await.expect("close fixture insertion");
    }
}

#[test]
fn cli_requires_an_explicit_command_and_advertises_serve() {
    let help = Command::new(binary())
        .arg("--help")
        .output()
        .expect("run codex-api help");
    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout).expect("UTF-8 help output");
    assert!(
        stdout.contains("serve"),
        "help did not advertise serve: {stdout}"
    );
    assert!(stdout.contains("CODEX_API_CONFIG"), "{stdout}");
    assert!(stdout.contains("/etc/codex-api/config.toml"), "{stdout}");

    let missing_command = Command::new(binary())
        .args(["--config", "unused.toml"])
        .output()
        .expect("run codex-api without a command");
    assert!(!missing_command.status.success());
    let stderr = String::from_utf8(missing_command.stderr).expect("UTF-8 CLI error");
    assert!(
        stderr.contains("<COMMAND>"),
        "missing-command error was not actionable: {stderr}"
    );
}

#[tokio::test]
async fn cli_discovers_config_from_environment_and_explicit_flag_takes_precedence() {
    let fixture = Fixture::new().await;

    let from_environment = Command::new(binary())
        .env("CODEX_API_CONFIG", &fixture.config_path)
        .arg("logs")
        .output()
        .expect("run logs with environment config");
    assert!(
        from_environment.status.success(),
        "environment config failed: {}",
        String::from_utf8_lossy(&from_environment.stderr)
    );

    let explicit_override = Command::new(binary())
        .env(
            "CODEX_API_CONFIG",
            fixture._directory.path().join("missing.toml"),
        )
        .arg("--config")
        .arg(&fixture.config_path)
        .arg("logs")
        .output()
        .expect("run logs with explicit config override");
    assert!(
        explicit_override.status.success(),
        "explicit config override failed: {}",
        String::from_utf8_lossy(&explicit_override.stderr)
    );
}

#[tokio::test]
async fn logs_renders_a_compact_human_readable_request_table() {
    let fixture = Fixture::new().await;
    fixture
        .execute(
            "INSERT INTO request_ledger (requested_at_ms, finished_at_ms, api_key_id, model, \
             reasoning_effort, api_protocol, transport, input_tokens, cached_input_tokens, \
             output_tokens, cost_nano_usd, duration_ms, status, http_status) VALUES \
             (1786365296000, 1786365296123, 'client-a', 'gpt-query', 'high', 'responses', \
              'websocket', 1234, 234, 56, 58300, 123, 'completed', NULL)",
        )
        .await;

    let output = fixture.command("logs").output().expect("run logs command");
    assert!(
        output.status.success(),
        "logs failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 logs output");
    for expected in [
        "API KEY",
        "TIME UTC",
        "MODEL (REASONING)",
        "PROTOCOL",
        "TOKENS I/C/O (K)",
        "COST USD",
        "DURATION",
        "STATUS",
        "client-a",
        "2026-08-10T12:34:56Z",
        "gpt-query (high)",
        "responses/websocket",
        "1.234/0.234/0.056",
        "0.000058300",
        "123 ms",
        "completed",
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in:\n{stdout}"
        );
    }
    assert!(!stdout.contains("secret-must-not-be-printed"));
}

#[tokio::test]
async fn logs_combines_common_filters_and_limits_newest_first() {
    let fixture = Fixture::new().await;
    fixture
        .execute(
            "INSERT INTO request_ledger (requested_at_ms, api_key_id, model, reasoning_effort, \
             api_protocol, transport, status) VALUES \
             (1786361400000, 'client-a', 'gpt-query', NULL, 'responses', 'http_sse', 'completed'), \
             (1786365296000, 'client-a', 'gpt-query', NULL, 'responses', 'http_sse', 'completed'), \
             (1786365356000, 'client-unlimited', 'gpt-query', NULL, 'responses', 'http_sse', 'completed'), \
             (1786365416000, 'client-a', 'other-model', NULL, 'responses', 'http_sse', 'completed'), \
             (1786365476000, 'client-a', 'gpt-query', NULL, 'responses', 'http_sse', 'canceled')",
        )
        .await;

    let output = fixture
        .command("logs")
        .args([
            "--limit",
            "1",
            "--api-key-id",
            "client-a",
            "--model",
            "gpt-query",
            "--status",
            "completed",
            "--since",
            "2026-08-10T12:00:00Z",
            "--until",
            "2026-08-10T13:00:00Z",
        ])
        .output()
        .expect("run filtered logs command");
    assert!(
        output.status.success(),
        "filtered logs failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 filtered logs output");
    assert!(stdout.contains("2026-08-10T12:34:56Z"), "{stdout}");
    assert!(!stdout.contains("2026-08-10T11:30:00Z"), "{stdout}");
    assert!(!stdout.contains("client-unlimited"), "{stdout}");
    assert!(!stdout.contains("other-model"), "{stdout}");
    assert!(!stdout.contains("canceled"), "{stdout}");
}

#[tokio::test]
async fn quota_lists_every_configured_key_for_the_current_utc_week() {
    let fixture = Fixture::new().await;
    let now = OffsetDateTime::now_utc();
    let week_start = (now.date()
        - Duration::days(i64::from(now.weekday().number_days_from_monday())))
    .midnight()
    .assume_utc();
    let week_start_ms = week_start.unix_timestamp() * 1_000;
    fixture
        .execute(&format!(
            "INSERT INTO request_ledger (requested_at_ms, api_key_id, model, api_protocol, \
             transport, cost_nano_usd, status) VALUES \
             ({}, 'client-a', 'gpt-query', 'responses', 'http_sse', 1000000000, 'completed'), \
             ({}, 'client-unlimited', 'gpt-query', 'responses', 'http_sse', 2500000000, 'completed'), \
             ({}, 'client-available', 'gpt-query', 'responses', 'http_sse', 250000000, 'completed'), \
             ({}, 'client-a', 'gpt-query', 'responses', 'http_sse', 9000000000, 'completed'), \
             ({}, 'client-a', 'gpt-query', 'responses', 'http_sse', NULL, 'upstream_error')",
            week_start_ms + 1_000,
            week_start_ms + 2_000,
            week_start_ms + 3_000,
            week_start_ms - 1_000,
            week_start_ms + 4_000,
        ))
        .await;

    let output = fixture
        .command("quota")
        .output()
        .expect("run quota command");
    assert!(
        output.status.success(),
        "quota failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 quota output");
    let formatted_week = week_start.format(&Rfc3339).expect("format week start");
    assert!(
        stdout.contains(&format!("Week starting {formatted_week}")),
        "{stdout}"
    );
    for expected in [
        "API KEY",
        "SPENT USD",
        "LIMIT USD",
        "HARD USD",
        "REMAINING USD",
        "STATUS",
        "client-a",
        "1.000000000",
        "blocked",
        "599",
        "client-unlimited",
        "2.500000000",
        "unlimited",
        "client-available",
        "0.250000000",
        "available",
        "599.75",
        "client-empty",
        "0.000000000",
        "600",
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in:\n{stdout}"
        );
    }
    let client_a = stdout.find("client-a").expect("client-a row");
    let unlimited = stdout
        .find("client-unlimited")
        .expect("client-unlimited row");
    let available = stdout
        .find("client-available")
        .expect("client-available row");
    let empty = stdout.find("client-empty").expect("client-empty row");
    assert!(
        client_a < unlimited && unlimited < available && available < empty,
        "{stdout}"
    );
    assert!(
        !stdout.contains("9.000000000"),
        "previous week leaked into quota:\n{stdout}"
    );
}

#[tokio::test]
async fn quota_reports_fallback_status_when_soft_limit_is_exhausted_with_fallback_model() {
    let fixture = Fixture::new().await;
    let config = std::fs::read_to_string(&fixture.config_path).expect("read CLI config");
    std::fs::write(
        &fixture.config_path,
        format!("fallback_model = \"gpt-query\"\n{config}"),
    )
    .expect("enable fallback model for quota CLI");
    let now = OffsetDateTime::now_utc();
    let week_start = (now.date()
        - Duration::days(i64::from(now.weekday().number_days_from_monday())))
    .midnight()
    .assume_utc();
    let week_start_ms = week_start.unix_timestamp() * 1_000;
    fixture
        .execute(&format!(
            "INSERT INTO request_ledger (requested_at_ms, api_key_id, model, api_protocol, \
             transport, cost_nano_usd, status) VALUES \
             ({}, 'client-available', 'gpt-query', 'responses', 'http_sse', 500000000, 'completed')",
            week_start_ms + 1_000,
        ))
        .await;

    let output = fixture
        .command("quota")
        .output()
        .expect("run quota command");
    assert!(
        output.status.success(),
        "quota failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 quota output");
    assert!(
        stdout.contains("fallback"),
        "expected fallback status in:\n{stdout}"
    );
    assert!(
        stdout.contains("599.5"),
        "expected remaining hard budget in:\n{stdout}"
    );
}

#[tokio::test]
async fn logs_defaults_to_the_latest_twenty_rows() {
    let fixture = Fixture::new().await;
    let values = (0..21)
        .map(|index| {
            format!(
                "({}, 'client-a', 'model-{index:02}', 'responses', 'http_sse', 'started')",
                1_786_365_000_000_i64 + i64::from(index) * 1_000
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    fixture
        .execute(&format!(
            "INSERT INTO request_ledger (requested_at_ms, api_key_id, model, api_protocol, \
             transport, status) VALUES {values}"
        ))
        .await;

    let output = fixture.command("logs").output().expect("run default logs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 default logs output");
    assert!(stdout.contains("model-20"), "{stdout}");
    assert!(stdout.contains("model-01"), "{stdout}");
    assert!(!stdout.contains("model-00"), "{stdout}");
    assert!(
        stdout.find("model-20").expect("newest row")
            < stdout.find("model-19").expect("second newest row"),
        "{stdout}"
    );
    assert!(
        stdout.contains("—/—/—"),
        "missing token marker absent:\n{stdout}"
    );
}

#[tokio::test]
async fn logs_rejects_invalid_filters_with_actionable_errors() {
    let fixture = Fixture::new().await;
    let cases = [
        (vec!["--limit", "0"], "0"),
        (vec!["--status", "not-a-status"], "not-a-status"),
        (vec!["--since", "not-a-time"], "not-a-time"),
        (
            vec!["--since", "2026-08-10T12:00:00.250Z"],
            "whole-second precision",
        ),
        (
            vec![
                "--since",
                "2026-08-10T13:00:00Z",
                "--until",
                "2026-08-10T12:00:00Z",
            ],
            "--since must be earlier than --until",
        ),
    ];
    for (args, expected) in cases {
        let output = fixture
            .command("logs")
            .args(args)
            .output()
            .expect("run invalid logs command");
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).expect("UTF-8 filter error");
        assert!(
            stderr.contains(expected),
            "missing {expected:?} in: {stderr}"
        );
    }
}

#[tokio::test]
async fn query_commands_are_read_only_and_never_print_configured_secrets() {
    let fixture = Fixture::new().await;
    let before = std::fs::read(&fixture.database_path).expect("read database before queries");
    for command in ["logs", "quota"] {
        let output = fixture
            .command(command)
            .output()
            .expect("run query command");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rendered = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for secret in [
            "secret-must-not-be-printed",
            "second-secret-must-not-be-printed",
            "third-secret-must-not-be-printed",
            "fourth-secret-must-not-be-printed",
        ] {
            assert!(
                !rendered.contains(secret),
                "query leaked {secret:?}: {rendered}"
            );
        }
    }
    let after = std::fs::read(&fixture.database_path).expect("read database after queries");
    assert_eq!(before, after, "query command modified the SQLite main file");

    std::fs::remove_file(&fixture.database_path).expect("remove fixture database");
    let output = fixture
        .command("logs")
        .output()
        .expect("query missing database");
    assert!(!output.status.success());
    assert!(
        !fixture.database_path.exists(),
        "read-only query recreated the missing database"
    );
}

#[tokio::test]
async fn logs_can_read_committed_state_while_a_wal_writer_is_active() {
    let fixture = Fixture::new().await;
    fixture
        .execute(
            "INSERT INTO request_ledger (requested_at_ms, api_key_id, model, api_protocol, \
             transport, status) VALUES \
             (1786365296000, 'client-a', 'committed-model', 'responses', 'http_sse', 'started')",
        )
        .await;
    let options = SqliteConnectOptions::new()
        .filename(&fixture.database_path)
        .journal_mode(SqliteJournalMode::Wal);
    let mut writer = sqlx::SqliteConnection::connect_with(&options)
        .await
        .expect("open WAL writer");
    writer
        .execute("BEGIN IMMEDIATE")
        .await
        .expect("hold WAL write transaction");

    let output = fixture.command("logs").output().expect("query active WAL");
    assert!(
        output.status.success(),
        "logs could not read active WAL: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 WAL logs output");
    assert!(stdout.contains("committed-model"), "{stdout}");

    writer
        .execute("ROLLBACK")
        .await
        .expect("release WAL writer");
    writer.close().await.expect("close WAL writer");
}
