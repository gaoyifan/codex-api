use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args as ClapArgs, Parser, Subcommand};
use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};
use rust_decimal::Decimal;
use secrecy::ExposeSecret;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, QueryBuilder, Row, Sqlite, SqliteConnection};
use time::{
    Date, Duration as TimeDuration, OffsetDateTime, UtcOffset,
    format_description::{self, well_known::Rfc3339},
};

use crate::config::Config;

#[derive(Debug, Parser)]
#[command(name = "codex-api")]
struct Args {
    #[arg(
        long,
        global = true,
        env = "CODEX_API_CONFIG",
        default_value = "/etc/codex-api/config.toml",
        help = "Path to the configuration file"
    )]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the API relay.
    Serve,
    /// Show recent request logs.
    Logs(LogsArgs),
    /// Show recent speech and transcription request logs.
    AudioLogs {
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..))]
        limit: u32,
    },
    /// Show usage statistics for every configured API key.
    Stat(StatArgs),
    /// Show the Codex account's live quota usage (requires the running relay).
    Usage,
}

#[derive(Debug, ClapArgs)]
struct LogsArgs {
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..))]
    limit: u32,
    #[arg(long)]
    api_key_id: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long, value_parser = ["started", "completed", "incomplete", "rejected", "upstream_error", "canceled", "internal_error"])]
    status: Option<String>,
    #[arg(long, value_parser = parse_rfc3339)]
    since: Option<OffsetDateTime>,
    #[arg(long, value_parser = parse_rfc3339)]
    until: Option<OffsetDateTime>,
}

#[derive(Debug, ClapArgs)]
struct StatArgs {
    #[arg(long, value_parser = parse_date)]
    start_date: Option<Date>,
    #[arg(long, value_parser = parse_date)]
    end_date: Option<Date>,
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Serve => crate::run(&args.config).await,
        Command::Logs(logs) => print_logs(&args.config, logs).await,
        Command::AudioLogs { limit } => print_audio_logs(&args.config, limit).await,
        Command::Stat(stat) => print_stat(&args.config, stat).await,
        Command::Usage => print_usage(&args.config).await,
    }
}

async fn print_usage(config_path: &Path) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let mut address = config.server.listen;
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .no_proxy()
        .build()?
        .get(format!("http://{address}/v1/usage"))
        .bearer_auth(config.api_keys[0].secret.expose_secret())
        .send()
        .await
        .context("failed to query the local relay; start codex-api serve first")?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("relay usage request failed (HTTP {status})"));
    }
    let usage: crate::usage::AccountUsage = response
        .json()
        .await
        .context("relay returned an invalid usage response")?;
    println!("Codex account usage — plan: {}", usage.plan_type);
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(["LIMIT", "WINDOW", "USED", "RESETS LOCAL", "STATUS"]);
    let limits = std::iter::once(("Codex", usage.rate_limit.as_ref())).chain(
        usage
            .additional_rate_limits
            .iter()
            .flatten()
            .map(|limit| (limit.limit_name.as_str(), limit.rate_limit.as_ref())),
    );
    for (name, limit) in limits {
        let Some(limit) = limit else {
            table.add_row([name, "—", "—", "—", "unavailable"]);
            continue;
        };
        let status = if limit.limit_reached {
            "limit reached"
        } else if limit.allowed {
            "available"
        } else {
            "blocked"
        };
        let windows = [
            limit.primary_window.as_ref(),
            limit.secondary_window.as_ref(),
        ];
        if windows.iter().all(Option::is_none) {
            table.add_row([name, "—", "—", "—", status]);
        }
        for window in windows.into_iter().flatten() {
            let seconds = window.limit_window_seconds;
            let duration = if seconds % 86400 == 0 {
                format!("{}d", seconds / 86400)
            } else if seconds % 3600 == 0 {
                format!("{}h", seconds / 3600)
            } else {
                format!("{}s", seconds)
            };
            let reset = OffsetDateTime::from_unix_timestamp(window.reset_at)?;
            table.add_row([
                name.to_owned(),
                duration,
                format!("{:.1}%", window.used_percent),
                format_local_timestamp(reset)?,
                status.to_owned(),
            ]);
        }
    }
    println!("{table}");
    if let Some(credits) = usage.credits {
        let balance = if credits.unlimited {
            "unlimited".to_owned()
        } else {
            credits.balance.unwrap_or_else(|| "—".to_owned())
        };
        println!("Credits: {balance} (has credits: {})", credits.has_credits);
    }
    Ok(())
}

async fn print_logs(config_path: &Path, args: LogsArgs) -> anyhow::Result<()> {
    if args
        .since
        .zip(args.until)
        .is_some_and(|(since, until)| since >= until)
    {
        return Err(anyhow!("--since must be earlier than --until"));
    }
    let config = Config::load(config_path)?;
    let mut connection = open_read_only(&config.state.path).await?;
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT requested_at, api_key_id, model, reasoning_effort, api_protocol, transport, \
                input_tokens, cached_input_tokens, output_tokens, cost_usd, duration_ms, status \
         FROM request_logs WHERE 1 = 1",
    );
    if let Some(api_key_id) = args.api_key_id {
        query.push(" AND api_key_id = ").push_bind(api_key_id);
    }
    if let Some(model) = args.model {
        query.push(" AND model = ").push_bind(model);
    }
    if let Some(status) = args.status {
        query.push(" AND status = ").push_bind(status);
    }
    if let Some(since) = args.since {
        query
            .push(" AND julianday(requested_at) >= julianday(")
            .push_bind(since.format(&Rfc3339)?)
            .push(")");
    }
    if let Some(until) = args.until {
        query
            .push(" AND julianday(requested_at) < julianday(")
            .push_bind(until.format(&Rfc3339)?)
            .push(")");
    }
    query
        .push(" ORDER BY id DESC LIMIT ")
        .push_bind(i64::from(args.limit));
    let rows = query
        .build()
        .fetch_all(&mut connection)
        .await
        .context("failed to query request logs")?;

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            "API KEY",
            "TIME LOCAL",
            "MODEL (REASONING)",
            "PROTOCOL",
            "TOKENS I/C/O (K)",
            "COST USD",
            "DURATION",
            "STATUS",
        ]);
    for row in rows {
        let requested_at = row.try_get::<String, _>("requested_at")?;
        let requested_at = OffsetDateTime::parse(&requested_at, &Rfc3339)
            .context("request_logs contains an invalid requested_at value")?;
        let requested_at = format_local_timestamp(requested_at)?;
        let model = row.try_get::<String, _>("model")?;
        let reasoning = row.try_get::<Option<String>, _>("reasoning_effort")?;
        let model = match reasoning {
            Some(reasoning) => format!("{model} ({reasoning})"),
            None => model,
        };
        let protocol = format!(
            "{}/{}",
            row.try_get::<String, _>("api_protocol")?,
            row.try_get::<String, _>("transport")?
        );
        let tokens = format!(
            "{}/{}/{}",
            format_k_tokens(row.try_get("input_tokens")?),
            format_k_tokens(row.try_get("cached_input_tokens")?),
            format_k_tokens(row.try_get("output_tokens")?),
        );
        let cost = row
            .try_get::<Option<String>, _>("cost_usd")?
            .unwrap_or_else(|| "—".to_owned());
        let duration = row
            .try_get::<Option<i64>, _>("duration_ms")?
            .map(|duration| format!("{duration} ms"))
            .unwrap_or_else(|| "—".to_owned());
        table.add_row([
            row.try_get::<String, _>("api_key_id")?,
            requested_at,
            model,
            protocol,
            tokens,
            cost,
            duration,
            row.try_get::<String, _>("status")?,
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn print_audio_logs(config_path: &Path, limit: u32) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let mut connection = open_read_only(&config.state.path).await?;
    let rows = sqlx::query(
        "SELECT strftime('%Y-%m-%dT%H:%M:%SZ', requested_at_ms / 1000.0, 'unixepoch') AS requested_at, \
                api_key_id, endpoint, duration_ms, http_status \
         FROM audio_request_logs ORDER BY id DESC LIMIT ?",
    )
    .bind(i64::from(limit))
    .fetch_all(&mut connection)
    .await
    .context("failed to query audio request logs")?;
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            "API KEY",
            "TIME LOCAL",
            "ENDPOINT",
            "DURATION",
            "HTTP STATUS",
        ]);
    for row in rows {
        let timestamp =
            OffsetDateTime::parse(&row.try_get::<String, _>("requested_at")?, &Rfc3339)?;
        table.add_row([
            row.try_get::<String, _>("api_key_id")?,
            format_local_timestamp(timestamp)?,
            row.try_get::<String, _>("endpoint")?,
            format!("{} ms", row.try_get::<i64, _>("duration_ms")?),
            row.try_get::<i64, _>("http_status")?.to_string(),
        ]);
    }
    println!("{table}");
    Ok(())
}

fn format_local_timestamp(timestamp: OffsetDateTime) -> anyhow::Result<String> {
    let offset = UtcOffset::local_offset_at(timestamp)
        .context("failed to determine the local UTC offset")?;
    timestamp
        .to_offset(offset)
        .format(&Rfc3339)
        .context("failed to format the local timestamp")
}

async fn open_read_only(path: &Path) -> anyhow::Result<SqliteConnection> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5));
    SqliteConnection::connect_with(&options)
        .await
        .with_context(|| format!("failed to open SQLite state {}", path.display()))
}

async fn print_stat(config_path: &Path, args: StatArgs) -> anyhow::Result<()> {
    let now = OffsetDateTime::now_utc();
    let week_start =
        now.date() - TimeDuration::days(i64::from(now.weekday().number_days_from_monday()));
    let start_date = args.start_date.unwrap_or(week_start);
    let end_date = args.end_date.unwrap_or(week_start + TimeDuration::days(6));
    if start_date > end_date {
        return Err(anyhow!(
            "--start-date must be earlier than or equal to --end-date"
        ));
    }
    let range_start = start_date.midnight().assume_utc();
    let range_end = end_date
        .next_day()
        .ok_or_else(|| anyhow!("--end-date is out of range"))?
        .midnight()
        .assume_utc();
    let config = Config::load(config_path)?;
    let mut connection = open_read_only(&config.state.path).await?;
    let rows = sqlx::query(
        "SELECT api_key_id, input_tokens, cached_input_tokens, cost_usd FROM request_logs \
         WHERE julianday(requested_at) >= julianday(?) \
           AND julianday(requested_at) < julianday(?)",
    )
    .bind(range_start.format(&Rfc3339)?)
    .bind(range_end.format(&Rfc3339)?)
    .fetch_all(&mut connection)
    .await
    .context("failed to query usage statistics")?;
    let mut totals_by_key = BTreeMap::<String, (Decimal, i64, i64)>::new();
    for row in rows {
        let api_key_id = row.try_get::<String, _>("api_key_id")?;
        let (spent, input, cached) = totals_by_key.entry(api_key_id).or_default();
        if let Some(cost) = row.try_get::<Option<String>, _>("cost_usd")? {
            let cost = cost
                .parse::<Decimal>()
                .context("request_logs contains an invalid cost_usd value")?;
            *spent = spent
                .checked_add(cost)
                .ok_or_else(|| anyhow!("usage spend is out of range"))?;
        }
        if let Some(input_tokens) = row.try_get::<Option<i64>, _>("input_tokens")? {
            let cached_input_tokens = row
                .try_get::<Option<i64>, _>("cached_input_tokens")?
                .unwrap_or(0);
            *input = input
                .checked_add(input_tokens)
                .ok_or_else(|| anyhow!("input token count is out of range"))?;
            *cached = cached
                .checked_add(cached_input_tokens)
                .ok_or_else(|| anyhow!("cached input token count is out of range"))?;
        }
    }

    println!("Date range {start_date} through {end_date}");
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(["API KEY", "SPENT USD", "CACHE RATE", "STATUS"]);
    let fallback_configured = config.fallback_model.is_some();
    for api_key in &config.api_keys {
        let (spent, input_tokens, cached_input_tokens) =
            totals_by_key.get(&api_key.id).copied().unwrap_or_default();
        let cache_rate = if input_tokens > 0 {
            format!(
                "{:.2}%",
                Decimal::from(cached_input_tokens) * Decimal::ONE_HUNDRED
                    / Decimal::from(input_tokens)
            )
        } else {
            "—".to_owned()
        };
        let status = match api_key.quota {
            None => "unlimited",
            Some(quota) => {
                if spent >= quota.hard_limit_usd
                    || (spent >= quota.weekly_limit_usd && !fallback_configured)
                {
                    "blocked"
                } else if spent >= quota.weekly_limit_usd {
                    "fallback"
                } else {
                    "available"
                }
            }
        };
        table.add_row([
            api_key.id.clone(),
            format!("{spent:.9}"),
            cache_rate,
            status.to_owned(),
        ]);
    }
    println!("{table}");
    Ok(())
}

fn format_k_tokens(tokens: Option<i64>) -> String {
    match tokens {
        Some(tokens) => format!("{}.{:03}", tokens / 1_000, tokens % 1_000),
        None => "—".to_owned(),
    }
}

fn parse_rfc3339(value: &str) -> Result<OffsetDateTime, String> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())?;
    if timestamp.nanosecond() != 0 {
        return Err("timestamps must use whole-second precision".to_owned());
    }
    Ok(timestamp)
}

fn parse_date(value: &str) -> Result<Date, String> {
    let format = format_description::parse_borrowed::<3>("[year]-[month]-[day]")
        .expect("date format description is valid");
    Date::parse(value, &format).map_err(|error| error.to_string())
}
