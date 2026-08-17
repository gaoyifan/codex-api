use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};
use rust_decimal::Decimal;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

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
    /// Show current-week quota usage for every configured API key.
    Quota,
}

#[derive(Debug, ClapArgs)]
struct LogsArgs {
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..))]
    limit: u32,
    #[arg(long)]
    api_key_id: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    status: Option<LogStatus>,
    #[arg(long, value_parser = parse_rfc3339)]
    since: Option<OffsetDateTime>,
    #[arg(long, value_parser = parse_rfc3339)]
    until: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "snake_case")]
enum LogStatus {
    Started,
    Completed,
    Incomplete,
    Rejected,
    UpstreamError,
    Canceled,
    InternalError,
}

impl LogStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Incomplete => "incomplete",
            Self::Rejected => "rejected",
            Self::UpstreamError => "upstream_error",
            Self::Canceled => "canceled",
            Self::InternalError => "internal_error",
        }
    }
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Serve => crate::run(&args.config).await,
        Command::Logs(logs) => print_logs(&args.config, logs).await,
        Command::Quota => print_quota(&args.config).await,
    }
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
    let pool = open_read_only(&config.state.path).await?;
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
        query.push(" AND status = ").push_bind(status.as_str());
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
        .fetch_all(&pool)
        .await
        .context("failed to query request logs")?;

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            "API KEY",
            "TIME UTC",
            "MODEL (REASONING)",
            "PROTOCOL",
            "TOKENS I/C/O (K)",
            "COST USD",
            "DURATION",
            "STATUS",
        ]);
    for row in rows {
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
            row.try_get::<String, _>("requested_at")?,
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

async fn open_read_only(path: &Path) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5));
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .with_context(|| format!("failed to open SQLite state {}", path.display()))
}

async fn print_quota(config_path: &Path) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    let pool = open_read_only(&config.state.path).await?;
    let now = OffsetDateTime::now_utc();
    let week_start = (now.date()
        - TimeDuration::days(i64::from(now.weekday().number_days_from_monday())))
    .midnight()
    .assume_utc();
    let next_week = week_start + TimeDuration::days(7);
    let rows = sqlx::query(
        "SELECT api_key_id, cost_usd FROM request_logs \
         WHERE cost_usd IS NOT NULL \
           AND julianday(requested_at) >= julianday(?) \
           AND julianday(requested_at) < julianday(?)",
    )
    .bind(week_start.format(&Rfc3339)?)
    .bind(next_week.format(&Rfc3339)?)
    .fetch_all(&pool)
    .await
    .context("failed to query current-week quota usage")?;
    let mut spent_by_key = BTreeMap::<String, Decimal>::new();
    for row in rows {
        let api_key_id = row.try_get::<String, _>("api_key_id")?;
        let cost = row
            .try_get::<String, _>("cost_usd")?
            .parse::<Decimal>()
            .context("request_logs contains an invalid cost_usd value")?;
        let spent = spent_by_key.entry(api_key_id).or_default();
        *spent = spent
            .checked_add(cost)
            .ok_or_else(|| anyhow!("current-week quota spend is out of range"))?;
    }

    println!("Week starting {}", week_start.format(&Rfc3339)?);
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            "API KEY",
            "SPENT USD",
            "LIMIT USD",
            "HARD USD",
            "REMAINING USD",
            "STATUS",
        ]);
    let fallback_configured = config.fallback_model.is_some();
    for api_key in &config.api_keys {
        let spent = spent_by_key
            .get(&api_key.id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let (limit, hard, remaining, status) = match (api_key.weekly_limit_usd, api_key.hard_limit_usd)
        {
            (None, _) => (
                "—".to_owned(),
                "—".to_owned(),
                "—".to_owned(),
                "unlimited",
            ),
            (Some(soft), Some(hard_limit)) => {
                let remaining = if spent >= hard_limit {
                    Decimal::ZERO
                } else {
                    hard_limit - spent
                };
                let status = if spent >= hard_limit
                    || (spent >= soft && !fallback_configured)
                {
                    "blocked"
                } else if spent >= soft {
                    "fallback"
                } else {
                    "available"
                };
                (
                    soft.normalize().to_string(),
                    hard_limit.normalize().to_string(),
                    remaining.normalize().to_string(),
                    status,
                )
            }
            (Some(_), None) => unreachable!("limited keys always have a hard limit"),
        };
        table.add_row([
            api_key.id.clone(),
            format!("{spent:.9}"),
            limit,
            hard,
            remaining,
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
