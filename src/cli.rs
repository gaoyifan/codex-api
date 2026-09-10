use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};
use rust_decimal::Decimal;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
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
    /// Show usage statistics for every configured API key.
    Stat(StatArgs),
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

#[derive(Debug, ClapArgs)]
struct StatArgs {
    #[arg(long, value_parser = parse_date)]
    start_date: Option<Date>,
    #[arg(long, value_parser = parse_date)]
    end_date: Option<Date>,
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
        Command::Stat(stat) => print_stat(&args.config, stat).await,
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
        let requested_at = format_local_timestamp(&requested_at)?;
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

fn format_local_timestamp(value: &str) -> anyhow::Result<String> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339)
        .context("request_logs contains an invalid requested_at value")?;
    let offset = UtcOffset::local_offset_at(timestamp)
        .context("failed to determine the local UTC offset")?;
    timestamp
        .to_offset(offset)
        .format(&Rfc3339)
        .context("failed to format the local request time")
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
    let pool = open_read_only(&config.state.path).await?;
    let rows = sqlx::query(
        "SELECT api_key_id, input_tokens, cached_input_tokens, cost_usd FROM request_logs \
         WHERE julianday(requested_at) >= julianday(?) \
           AND julianday(requested_at) < julianday(?)",
    )
    .bind(range_start.format(&Rfc3339)?)
    .bind(range_end.format(&Rfc3339)?)
    .fetch_all(&pool)
    .await
    .context("failed to query usage statistics")?;
    let mut spent_by_key = BTreeMap::<String, Decimal>::new();
    let mut tokens_by_key = BTreeMap::<String, (i64, i64)>::new();
    for row in rows {
        let api_key_id = row.try_get::<String, _>("api_key_id")?;
        if let Some(cost) = row.try_get::<Option<String>, _>("cost_usd")? {
            let cost = cost
                .parse::<Decimal>()
                .context("request_logs contains an invalid cost_usd value")?;
            let spent = spent_by_key.entry(api_key_id.clone()).or_default();
            *spent = spent
                .checked_add(cost)
                .ok_or_else(|| anyhow!("usage spend is out of range"))?;
        }
        if let Some(input_tokens) = row.try_get::<Option<i64>, _>("input_tokens")? {
            let cached_input_tokens = row
                .try_get::<Option<i64>, _>("cached_input_tokens")?
                .unwrap_or(0);
            let tokens = tokens_by_key.entry(api_key_id).or_default();
            tokens.0 = tokens
                .0
                .checked_add(input_tokens)
                .ok_or_else(|| anyhow!("input token count is out of range"))?;
            tokens.1 = tokens
                .1
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
        let spent = spent_by_key
            .get(&api_key.id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let cache_rate = match tokens_by_key.get(&api_key.id) {
            Some((input_tokens, cached_input_tokens)) if *input_tokens > 0 => format!(
                "{:.2}%",
                Decimal::from(*cached_input_tokens) * Decimal::ONE_HUNDRED
                    / Decimal::from(*input_tokens)
            ),
            _ => "—".to_owned(),
        };
        let status = match (api_key.weekly_limit_usd, api_key.hard_limit_usd) {
            (None, _) => "unlimited",
            (Some(soft), Some(hard_limit)) => {
                if spent >= hard_limit || (spent >= soft && !fallback_configured) {
                    "blocked"
                } else if spent >= soft {
                    "fallback"
                } else {
                    "available"
                }
            }
            (Some(_), None) => unreachable!("limited keys always have a hard limit"),
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
