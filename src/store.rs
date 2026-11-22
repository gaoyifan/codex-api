use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration as StdDuration;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use anyhow::{Context, ensure};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqliteConnection, SqlitePool};
use time::{Duration, OffsetDateTime, UtcOffset};

use crate::Clock;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub(crate) struct Credential {
    pub(crate) account_id: String,
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) access_expires_at: Option<OffsetDateTime>,
    pub(crate) last_refresh: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiProtocol {
    Responses,
    ChatCompletions,
}

impl ApiProtocol {
    fn as_str(self) -> &'static str {
        match self {
            Self::Responses => "responses",
            Self::ChatCompletions => "chat_completions",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Transport {
    HttpSse,
    WebSocket,
}

impl Transport {
    fn as_str(self) -> &'static str {
        match self {
            Self::HttpSse => "http_sse",
            Self::WebSocket => "websocket",
        }
    }
}

pub(crate) struct RequestContext {
    pub(crate) api_key_id: String,
    pub(crate) api_protocol: ApiProtocol,
    pub(crate) transport: Transport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestId(pub(crate) i64);

pub(crate) enum Admission {
    Admitted {
        request_id: RequestId,
        effective_model: EffectiveModel,
    },
    WeeklyQuotaExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ModelAccess {
    All,
    FallbackOnly,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FinalStatus {
    Completed,
    Incomplete,
    Rejected,
    UpstreamError,
    Canceled,
    InternalError,
}

impl FinalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Incomplete => "incomplete",
            Self::Rejected => "rejected",
            Self::UpstreamError => "upstream_error",
            Self::Canceled => "canceled",
            Self::InternalError => "internal_error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QuotaLimits {
    pub(crate) weekly_limit_usd: Decimal,
    pub(crate) hard_limit_usd: Decimal,
}

#[derive(Clone, Copy)]
pub(crate) struct ModelRates {
    pub(crate) input_usd_per_million: Decimal,
    pub(crate) cached_input_usd_per_million: Decimal,
    pub(crate) output_usd_per_million: Decimal,
}

pub(crate) struct EffectiveModel {
    pub(crate) model: String,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) rates: ModelRates,
}

pub(crate) struct ModelCandidates {
    pub(crate) primary: EffectiveModel,
    pub(crate) fallback: Option<EffectiveModel>,
}

#[derive(Clone, Copy)]
pub(crate) struct BillableUsage {
    pub(crate) input_tokens: u64,
    pub(crate) cached_input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) rates: ModelRates,
}

pub(crate) struct Store {
    pool: SqlitePool,
    clock: Arc<dyn Clock>,
}

impl Store {
    pub(crate) async fn open(
        path: &Path,
        file_mode: u32,
        clock: Arc<dyn Clock>,
    ) -> anyhow::Result<Self> {
        prepare_state_file(path, file_mode)?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(StdDuration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool, clock })
    }

    pub(crate) async fn load_or_import_credentials(
        &self,
        seed: &Credential,
    ) -> anyhow::Result<Credential> {
        let mut transaction = self.pool.begin().await?;
        let stored = sqlx::query(
            "SELECT account_id, access_token, refresh_token, access_expires_at_ns, \
                    last_refresh_ns FROM credentials WHERE singleton = 1",
        )
        .fetch_optional(&mut *transaction)
        .await?;

        let credential = match stored {
            None => {
                insert_credentials(&mut transaction, seed).await?;
                seed.clone()
            }
            Some(row) => {
                let current = credential_from_row(&row)?;
                if seed.last_refresh > current.last_refresh {
                    update_credentials(&mut transaction, seed).await?;
                    seed.clone()
                } else {
                    current
                }
            }
        };
        transaction.commit().await?;
        Ok(credential)
    }

    pub(crate) async fn save_credentials(&self, credential: &Credential) -> anyhow::Result<()> {
        let mut connection = self.pool.acquire().await?;
        update_credentials(&mut connection, credential).await
    }

    pub(crate) async fn begin_request(
        &self,
        context: &RequestContext,
        quota: Option<QuotaLimits>,
        candidates: ModelCandidates,
    ) -> anyhow::Result<Admission> {
        let now = self.clock.now().to_offset(UtcOffset::UTC);
        let requested_at_ms = timestamp_ms(now);
        let spent = self
            .current_week_spend_nano_usd(&context.api_key_id, now)
            .await?;
        let access = quota_access(spent, quota, candidates.fallback.is_some())?;
        let rejected = access == ModelAccess::Blocked;
        let (status, finished_at_ms, duration_ms, http_status) = if rejected {
            (
                "rejected",
                Some(requested_at_ms),
                Some(0_i64),
                match context.transport {
                    Transport::HttpSse => Some(429_i64),
                    Transport::WebSocket => None,
                },
            )
        } else {
            ("started", None, None, None)
        };
        let effective_model = if access == ModelAccess::FallbackOnly {
            candidates
                .fallback
                .expect("fallback admission requires a fallback candidate")
        } else {
            candidates.primary
        };
        let result = sqlx::query(
            "INSERT INTO request_ledger (requested_at_ms, finished_at_ms, api_key_id, model, \
                    reasoning_effort, api_protocol, transport, duration_ms, status, http_status) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(requested_at_ms)
        .bind(finished_at_ms)
        .bind(&context.api_key_id)
        .bind(&effective_model.model)
        .bind(&effective_model.reasoning_effort)
        .bind(context.api_protocol.as_str())
        .bind(context.transport.as_str())
        .bind(duration_ms)
        .bind(status)
        .bind(http_status)
        .execute(&self.pool)
        .await?;
        let request_id = RequestId(result.last_insert_rowid());
        Ok(if rejected {
            Admission::WeeklyQuotaExceeded
        } else {
            Admission::Admitted {
                request_id,
                effective_model,
            }
        })
    }

    pub(crate) async fn model_access(
        &self,
        api_key_id: &str,
        quota: Option<QuotaLimits>,
        has_fallback: bool,
    ) -> anyhow::Result<ModelAccess> {
        if quota.is_none() {
            return Ok(ModelAccess::All);
        }

        let spent = self
            .current_week_spend_nano_usd(api_key_id, self.clock.now())
            .await?;
        quota_access(spent, quota, has_fallback)
    }

    async fn current_week_spend_nano_usd(
        &self,
        api_key_id: &str,
        now: OffsetDateTime,
    ) -> anyhow::Result<Decimal> {
        let week_start_ms = timestamp_ms(monday_start(now));
        let next_week_ms = week_start_ms + 604_800_000;
        let spent_nano_usd: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(cost_nano_usd), 0) FROM request_ledger \
             WHERE api_key_id = ? AND cost_nano_usd IS NOT NULL \
               AND requested_at_ms >= ? AND requested_at_ms < ?",
        )
        .bind(api_key_id)
        .bind(week_start_ms)
        .bind(next_week_ms)
        .fetch_one(&self.pool)
        .await?;
        Ok(Decimal::from(spent_nano_usd))
    }

    pub(crate) async fn record_rejection(
        &self,
        context: &RequestContext,
        model: &str,
        reasoning_effort: Option<&str>,
        http_status: Option<u16>,
    ) -> anyhow::Result<()> {
        let requested_at_ms = timestamp_ms(self.clock.now().to_offset(UtcOffset::UTC));
        sqlx::query(
            "INSERT INTO request_ledger (requested_at_ms, finished_at_ms, api_key_id, model, \
                    reasoning_effort, api_protocol, transport, duration_ms, status, http_status) \
             VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'rejected', ?)",
        )
        .bind(requested_at_ms)
        .bind(requested_at_ms)
        .bind(&context.api_key_id)
        .bind(model)
        .bind(reasoning_effort)
        .bind(context.api_protocol.as_str())
        .bind(context.transport.as_str())
        .bind(http_status.map(i64::from))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn record_audio_request(
        &self,
        api_key_id: &str,
        endpoint: &str,
        duration: StdDuration,
        http_status: u16,
    ) -> anyhow::Result<()> {
        let duration_ms =
            i64::try_from(duration.as_millis()).context("audio duration is out of range")?;
        sqlx::query(
            "INSERT INTO audio_request_logs \
             (requested_at_ms, api_key_id, endpoint, duration_ms, http_status) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(timestamp_ms(self.clock.now()))
        .bind(api_key_id)
        .bind(endpoint)
        .bind(duration_ms)
        .bind(i64::from(http_status))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn finalize_request(
        &self,
        request_id: RequestId,
        status: FinalStatus,
        http_status: Option<u16>,
        usage: Option<BillableUsage>,
    ) -> anyhow::Result<()> {
        let finished_at_ms = timestamp_ms(self.clock.now().to_offset(UtcOffset::UTC));

        let (input_tokens, cached_input_tokens, output_tokens, cost_nano_usd) = match usage {
            None => (None, None, None, None),
            Some(usage) => (
                Some(token_count(usage.input_tokens)?),
                Some(token_count(usage.cached_input_tokens)?),
                Some(token_count(usage.output_tokens)?),
                Some(calculate_cost_nano_usd(usage)?),
            ),
        };
        // CAST preserves i64 saturation when SQLite promotes an overflowing duration to REAL.
        let result = sqlx::query(
            "UPDATE request_ledger SET finished_at_ms = ?, input_tokens = ?, \
                    cached_input_tokens = ?, output_tokens = ?, cost_nano_usd = ?, \
                    duration_ms = CAST(MAX(0, ? - requested_at_ms) AS INTEGER), \
                    status = ?, http_status = ? \
             WHERE id = ? AND status = 'started'",
        )
        .bind(finished_at_ms)
        .bind(input_tokens)
        .bind(cached_input_tokens)
        .bind(output_tokens)
        .bind(cost_nano_usd)
        .bind(finished_at_ms)
        .bind(status.as_str())
        .bind(http_status.map(i64::from))
        .bind(request_id.0)
        .execute(&self.pool)
        .await?;
        ensure!(
            result.rows_affected() == 1,
            "request {request_id:?} is missing or is no longer started"
        );
        Ok(())
    }
}

async fn insert_credentials(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    seed: &Credential,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO credentials (singleton, account_id, access_token, refresh_token, \
                access_expires_at_ns, last_refresh_ns) VALUES (1, ?, ?, ?, ?, ?)",
    )
    .bind(&seed.account_id)
    .bind(&seed.access_token)
    .bind(&seed.refresh_token)
    .bind(optional_timestamp_ns(seed.access_expires_at)?)
    .bind(timestamp_ns(seed.last_refresh)?)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_credentials(
    connection: &mut SqliteConnection,
    credential: &Credential,
) -> anyhow::Result<()> {
    let result = sqlx::query(
        "UPDATE credentials SET account_id = ?, access_token = ?, refresh_token = ?, \
                access_expires_at_ns = ?, last_refresh_ns = ? WHERE singleton = 1",
    )
    .bind(&credential.account_id)
    .bind(&credential.access_token)
    .bind(&credential.refresh_token)
    .bind(optional_timestamp_ns(credential.access_expires_at)?)
    .bind(timestamp_ns(credential.last_refresh)?)
    .execute(connection)
    .await?;
    ensure!(
        result.rows_affected() == 1,
        "credential state has not been initialized"
    );
    Ok(())
}

fn credential_from_row(row: &SqliteRow) -> anyhow::Result<Credential> {
    Ok(Credential {
        account_id: row.try_get("account_id")?,
        access_token: row.try_get("access_token")?,
        refresh_token: row.try_get("refresh_token")?,
        access_expires_at: row
            .try_get::<Option<i64>, _>("access_expires_at_ns")?
            .map(offset_from_ns),
        last_refresh: offset_from_ns(row.try_get("last_refresh_ns")?),
    })
}

fn prepare_state_file(path: &Path, file_mode: u32) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(file_mode);
    #[cfg(not(unix))]
    let _ = file_mode;
    match options.open(path) {
        Ok(file) => {
            drop(file);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !std::fs::symlink_metadata(path)?.file_type().is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "SQLite state path is not a regular file",
                )
                .into());
            }
            #[cfg(unix)]
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(file_mode))?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn quota_access(
    spent_nano_usd: Decimal,
    quota: Option<QuotaLimits>,
    has_fallback: bool,
) -> anyhow::Result<ModelAccess> {
    let Some(quota) = quota else {
        return Ok(ModelAccess::All);
    };
    let nano_usd = Decimal::from(1_000_000_000_u64);
    let hard = quota
        .hard_limit_usd
        .checked_mul(nano_usd)
        .context("calculated request cost is outside SQLite's supported range")?;
    let soft = quota
        .weekly_limit_usd
        .checked_mul(nano_usd)
        .context("calculated request cost is outside SQLite's supported range")?;
    if spent_nano_usd >= hard || (spent_nano_usd >= soft && !has_fallback) {
        Ok(ModelAccess::Blocked)
    } else if spent_nano_usd >= soft {
        Ok(ModelAccess::FallbackOnly)
    } else {
        Ok(ModelAccess::All)
    }
}

fn monday_start(now: OffsetDateTime) -> OffsetDateTime {
    let utc = now.to_offset(UtcOffset::UTC);
    let days_since_monday = i64::from(utc.weekday().number_days_from_monday());
    (utc.date() - Duration::days(days_since_monday))
        .midnight()
        .assume_utc()
}

fn calculate_cost_nano_usd(usage: BillableUsage) -> anyhow::Result<i64> {
    let uncached = Decimal::from(usage.input_tokens - usage.cached_input_tokens);
    let cached = Decimal::from(usage.cached_input_tokens);
    let output = Decimal::from(usage.output_tokens);
    let weighted = uncached
        .checked_mul(usage.rates.input_usd_per_million)
        .and_then(|value| {
            cached
                .checked_mul(usage.rates.cached_input_usd_per_million)
                .and_then(|cached_cost| value.checked_add(cached_cost))
        })
        .and_then(|value| {
            output
                .checked_mul(usage.rates.output_usd_per_million)
                .and_then(|output_cost| value.checked_add(output_cost))
        })
        .and_then(|value| value.checked_mul(Decimal::from(1_000_u64)))
        .context("calculated request cost is outside SQLite's supported range")?;
    weighted
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .context("calculated request cost is outside SQLite's supported range")
}

fn token_count(value: u64) -> anyhow::Result<i64> {
    i64::try_from(value).context("token usage is outside SQLite's supported range")
}

fn optional_timestamp_ns(value: Option<OffsetDateTime>) -> anyhow::Result<Option<i64>> {
    value.map(timestamp_ns).transpose()
}

fn timestamp_ns(value: OffsetDateTime) -> anyhow::Result<i64> {
    i64::try_from(value.unix_timestamp_nanos())
        .context("timestamp is outside SQLite's supported range")
}

fn timestamp_ms(value: OffsetDateTime) -> i64 {
    value.unix_timestamp() * 1_000 + i64::from(value.millisecond())
}

fn offset_from_ns(value: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::nanoseconds(value)
}
