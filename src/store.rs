use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration as StdDuration;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
use time::{Duration, OffsetDateTime, UtcOffset};

use crate::Clock;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub(crate) struct CredentialSeed {
    pub(crate) account_id: String,
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) access_expires_at: Option<OffsetDateTime>,
    pub(crate) last_refresh: OffsetDateTime,
}

#[derive(Clone)]
pub(crate) struct Credential {
    pub(crate) account_id: String,
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) access_expires_at: Option<OffsetDateTime>,
    pub(crate) last_refresh: OffsetDateTime,
}

impl From<&CredentialSeed> for Credential {
    fn from(seed: &CredentialSeed) -> Self {
        Self {
            account_id: seed.account_id.clone(),
            access_token: seed.access_token.clone(),
            refresh_token: seed.refresh_token.clone(),
            access_expires_at: seed.access_expires_at,
            last_refresh: seed.last_refresh,
        }
    }
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

pub(crate) struct RequestMetadata {
    pub(crate) api_key_id: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) api_protocol: ApiProtocol,
    pub(crate) transport: Transport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestId(pub(crate) i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    Admitted(RequestId),
    WeeklyQuotaExceeded(RequestId),
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

#[derive(Clone, Copy)]
pub(crate) struct ModelRates {
    pub(crate) input_usd_per_million: Decimal,
    pub(crate) cached_input_usd_per_million: Decimal,
    pub(crate) output_usd_per_million: Decimal,
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
    pub(crate) async fn open(path: &Path, clock: Arc<dyn Clock>) -> Result<Self, StoreError> {
        create_private_file(path)?;
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
        seed: &CredentialSeed,
    ) -> Result<Credential, StoreError> {
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
                Credential::from(seed)
            }
            Some(row) => {
                let current = credential_from_row(&row)?;
                if seed.last_refresh > current.last_refresh {
                    update_credentials(&mut transaction, seed).await?;
                    Credential::from(seed)
                } else {
                    current
                }
            }
        };
        transaction.commit().await?;
        Ok(credential)
    }

    pub(crate) async fn save_credentials(&self, credential: &Credential) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE credentials SET account_id = ?, access_token = ?, refresh_token = ?, \
                    access_expires_at_ns = ?, last_refresh_ns = ? WHERE singleton = 1",
        )
        .bind(&credential.account_id)
        .bind(&credential.access_token)
        .bind(&credential.refresh_token)
        .bind(optional_timestamp_ns(credential.access_expires_at)?)
        .bind(timestamp_ns(credential.last_refresh)?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::CredentialsNotInitialized);
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn begin_request(
        &self,
        metadata: &RequestMetadata,
        weekly_limit_usd: Option<Decimal>,
    ) -> Result<Admission, StoreError> {
        let now = self.clock.now().to_offset(UtcOffset::UTC);
        let requested_at_ms = timestamp_ms(now)?;
        let week_start_ms = timestamp_ms(monday_start(now))?;
        let next_week_ms = week_start_ms
            .checked_add(604_800_000)
            .ok_or(StoreError::TimestampOutOfRange)?;

        let spent_nano_usd: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(cost_nano_usd), 0) FROM request_ledger \
             WHERE api_key_id = ? AND cost_nano_usd IS NOT NULL \
               AND requested_at_ms >= ? AND requested_at_ms < ?",
        )
        .bind(&metadata.api_key_id)
        .bind(week_start_ms)
        .bind(next_week_ms)
        .fetch_one(&self.pool)
        .await?;

        let quota_exceeded = match weekly_limit_usd {
            None => false,
            Some(limit) => {
                let limit_nano_usd = limit
                    .checked_mul(Decimal::from(1_000_000_000_u64))
                    .ok_or(StoreError::CostOutOfRange)?;
                Decimal::from(spent_nano_usd) >= limit_nano_usd
            }
        };
        let (status, finished_at_ms, duration_ms, http_status) = if quota_exceeded {
            (
                "rejected",
                Some(requested_at_ms),
                Some(0_i64),
                match metadata.transport {
                    Transport::HttpSse => Some(429_i64),
                    Transport::WebSocket => None,
                },
            )
        } else {
            ("started", None, None, None)
        };
        let result = sqlx::query(
            "INSERT INTO request_ledger (requested_at_ms, finished_at_ms, api_key_id, model, \
                    reasoning_effort, api_protocol, transport, duration_ms, status, http_status) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(requested_at_ms)
        .bind(finished_at_ms)
        .bind(&metadata.api_key_id)
        .bind(&metadata.model)
        .bind(&metadata.reasoning_effort)
        .bind(metadata.api_protocol.as_str())
        .bind(metadata.transport.as_str())
        .bind(duration_ms)
        .bind(status)
        .bind(http_status)
        .execute(&self.pool)
        .await?;
        let request_id = RequestId(result.last_insert_rowid());
        Ok(if quota_exceeded {
            Admission::WeeklyQuotaExceeded(request_id)
        } else {
            Admission::Admitted(request_id)
        })
    }

    pub(crate) async fn finalize_request(
        &self,
        request_id: RequestId,
        status: FinalStatus,
        http_status: Option<u16>,
        usage: Option<BillableUsage>,
    ) -> Result<(), StoreError> {
        let requested_at_ms: i64 = sqlx::query_scalar(
            "SELECT requested_at_ms FROM request_ledger WHERE id = ? AND status = 'started'",
        )
        .bind(request_id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::RequestNotStarted(request_id))?;
        let finished_at_ms = timestamp_ms(self.clock.now().to_offset(UtcOffset::UTC))?;
        let duration_ms = finished_at_ms.saturating_sub(requested_at_ms).max(0);

        let (input_tokens, cached_input_tokens, output_tokens, cost_nano_usd) = match usage {
            None => (None, None, None, None),
            Some(usage) => (
                Some(token_count(usage.input_tokens)?),
                Some(token_count(usage.cached_input_tokens)?),
                Some(token_count(usage.output_tokens)?),
                Some(calculate_cost_nano_usd(usage)?),
            ),
        };
        let result = sqlx::query(
            "UPDATE request_ledger SET finished_at_ms = ?, input_tokens = ?, \
                    cached_input_tokens = ?, output_tokens = ?, cost_nano_usd = ?, \
                    duration_ms = ?, status = ?, http_status = ? \
             WHERE id = ? AND status = 'started'",
        )
        .bind(finished_at_ms)
        .bind(input_tokens)
        .bind(cached_input_tokens)
        .bind(output_tokens)
        .bind(cost_nano_usd)
        .bind(duration_ms)
        .bind(status.as_str())
        .bind(http_status.map(i64::from))
        .bind(request_id.0)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::RequestNotStarted(request_id));
        }
        Ok(())
    }
}

async fn insert_credentials(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    seed: &CredentialSeed,
) -> Result<(), StoreError> {
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
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    seed: &CredentialSeed,
) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE credentials SET account_id = ?, access_token = ?, refresh_token = ?, \
                access_expires_at_ns = ?, last_refresh_ns = ? WHERE singleton = 1",
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

fn credential_from_row(row: &SqliteRow) -> Result<Credential, StoreError> {
    Ok(Credential {
        account_id: row.try_get("account_id")?,
        access_token: row.try_get("access_token")?,
        refresh_token: row.try_get("refresh_token")?,
        access_expires_at: row
            .try_get::<Option<i64>, _>("access_expires_at_ns")?
            .map(offset_from_ns)
            .transpose()?,
        last_refresh: offset_from_ns(row.try_get("last_refresh_ns")?)?,
    })
}

fn create_private_file(path: &Path) -> Result<(), StoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
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
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn monday_start(now: OffsetDateTime) -> OffsetDateTime {
    let utc = now.to_offset(UtcOffset::UTC);
    let days_since_monday = i64::from(utc.weekday().number_days_from_monday());
    (utc.date() - Duration::days(days_since_monday))
        .midnight()
        .assume_utc()
}

fn calculate_cost_nano_usd(usage: BillableUsage) -> Result<i64, StoreError> {
    if usage.cached_input_tokens > usage.input_tokens {
        return Err(StoreError::InvalidUsage);
    }
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
        .ok_or(StoreError::CostOutOfRange)?;
    weighted
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .ok_or(StoreError::CostOutOfRange)
}

fn token_count(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::UsageOutOfRange)
}

fn optional_timestamp_ns(value: Option<OffsetDateTime>) -> Result<Option<i64>, StoreError> {
    value.map(timestamp_ns).transpose()
}

fn timestamp_ns(value: OffsetDateTime) -> Result<i64, StoreError> {
    i64::try_from(value.unix_timestamp_nanos()).map_err(|_| StoreError::TimestampOutOfRange)
}

fn timestamp_ms(value: OffsetDateTime) -> Result<i64, StoreError> {
    i64::try_from(value.unix_timestamp_nanos().div_euclid(1_000_000))
        .map_err(|_| StoreError::TimestampOutOfRange)
}

fn offset_from_ns(value: i64) -> Result<OffsetDateTime, StoreError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(value))
        .map_err(|_| StoreError::TimestampOutOfRange)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("failed to access SQLite state file")]
    Io(#[from] std::io::Error),
    #[error("SQLite state operation failed")]
    Database(#[from] sqlx::Error),
    #[error("SQLite state migration failed")]
    Migration(#[from] MigrateError),
    #[error("credential state has not been initialized")]
    CredentialsNotInitialized,
    #[error("request {0:?} is missing or is no longer started")]
    RequestNotStarted(RequestId),
    #[error("token usage is invalid")]
    InvalidUsage,
    #[error("token usage is outside SQLite's supported range")]
    UsageOutOfRange,
    #[error("calculated request cost is outside SQLite's supported range")]
    CostOutOfRange,
    #[error("timestamp is outside SQLite's supported range")]
    TimestampOutOfRange,
}
