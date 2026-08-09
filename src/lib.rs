use std::path::Path;
use std::sync::Arc;

/// Time boundary used by quota windows, token refresh, and request timing.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> time::OffsetDateTime;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> time::OffsetDateTime {
        time::OffsetDateTime::now_utc()
    }
}

/// Starts the relay from a TOML configuration file.
///
/// The behavior is intentionally absent while the tests-first contract suite is
/// being written.
pub async fn run(_config_path: &Path) -> anyhow::Result<()> {
    run_with_clock(_config_path, Arc::new(SystemClock)).await
}

/// Starts the relay with an injected system-time boundary while preserving the
/// real network and persistence seams used by integration tests.
pub async fn run_with_clock(_config_path: &Path, _clock: Arc<dyn Clock>) -> anyhow::Result<()> {
    anyhow::bail!("codex-api is not implemented yet")
}
