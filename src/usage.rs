use serde::{Deserialize, Serialize};

/// Account quota snapshots returned by the ChatGPT usage endpoint.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AccountUsage {
    pub(crate) plan_type: String,
    pub(crate) rate_limit: Option<RateLimit>,
    pub(crate) credits: Option<Credits>,
    pub(crate) additional_rate_limits: Option<Vec<AdditionalRateLimit>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RateLimit {
    pub(crate) allowed: bool,
    pub(crate) limit_reached: bool,
    pub(crate) primary_window: Option<UsageWindow>,
    pub(crate) secondary_window: Option<UsageWindow>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UsageWindow {
    pub(crate) used_percent: f64,
    pub(crate) limit_window_seconds: i64,
    pub(crate) reset_at: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Credits {
    pub(crate) has_credits: bool,
    pub(crate) unlimited: bool,
    pub(crate) balance: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AdditionalRateLimit {
    pub(crate) limit_name: String,
    pub(crate) rate_limit: Option<RateLimit>,
}
