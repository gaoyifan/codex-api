use axum::http::{HeaderMap, header::AUTHORIZATION};
use secrecy::ExposeSecret;
use subtle::ConstantTimeEq;

use crate::{
    config::Config,
    error::ApiError,
    store::QuotaLimits,
};

#[derive(Clone, Debug)]
pub(crate) struct ClientIdentity {
    pub(crate) id: String,
    pub(crate) quota: QuotaLimits,
}

pub(crate) fn authenticate(
    headers: &HeaderMap,
    config: &Config,
) -> Result<ClientIdentity, ApiError> {
    let mut authorization_values = headers.get_all(AUTHORIZATION).iter();
    let authorization = authorization_values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ApiError::invalid_api_key)?;
    if authorization_values.next().is_some() {
        return Err(ApiError::invalid_api_key());
    }
    let provided = authorization
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.bytes().any(|byte| byte.is_ascii_whitespace()))
        .ok_or_else(ApiError::invalid_api_key)?;

    let matched = config.api_keys.iter().find(|candidate| {
        bool::from(
            provided
                .as_bytes()
                .ct_eq(candidate.secret.expose_secret().as_bytes()),
        )
    });
    matched
        .map(|candidate| ClientIdentity {
            id: candidate.id.clone(),
            quota: QuotaLimits {
                weekly_limit_usd: candidate.weekly_limit_usd,
                hard_limit_usd: candidate.hard_limit_usd,
            },
        })
        .ok_or_else(ApiError::invalid_api_key)
}
