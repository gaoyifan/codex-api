use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::HeaderValue;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::sync::{Mutex, watch};
use tokio_util::task::TaskTracker;
use url::Url;

use crate::Clock;
use crate::store::{Credential, Store};

const CODEX_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const PROACTIVE_REFRESH_WINDOW: Duration = Duration::minutes(5);
const OPAQUE_TOKEN_REFRESH_AGE: Duration = Duration::days(8);

#[derive(Clone, Debug)]
pub(crate) struct CredentialSnapshot {
    pub(crate) access_token: SecretString,
    pub(crate) account_id: String,
    pub(crate) generation: u64,
}

pub(crate) struct CredentialManager {
    store: Arc<Store>,
    http: reqwest::Client,
    oauth_token_url: Url,
    clock: Arc<dyn Clock>,
    state: Mutex<CredentialState>,
    refresh_tasks: TaskTracker,
}

struct CredentialState {
    credential: Credential,
    generation: u64,
    refresh_flight: Option<watch::Receiver<Option<SharedRefreshOutcome>>>,
}

type SharedRefreshOutcome = Result<CredentialSnapshot, Arc<CredentialError>>;

impl CredentialManager {
    pub(crate) async fn load(
        store: Arc<Store>,
        auth_file: &Path,
        oauth_token_url: Url,
        http: reqwest::Client,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, CredentialError> {
        let seed = read_auth_seed(auth_file)?;
        let credential = store.load_or_import_credentials(&seed).await?;

        Ok(Self {
            store,
            http,
            oauth_token_url,
            clock,
            state: Mutex::new(CredentialState {
                credential,
                generation: 0,
                refresh_flight: None,
            }),
            refresh_tasks: TaskTracker::new(),
        })
    }

    /// Returns credentials ready for an upstream request, refreshing first when
    /// the access JWT is within the five-minute window or an opaque token has
    /// not been refreshed for more than eight days.
    pub(crate) async fn credentials(
        self: &Arc<Self>,
    ) -> Result<CredentialSnapshot, CredentialError> {
        self.refresh_generation(None).await
    }

    /// Recovers from a pre-stream upstream 401. If another request has already
    /// refreshed the generation used by the failed request, this returns that
    /// newer generation without exchanging the rotating refresh token again.
    pub(crate) async fn refresh_after_unauthorized(
        self: &Arc<Self>,
        failed_generation: u64,
    ) -> Result<CredentialSnapshot, CredentialError> {
        self.refresh_generation(Some(failed_generation)).await
    }

    pub(crate) async fn finish_refreshes(&self) {
        self.refresh_tasks.close();
        self.refresh_tasks.wait().await;
    }

    async fn refresh_generation(
        self: &Arc<Self>,
        failed_generation: Option<u64>,
    ) -> Result<CredentialSnapshot, CredentialError> {
        let mut outcome = {
            let mut state = self.state.lock().await;
            let ready = match failed_generation {
                Some(generation) => state.generation != generation,
                None => !credential_needs_refresh(&state.credential, self.clock.now()),
            };
            if ready {
                return Ok(snapshot_from_state(&state));
            }
            if let Some(outcome) = &state.refresh_flight {
                outcome.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                state.refresh_flight = Some(receiver.clone());
                let manager = Arc::clone(self);
                self.refresh_tasks.spawn(async move {
                    let outcome = manager.perform_refresh().await.map_err(Arc::new);
                    let retryable_server_error = matches!(
                        &outcome,
                        Err(error)
                            if matches!(
                                error.as_ref(),
                                CredentialError::OauthRejected(500..=599)
                            )
                    );
                    if retryable_server_error {
                        manager.state.lock().await.refresh_flight = None;
                    }
                    sender.send_replace(Some(outcome));
                });
                receiver
            }
        };

        loop {
            if let Some(completed) = outcome.borrow().clone() {
                return completed.map_err(CredentialError::SharedRefresh);
            }
            outcome
                .changed()
                .await
                .map_err(|_| CredentialError::RefreshTaskStopped)?;
        }
    }

    async fn perform_refresh(&self) -> Result<CredentialSnapshot, CredentialError> {
        let refresh_token = self.state.lock().await.credential.refresh_token.clone();

        let response = self
            .http
            .post(self.oauth_token_url.clone())
            .json(&RefreshRequest {
                client_id: CODEX_OAUTH_CLIENT_ID,
                grant_type: "refresh_token",
                refresh_token: &refresh_token,
            })
            .send()
            .await
            .map_err(CredentialError::OauthRequest)?;
        let status = response.status();
        if !status.is_success() {
            return Err(CredentialError::OauthRejected(status.as_u16()));
        }
        let refreshed = response
            .json::<RefreshResponse>()
            .await
            .map_err(CredentialError::OauthResponse)?;

        let mut state = self.state.lock().await;
        let account_id = refreshed
            .id_token
            .as_deref()
            .and_then(account_id_from_id_token)
            .unwrap_or_else(|| state.credential.account_id.clone());
        let access_token = optional_nonempty(refreshed.access_token)?
            .unwrap_or_else(|| state.credential.access_token.clone());
        let refresh_token = optional_nonempty(refreshed.refresh_token)?
            .unwrap_or_else(|| state.credential.refresh_token.clone());
        if !upstream_headers_are_valid(&access_token, &account_id) {
            return Err(CredentialError::InvalidOauthResponse);
        }
        let updated = Credential {
            account_id,
            access_expires_at: access_expiry(&access_token),
            access_token,
            refresh_token,
            last_refresh: self.clock.now(),
        };

        self.store.save_credentials(&updated).await?;
        state.credential = updated;
        state.generation += 1;
        state.refresh_flight = None;
        Ok(snapshot_from_state(&state))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CredentialError {
    #[error("failed to read authentication seed")]
    ReadSeed(#[source] std::io::Error),
    #[error("invalid authentication seed")]
    InvalidSeed(#[source] serde_json::Error),
    #[error("invalid authentication seed")]
    InvalidSeedStructure,
    #[error("failed to load or save credential state")]
    Store(#[from] anyhow::Error),
    #[error("OAuth token refresh request failed")]
    OauthRequest(#[source] reqwest::Error),
    #[error("OAuth token endpoint returned HTTP {0}")]
    OauthRejected(u16),
    #[error("OAuth token endpoint returned an invalid response")]
    OauthResponse(#[source] reqwest::Error),
    #[error("OAuth token endpoint returned an invalid response")]
    InvalidOauthResponse,
    #[error("credential refresh failed")]
    SharedRefresh(#[source] Arc<CredentialError>),
    #[error("credential refresh task stopped before publishing its result")]
    RefreshTaskStopped,
}

#[derive(Deserialize)]
struct AuthFile {
    auth_mode: String,
    tokens: AuthTokens,
    last_refresh: String,
}

#[derive(Deserialize)]
struct AuthTokens {
    id_token: String,
    access_token: String,
    refresh_token: String,
    account_id: Option<String>,
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    client_id: &'static str,
    grant_type: &'static str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
struct RefreshResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct ExpiryClaims {
    exp: i64,
}

#[derive(Deserialize)]
struct AccountClaims {
    #[serde(rename = "https://api.openai.com/auth")]
    auth: Option<AccountClaim>,
}

#[derive(Deserialize)]
struct AccountClaim {
    chatgpt_account_id: Option<String>,
}

fn read_auth_seed(path: &Path) -> Result<Credential, CredentialError> {
    let contents = std::fs::read(path).map_err(CredentialError::ReadSeed)?;
    let auth: AuthFile = serde_json::from_slice(&contents).map_err(CredentialError::InvalidSeed)?;

    if auth.auth_mode != "chatgpt"
        || auth.tokens.access_token.trim().is_empty()
        || auth.tokens.refresh_token.trim().is_empty()
        || auth.tokens.id_token.trim().is_empty()
    {
        return Err(CredentialError::InvalidSeedStructure);
    }
    let account_id = auth
        .tokens
        .account_id
        .filter(|account_id| !account_id.trim().is_empty())
        .or_else(|| account_id_from_id_token(&auth.tokens.id_token))
        .ok_or(CredentialError::InvalidSeedStructure)?;
    if !upstream_headers_are_valid(&auth.tokens.access_token, &account_id) {
        return Err(CredentialError::InvalidSeedStructure);
    }
    let last_refresh = OffsetDateTime::parse(&auth.last_refresh, &Rfc3339)
        .map_err(|_| CredentialError::InvalidSeedStructure)?;

    Ok(Credential {
        account_id,
        access_expires_at: access_expiry(&auth.tokens.access_token),
        access_token: auth.tokens.access_token,
        refresh_token: auth.tokens.refresh_token,
        last_refresh,
    })
}

fn snapshot_from_state(state: &CredentialState) -> CredentialSnapshot {
    CredentialSnapshot {
        access_token: SecretString::from(state.credential.access_token.clone()),
        account_id: state.credential.account_id.clone(),
        generation: state.generation,
    }
}

fn credential_needs_refresh(credential: &Credential, now: OffsetDateTime) -> bool {
    match credential.access_expires_at {
        Some(expires_at) => expires_at <= now + PROACTIVE_REFRESH_WINDOW,
        None => credential.last_refresh < now - OPAQUE_TOKEN_REFRESH_AGE,
    }
}

fn access_expiry(access_token: &str) -> Option<OffsetDateTime> {
    let claims: ExpiryClaims = decode_jwt_payload(access_token)?;
    OffsetDateTime::from_unix_timestamp(claims.exp).ok()
}

fn account_id_from_id_token(id_token: &str) -> Option<String> {
    let claims: AccountClaims = decode_jwt_payload(id_token)?;
    claims
        .auth?
        .chatgpt_account_id
        .filter(|account_id| !account_id.trim().is_empty())
}

fn decode_jwt_payload<T: for<'de> Deserialize<'de>>(token: &str) -> Option<T> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn optional_nonempty(value: Option<String>) -> Result<Option<String>, CredentialError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(CredentialError::InvalidOauthResponse),
        value => Ok(value),
    }
}

fn upstream_headers_are_valid(access_token: &str, account_id: &str) -> bool {
    HeaderValue::from_str(&format!("Bearer {access_token}")).is_ok()
        && HeaderValue::from_str(account_id).is_ok()
}
