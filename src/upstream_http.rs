use std::sync::Arc;

use http::HeaderMap;
use http::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use reqwest::{Response, StatusCode, header::HeaderValue};
use secrecy::ExposeSecret;
use serde_json::Value;
use time::{Duration, OffsetDateTime};
use tokio::sync::Mutex;
use url::Url;

use crate::upstream_headers::{
    CODEX_ORIGINATOR, CODEX_USER_AGENT, CODEX_VERSION, codex_passthrough_headers,
};
use crate::{
    Clock,
    credentials::{CredentialError, CredentialManager, CredentialSnapshot},
};

pub(crate) struct UpstreamHttpClient {
    client: reqwest::Client,
    responses_url: Url,
    models_url: Url,
    credentials: Arc<CredentialManager>,
    clock: Arc<dyn Clock>,
    models_cache: Mutex<Option<CachedModels>>,
}

struct CachedModels {
    fetched_at: OffsetDateTime,
    models: Vec<Value>,
}

const MODELS_CACHE_TTL: Duration = Duration::hours(1);

impl UpstreamHttpClient {
    pub(crate) fn new(
        client: reqwest::Client,
        base_url: &Url,
        credentials: Arc<CredentialManager>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let mut responses_url = base_url.clone();
        let base_path = base_url.path().trim_end_matches('/');
        responses_url.set_path(&format!("{base_path}/responses"));
        responses_url.set_query(None);
        responses_url.set_fragment(None);
        let mut models_url = base_url.clone();
        models_url.set_path(&format!("{base_path}/models"));
        models_url.set_query(None);
        models_url.set_fragment(None);
        models_url
            .query_pairs_mut()
            .append_pair("client_version", CODEX_VERSION);
        Self {
            client,
            responses_url,
            models_url,
            credentials,
            clock,
            models_cache: Mutex::new(None),
        }
    }

    pub(crate) async fn models(&self) -> Result<Vec<Value>, UpstreamHttpError> {
        let mut cache = self.models_cache.lock().await;
        let now = self.clock.now();
        if let Some(cached) = cache
            .as_ref()
            .filter(|cached| now - cached.fetched_at < MODELS_CACHE_TTL)
        {
            return Ok(cached.models.clone());
        }

        let credential = self
            .credentials
            .credentials()
            .await
            .map_err(UpstreamHttpError::Credentials)?;
        let mut response = self.send_models_once(&credential).await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            let credential = self
                .credentials
                .refresh_after_unauthorized(credential.generation)
                .await
                .map_err(UpstreamHttpError::CredentialRefresh)?;
            response = self.send_models_once(&credential).await?;
            if response.status() == StatusCode::UNAUTHORIZED {
                return Err(UpstreamHttpError::AuthenticationRejected);
            }
        }
        let payload: Value = response
            .error_for_status()
            .map_err(UpstreamHttpError::Transport)?
            .json()
            .await
            .map_err(UpstreamHttpError::Transport)?;
        let models = payload
            .get("models")
            .and_then(Value::as_array)
            .filter(|models| {
                models.iter().all(|model| {
                    model.as_object().is_some_and(|object| {
                        object
                            .get("slug")
                            .and_then(Value::as_str)
                            .is_some_and(|slug| !slug.is_empty())
                    })
                })
            })
            .cloned()
            .ok_or(UpstreamHttpError::InvalidModelsPayload)?;
        *cache = Some(CachedModels {
            fetched_at: now,
            models: models.clone(),
        });
        Ok(models)
    }

    pub(crate) async fn send(
        &self,
        body: &Value,
        downstream_headers: &HeaderMap,
    ) -> Result<Response, UpstreamHttpError> {
        let credential = self
            .credentials
            .credentials()
            .await
            .map_err(UpstreamHttpError::Credentials)?;
        let response = self
            .send_once(body, downstream_headers, &credential)
            .await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        let credential = self
            .credentials
            .refresh_after_unauthorized(credential.generation)
            .await
            .map_err(UpstreamHttpError::CredentialRefresh)?;
        let response = self
            .send_once(body, downstream_headers, &credential)
            .await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            Err(UpstreamHttpError::AuthenticationRejected)
        } else {
            Ok(response)
        }
    }

    async fn send_once(
        &self,
        body: &Value,
        downstream_headers: &HeaderMap,
        credential: &CredentialSnapshot,
    ) -> Result<Response, UpstreamHttpError> {
        let mut authorization = HeaderValue::from_str(&format!(
            "Bearer {}",
            credential.access_token.expose_secret()
        ))
        .map_err(|_| UpstreamHttpError::InvalidCredentialHeader)?;
        authorization.set_sensitive(true);
        self.client
            .post(self.responses_url.clone())
            .header(AUTHORIZATION, authorization)
            .header("ChatGPT-Account-ID", &credential.account_id)
            .header("originator", CODEX_ORIGINATOR)
            .header("version", CODEX_VERSION)
            .header(USER_AGENT, CODEX_USER_AGENT)
            .header(ACCEPT, "text/event-stream")
            .headers(codex_passthrough_headers(downstream_headers))
            .json(body)
            .send()
            .await
            .map_err(UpstreamHttpError::Transport)
    }

    async fn send_models_once(
        &self,
        credential: &CredentialSnapshot,
    ) -> Result<Response, UpstreamHttpError> {
        let mut authorization = HeaderValue::from_str(&format!(
            "Bearer {}",
            credential.access_token.expose_secret()
        ))
        .map_err(|_| UpstreamHttpError::InvalidCredentialHeader)?;
        authorization.set_sensitive(true);
        self.client
            .get(self.models_url.clone())
            .header(AUTHORIZATION, authorization)
            .header("ChatGPT-Account-ID", &credential.account_id)
            .header("originator", CODEX_ORIGINATOR)
            .header("version", CODEX_VERSION)
            .header(USER_AGENT, CODEX_USER_AGENT)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(UpstreamHttpError::Transport)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpstreamHttpError {
    #[error("failed to obtain upstream credentials")]
    Credentials(#[source] CredentialError),
    #[error("failed to refresh upstream credentials")]
    CredentialRefresh(#[source] CredentialError),
    #[error("upstream authentication was rejected")]
    AuthenticationRejected,
    #[error("upstream credential cannot be represented as an HTTP header")]
    InvalidCredentialHeader,
    #[error("upstream HTTP request failed")]
    Transport(#[source] reqwest::Error),
    #[error("upstream models response had an invalid shape")]
    InvalidModelsPayload,
}
