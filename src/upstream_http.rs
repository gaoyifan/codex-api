use std::sync::Arc;

use anyhow::{Context, ensure};
use http::HeaderMap;
use http::header::{ACCEPT, USER_AGENT};
use reqwest::{Response, StatusCode};
use secrecy::ExposeSecret;
use serde_json::Value;
use time::{Duration, OffsetDateTime};
use tokio::sync::Mutex;
use url::Url;

use crate::upstream_headers::{
    CODEX_ORIGINATOR, CODEX_USER_AGENT, CODEX_VERSION, codex_request_headers,
};
use crate::{
    Clock,
    credentials::{CredentialManager, CredentialSnapshot},
};

pub(crate) struct UpstreamHttpClient {
    client: reqwest::Client,
    responses_url: Url,
    images_url: Url,
    models_url: Url,
    usage_url: Url,
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
        let mut images_url = responses_url.clone();
        images_url.set_path(&format!("{base_path}/images"));
        let mut models_url = responses_url.clone();
        models_url.set_path(&format!("{base_path}/models"));
        models_url
            .query_pairs_mut()
            .append_pair("client_version", CODEX_VERSION);
        let mut usage_url = responses_url.clone();
        let backend_path = base_path.strip_suffix("/codex").unwrap_or(base_path);
        usage_url.set_path(&format!("{backend_path}/wham/usage"));
        Self {
            client,
            responses_url,
            images_url,
            models_url,
            usage_url,
            credentials,
            clock,
            models_cache: Mutex::new(None),
        }
    }

    pub(crate) async fn models(&self) -> anyhow::Result<Vec<Value>> {
        let mut cache = self.models_cache.lock().await;
        let now = self.clock.now();
        if let Some(cached) = cache
            .as_ref()
            .filter(|cached| now - cached.fetched_at < MODELS_CACHE_TTL)
        {
            return Ok(cached.models.clone());
        }

        let payload: Value = self
            .send_authenticated(&self.models_url, "application/json", None)
            .await?
            .error_for_status()?
            .json()
            .await?;
        let models = payload
            .get("models")
            .and_then(Value::as_array)
            .filter(|models| {
                models.iter().all(|model| {
                    model
                        .get("slug")
                        .and_then(Value::as_str)
                        .is_some_and(|slug| !slug.is_empty())
                })
            })
            .cloned()
            .context("upstream models response had an invalid shape")?;
        *cache = Some(CachedModels {
            fetched_at: now,
            models: models.clone(),
        });
        Ok(models)
    }

    pub(crate) async fn usage(&self) -> anyhow::Result<crate::usage::AccountUsage> {
        self.send_authenticated(&self.usage_url, "application/json", None)
            .await?
            .error_for_status()?
            .json()
            .await
            .context("upstream usage request failed")
    }

    pub(crate) async fn model(&self, slug: &str) -> anyhow::Result<Option<Value>> {
        Ok(self
            .models()
            .await?
            .into_iter()
            .find(|model| model.get("slug").and_then(Value::as_str) == Some(slug)))
    }

    pub(crate) async fn send(
        &self,
        body: &Value,
        downstream_headers: &HeaderMap,
    ) -> anyhow::Result<Response> {
        self.send_authenticated(
            &self.responses_url,
            "text/event-stream",
            Some((body, downstream_headers)),
        )
        .await
    }

    pub(crate) async fn images(
        &self,
        operation: &str,
        body: &Value,
        downstream_headers: &HeaderMap,
    ) -> anyhow::Result<Response> {
        let mut url = self.images_url.clone();
        url.set_path(&format!("{}/{operation}", self.images_url.path()));
        self.send_authenticated(&url, "application/json", Some((body, downstream_headers)))
            .await
    }

    async fn send_authenticated(
        &self,
        url: &Url,
        accept: &str,
        payload: Option<(&Value, &HeaderMap)>,
    ) -> anyhow::Result<Response> {
        let send_once = |credential: &CredentialSnapshot| {
            let request = if payload.is_some() {
                self.client.post(url.clone())
            } else {
                self.client.get(url.clone())
            }
            .bearer_auth(credential.access_token.expose_secret())
            .header("ChatGPT-Account-ID", &credential.account_id)
            .header("originator", CODEX_ORIGINATOR)
            .header("version", CODEX_VERSION)
            .header(USER_AGENT, CODEX_USER_AGENT)
            .header(ACCEPT, accept);
            let request = match payload {
                Some((body, headers)) => request
                    .headers(codex_request_headers(headers, body))
                    .json(body),
                None => request,
            };
            request.send()
        };
        let credential = self.credentials.credentials().await?;
        let response = send_once(&credential).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        let credential = self
            .credentials
            .refresh_after_unauthorized(credential.generation)
            .await?;
        let response = send_once(&credential).await?;
        ensure!(
            response.status() != StatusCode::UNAUTHORIZED,
            "upstream authentication was rejected"
        );
        Ok(response)
    }
}
