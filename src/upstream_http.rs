use std::sync::Arc;

use http::HeaderMap;
use http::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use reqwest::{Response, StatusCode, header::HeaderValue};
use secrecy::ExposeSecret;
use serde_json::Value;
use url::Url;

use crate::credentials::{CredentialError, CredentialManager, CredentialSnapshot};
use crate::upstream_headers::{
    CODEX_ORIGINATOR, CODEX_USER_AGENT, CODEX_VERSION, codex_passthrough_headers,
};

pub(crate) struct UpstreamHttpClient {
    client: reqwest::Client,
    responses_url: Url,
    credentials: Arc<CredentialManager>,
}

impl UpstreamHttpClient {
    pub(crate) fn new(
        client: reqwest::Client,
        base_url: &Url,
        credentials: Arc<CredentialManager>,
    ) -> Self {
        let mut responses_url = base_url.clone();
        let base_path = base_url.path().trim_end_matches('/');
        responses_url.set_path(&format!("{base_path}/responses"));
        responses_url.set_query(None);
        responses_url.set_fragment(None);
        Self {
            client,
            responses_url,
            credentials,
        }
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
}
