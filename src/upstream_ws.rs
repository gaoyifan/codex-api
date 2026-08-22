use std::sync::Arc;

use http::header::{AUTHORIZATION, USER_AGENT};
use http::{HeaderMap, StatusCode};
use http::{HeaderName, HeaderValue};
use secrecy::ExposeSecret;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as TungsteniteError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use crate::credentials::{CredentialError, CredentialManager, CredentialSnapshot};
use crate::upstream_headers::{
    CODEX_ORIGINATOR, CODEX_USER_AGENT, CODEX_VERSION, codex_passthrough_headers,
};

const RESPONSES_PATH: &str = "responses";
const OPENAI_BETA_HEADER: HeaderName = HeaderName::from_static("openai-beta");
const OPENAI_BETA_VALUE: &str = "responses_websockets=2026-02-06";
const CHATGPT_ACCOUNT_ID_HEADER: HeaderName = HeaderName::from_static("chatgpt-account-id");
const ORIGINATOR_HEADER: HeaderName = HeaderName::from_static("originator");
const VERSION_HEADER: HeaderName = HeaderName::from_static("version");
pub(crate) type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Error)]
pub(crate) enum UpstreamWebSocketError {
    #[error("failed to obtain upstream credentials")]
    Credentials(#[source] CredentialError),

    #[error("failed to refresh upstream credentials after an authentication rejection")]
    CredentialRefresh(#[source] CredentialError),

    #[error("upstream WebSocket authentication was rejected")]
    AuthenticationRejected,

    #[error("upstream WebSocket handshake failed with HTTP status {status}")]
    Handshake { status: StatusCode },

    #[error("failed to build upstream WebSocket request")]
    Request,

    #[error("failed to connect to upstream WebSocket")]
    Transport(#[source] TungsteniteError),
}

/// Opens one authenticated ChatGPT Codex Responses WebSocket connection.
///
/// An HTTP 401 during the initial WebSocket handshake triggers one serialized
/// credential refresh and exactly one retry. Other failures are returned
/// immediately and never cause a transport fallback.
pub(crate) async fn connect_upstream_websocket(
    base_url: &Url,
    credential_manager: Arc<CredentialManager>,
    downstream_headers: &HeaderMap,
) -> Result<UpstreamWebSocket, UpstreamWebSocketError> {
    let credentials = credential_manager
        .credentials()
        .await
        .map_err(UpstreamWebSocketError::Credentials)?;

    match connect_once(base_url, downstream_headers, &credentials).await {
        Err(UpstreamWebSocketError::AuthenticationRejected) => {
            let refreshed = credential_manager
                .refresh_after_unauthorized(credentials.generation)
                .await
                .map_err(UpstreamWebSocketError::CredentialRefresh)?;
            connect_once(base_url, downstream_headers, &refreshed).await
        }
        result => result,
    }
}

async fn connect_once(
    base_url: &Url,
    downstream_headers: &HeaderMap,
    credentials: &CredentialSnapshot,
) -> Result<UpstreamWebSocket, UpstreamWebSocketError> {
    let websocket_url = responses_websocket_url(base_url);
    let mut request = websocket_url
        .as_str()
        .into_client_request()
        .map_err(|_| UpstreamWebSocketError::Request)?;

    let mut authorization = HeaderValue::from_str(&format!(
        "Bearer {}",
        credentials.access_token.expose_secret()
    ))
    .map_err(|_| UpstreamWebSocketError::Request)?;
    authorization.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, authorization);
    request.headers_mut().insert(
        CHATGPT_ACCOUNT_ID_HEADER,
        HeaderValue::from_str(&credentials.account_id)
            .map_err(|_| UpstreamWebSocketError::Request)?,
    );
    request.headers_mut().insert(
        OPENAI_BETA_HEADER,
        HeaderValue::from_static(OPENAI_BETA_VALUE),
    );
    request.headers_mut().insert(
        ORIGINATOR_HEADER,
        HeaderValue::from_static(CODEX_ORIGINATOR),
    );
    request
        .headers_mut()
        .insert(VERSION_HEADER, HeaderValue::from_static(CODEX_VERSION));
    request
        .headers_mut()
        .insert(USER_AGENT, HeaderValue::from_static(CODEX_USER_AGENT));
    request
        .headers_mut()
        .extend(codex_passthrough_headers(downstream_headers));

    connect_async(request)
        .await
        .map(|(stream, _response)| stream)
        .map_err(classify_connect_error)
}

fn responses_websocket_url(base_url: &Url) -> Url {
    let mut url = base_url.clone();
    let websocket_scheme = if base_url.scheme() == "http" {
        "ws"
    } else {
        "wss"
    };
    url.set_scheme(websocket_scheme)
        .expect("validated HTTP(S) upstream URL accepts a WebSocket scheme");

    let base_path = base_url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/{RESPONSES_PATH}"));
    url.set_query(None);
    url.set_fragment(None);
    url
}

fn classify_connect_error(error: TungsteniteError) -> UpstreamWebSocketError {
    match error {
        TungsteniteError::Http(response) if response.status() == StatusCode::UNAUTHORIZED => {
            UpstreamWebSocketError::AuthenticationRejected
        }
        TungsteniteError::Http(response) => UpstreamWebSocketError::Handshake {
            status: response.status(),
        },
        error => UpstreamWebSocketError::Transport(error),
    }
}
