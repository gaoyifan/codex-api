use std::{collections::HashMap, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::{
    auth::ClientIdentity, config::Config, credentials::CredentialManager, error::ApiError,
    http_api::ResponsesSessions, store::Store, upstream_http::UpstreamHttpClient,
};

pub(crate) struct AppState {
    pub(crate) request_limits: HashMap<String, Arc<Semaphore>>,
    pub(crate) config: Arc<Config>,
    pub(crate) store: Arc<Store>,
    pub(crate) credentials: Arc<CredentialManager>,
    pub(crate) upstream_http: UpstreamHttpClient,
    pub(crate) responses_sessions: ResponsesSessions,
    pub(crate) shutdown: CancellationToken,
    pub(crate) pending_requests: TaskTracker,
    pub(crate) websocket_tasks: TaskTracker,
}

impl AppState {
    pub(crate) async fn acquire_request_slot(
        &self,
        identity: &ClientIdentity,
    ) -> Result<OwnedSemaphorePermit, ApiError> {
        let limit = Arc::clone(&self.request_limits[&identity.id]);
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => Err(ApiError::shutdown()),
            permit = limit.acquire_owned() => Ok(permit.expect("request semaphores are never closed")),
        }
    }
}
