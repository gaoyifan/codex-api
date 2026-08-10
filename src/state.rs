use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::{
    config::Config, credentials::CredentialManager, store::Store, upstream_http::UpstreamHttpClient,
};

pub(crate) struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) store: Arc<Store>,
    pub(crate) credentials: Arc<CredentialManager>,
    pub(crate) upstream_http: UpstreamHttpClient,
    pub(crate) shutdown: CancellationToken,
    pub(crate) pending_requests: TaskTracker,
    pub(crate) websocket_tasks: TaskTracker,
}
