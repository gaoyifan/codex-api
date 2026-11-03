use std::sync::Arc;

use axum::http::StatusCode;
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::{
    error::ApiError,
    store::{BillableUsage, FinalStatus, RequestId, Store},
};

pub(super) struct PendingRequest {
    store: Arc<Store>,
    open: Option<OpenRequest>,
}

struct OpenRequest {
    permit: OwnedSemaphorePermit,
    request_id: RequestId,
    tracker: TaskTrackerToken,
    drop_http_status: Option<u16>,
}

impl PendingRequest {
    pub(super) fn new(
        store: Arc<Store>,
        request_id: RequestId,
        tracker: TaskTrackerToken,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            store,
            open: Some(OpenRequest {
                permit,
                request_id,
                tracker,
                drop_http_status: None,
            }),
        }
    }

    pub(super) fn response_started(&mut self, status: StatusCode) {
        self.open
            .as_mut()
            .expect("a finalized request is no longer open")
            .drop_http_status = Some(status.as_u16());
    }

    pub(super) async fn finish(
        &mut self,
        status: FinalStatus,
        http_status: Option<StatusCode>,
        usage: Option<BillableUsage>,
    ) -> Result<(), ApiError> {
        let open = self.take_open();
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            let _permit = open.permit;
            let _tracker = open.tracker;
            store
                .finalize_request(
                    open.request_id,
                    status,
                    http_status.map(|status| status.as_u16()),
                    usage,
                )
                .await
        })
        .await
        .expect("request finalization task must not panic")
        .map_err(|_| ApiError::internal())
    }

    pub(super) async fn finish_terminal(
        &mut self,
        status: FinalStatus,
        http_status: StatusCode,
        fallback_http_status: StatusCode,
        usage: Option<BillableUsage>,
    ) -> Result<(), ()> {
        let open = self.take_open();
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            let _permit = open.permit;
            let _tracker = open.tracker;
            let result = store
                .finalize_request(open.request_id, status, Some(http_status.as_u16()), usage)
                .await;
            if result.is_ok() {
                return Ok(());
            }
            let _ = store
                .finalize_request(
                    open.request_id,
                    FinalStatus::UpstreamError,
                    Some(fallback_http_status.as_u16()),
                    None,
                )
                .await;
            Err(())
        })
        .await
        .expect("terminal finalization task must not panic")
    }

    fn take_open(&mut self) -> OpenRequest {
        self.open
            .take()
            .expect("a pending request can only be finalized once")
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        let Some(open) = self.open.take() else { return };
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            let _permit = open.permit;
            let _tracker = open.tracker;
            let _ = store
                .finalize_request(
                    open.request_id,
                    FinalStatus::Canceled,
                    open.drop_http_status,
                    None,
                )
                .await;
        });
    }
}
