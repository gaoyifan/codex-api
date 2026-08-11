use std::sync::Arc;

use axum::http::StatusCode;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::{
    error::ApiError,
    store::{BillableUsage, FinalStatus, RequestId, Store},
};

pub(super) struct PendingRequest {
    store: Arc<Store>,
    state: State,
}

enum State {
    Open(OpenRequest),
    Finalized,
}

struct OpenRequest {
    request_id: RequestId,
    tracker: TaskTrackerToken,
    drop_status: FinalStatus,
    drop_http_status: Option<u16>,
}

impl PendingRequest {
    pub(super) fn new(store: Arc<Store>, request_id: RequestId, tracker: TaskTrackerToken) -> Self {
        Self {
            store,
            state: State::Open(OpenRequest {
                request_id,
                tracker,
                drop_status: FinalStatus::Canceled,
                drop_http_status: None,
            }),
        }
    }

    pub(super) fn response_started(&mut self, status: StatusCode) {
        self.open_mut().drop_http_status = Some(status.as_u16());
    }

    pub(super) fn upstream_error_started(&mut self, status: StatusCode) {
        let open = self.open_mut();
        open.drop_status = FinalStatus::UpstreamError;
        open.drop_http_status = Some(status.as_u16());
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

    fn open_mut(&mut self) -> &mut OpenRequest {
        match &mut self.state {
            State::Open(open) => open,
            State::Finalized => panic!("a finalized request is no longer open"),
        }
    }

    fn take_open(&mut self) -> OpenRequest {
        match std::mem::replace(&mut self.state, State::Finalized) {
            State::Open(open) => open,
            State::Finalized => panic!("a pending request can only be finalized once"),
        }
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        let open = match std::mem::replace(&mut self.state, State::Finalized) {
            State::Open(open) => open,
            State::Finalized => return,
        };
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            let _tracker = open.tracker;
            let _ = store
                .finalize_request(
                    open.request_id,
                    open.drop_status,
                    open.drop_http_status,
                    None,
                )
                .await;
        });
    }
}
