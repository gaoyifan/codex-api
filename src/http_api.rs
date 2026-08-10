use std::{convert::Infallible, sync::Arc};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use futures_util::stream;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::{
    auth::{ClientIdentity, authenticate},
    chat::{self, ChatErrorKind, TerminalStatus},
    error::ApiError,
    responses_terminal::{TerminalKind, Usage},
    sse::{SseRead, SseReader},
    state::AppState,
    store::{
        Admission, ApiProtocol, BillableUsage, FinalStatus, ModelRates, RequestId, RequestMetadata,
        Store, Transport,
    },
};

struct PendingRequest {
    store: Arc<Store>,
    shutdown: CancellationToken,
    request_id: Option<RequestId>,
    drop_status: FinalStatus,
    drop_http_status: Option<u16>,
    pending_request: Option<TaskTrackerToken>,
}

impl PendingRequest {
    fn new(
        store: Arc<Store>,
        shutdown: CancellationToken,
        request_id: RequestId,
        pending_request: TaskTrackerToken,
    ) -> Self {
        Self {
            store,
            shutdown,
            request_id: Some(request_id),
            drop_status: FinalStatus::Canceled,
            drop_http_status: None,
            pending_request: Some(pending_request),
        }
    }

    fn response_started(&mut self, status: StatusCode) {
        self.drop_http_status = Some(status.as_u16());
    }

    async fn finish(
        &mut self,
        status: FinalStatus,
        http_status: Option<StatusCode>,
        usage: Option<BillableUsage>,
    ) -> Result<(), ApiError> {
        let request_id = self
            .request_id
            .take()
            .expect("a pending request can only be finalized once");
        let pending_request = self
            .pending_request
            .take()
            .expect("a pending request has a tracker token");
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            let _pending_request = pending_request;
            store
                .finalize_request(
                    request_id,
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

    async fn finish_terminal(
        &mut self,
        status: FinalStatus,
        http_status: StatusCode,
        fallback_http_status: StatusCode,
        usage: Option<BillableUsage>,
    ) -> bool {
        let request_id = self
            .request_id
            .take()
            .expect("a pending request can only be finalized once");
        let pending_request = self
            .pending_request
            .take()
            .expect("a pending request has a tracker token");
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            let _pending_request = pending_request;
            let result = store
                .finalize_request(request_id, status, Some(http_status.as_u16()), usage)
                .await;
            if result.is_ok() {
                return true;
            }
            let _ = store
                .finalize_request(
                    request_id,
                    FinalStatus::UpstreamError,
                    Some(fallback_http_status.as_u16()),
                    None,
                )
                .await;
            false
        })
        .await
        .expect("terminal finalization task must not panic")
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        let Some(request_id) = self.request_id.take() else {
            return;
        };
        let store = Arc::clone(&self.store);
        let status = self.drop_status;
        let http_status = self.drop_http_status;
        let pending_request = self
            .pending_request
            .take()
            .expect("a pending request has a tracker token");
        tokio::spawn(async move {
            let _pending_request = pending_request;
            let _ = store
                .finalize_request(request_id, status, http_status, None)
                .await;
        });
    }
}

pub(crate) async fn responses(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = authenticate(&headers, &state.config)?;
    let parsed = match serde_json::from_slice::<Value>(&body) {
        Ok(parsed) => parsed,
        Err(_) => {
            reject_request(&state, &identity, "", None, ApiProtocol::Responses).await?;
            return Err(ApiError::invalid(
                "request",
                "request body must be valid JSON",
            ));
        }
    };

    let model = requested_string(&parsed, "model").unwrap_or_default();
    let reasoning_effort = parsed
        .pointer("/reasoning/effort")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let rates = match model_rates(&state, &model) {
        Some(rates) => rates,
        None => {
            reject_request(
                &state,
                &identity,
                &model,
                reasoning_effort,
                ApiProtocol::Responses,
            )
            .await?;
            return Err(ApiError::invalid("model", "model is not configured"));
        }
    };

    let upstream_body = match normalize_responses_request(parsed) {
        Ok(body) => body,
        Err(error) => {
            reject_request(
                &state,
                &identity,
                &model,
                reasoning_effort,
                ApiProtocol::Responses,
            )
            .await?;
            return Err(error);
        }
    };
    let request_id = admit_request(
        &state,
        &identity,
        model,
        reasoning_effort,
        ApiProtocol::Responses,
        Transport::HttpSse,
    )
    .await?;
    let mut request = PendingRequest::new(
        Arc::clone(&state.store),
        state.shutdown.clone(),
        request_id,
        state.pending_requests.token(),
    );

    let upstream_result = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => {
            request.finish(FinalStatus::Canceled, None, None).await?;
            return Err(ApiError::shutdown());
        }
        result = state.upstream_http.send(&upstream_body) => result,
    };
    let upstream = match upstream_result {
        Ok(response) => response,
        Err(_) => {
            request
                .finish(
                    FinalStatus::UpstreamError,
                    Some(StatusCode::BAD_GATEWAY),
                    None,
                )
                .await?;
            return Err(ApiError::gateway("Upstream request failed"));
        }
    };
    if !upstream.status().is_success() {
        return upstream_error_response(&mut request, state.shutdown.clone(), upstream).await;
    }

    request.response_started(StatusCode::OK);
    let (sender, receiver) = mpsc::channel::<Bytes>(16);
    state
        .pending_requests
        .spawn(forward_responses_stream(request, rates, upstream, sender));
    let body_stream = stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|bytes| (Ok::<Bytes, Infallible>(bytes), receiver))
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(body_stream))
        .expect("static streaming response is valid"))
}

pub(crate) async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = authenticate(&headers, &state.config)?;
    let parsed = match serde_json::from_slice::<Value>(&body) {
        Ok(parsed) => parsed,
        Err(_) => {
            reject_request(&state, &identity, "", None, ApiProtocol::ChatCompletions).await?;
            return Err(ApiError::invalid(
                "request",
                "request body must be valid JSON",
            ));
        }
    };
    let tentative_model = requested_string(&parsed, "model").unwrap_or_default();
    let tentative_reasoning_effort = parsed
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if model_rates(&state, &tentative_model).is_none() {
        reject_request(
            &state,
            &identity,
            &tentative_model,
            tentative_reasoning_effort.clone(),
            ApiProtocol::ChatCompletions,
        )
        .await?;
        return Err(ApiError::invalid("model", "model is not configured"));
    }
    let converted = match chat::convert_request(parsed) {
        Ok(converted) => converted,
        Err(error) => {
            reject_request(
                &state,
                &identity,
                &tentative_model,
                tentative_reasoning_effort,
                ApiProtocol::ChatCompletions,
            )
            .await?;
            return Err(ApiError::invalid(
                error.param.unwrap_or_else(|| "request".to_owned()),
                error.message,
            ));
        }
    };
    let rates = model_rates(&state, &converted.model).expect("model was admitted above");
    let request_id = admit_request(
        &state,
        &identity,
        converted.model,
        converted.reasoning_effort,
        ApiProtocol::ChatCompletions,
        Transport::HttpSse,
    )
    .await?;
    let mut request = PendingRequest::new(
        Arc::clone(&state.store),
        state.shutdown.clone(),
        request_id,
        state.pending_requests.token(),
    );

    let upstream_result = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => {
            request.finish(FinalStatus::Canceled, None, None).await?;
            return Err(ApiError::shutdown());
        }
        result = state.upstream_http.send(&converted.upstream_request) => result,
    };
    let upstream = match upstream_result {
        Ok(response) => response,
        Err(_) => {
            request
                .finish(
                    FinalStatus::UpstreamError,
                    Some(StatusCode::BAD_GATEWAY),
                    None,
                )
                .await?;
            return Err(ApiError::gateway("Upstream request failed"));
        }
    };
    if !upstream.status().is_success() {
        request
            .finish(
                FinalStatus::UpstreamError,
                Some(StatusCode::BAD_GATEWAY),
                None,
            )
            .await?;
        return Err(ApiError::gateway("Upstream request failed"));
    }

    let mut reader = SseReader::new(upstream.bytes_stream());
    let mut completed_output_items = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => {
                request.finish(FinalStatus::Canceled, None, None).await?;
                return Err(ApiError::shutdown());
            }
            next = reader.next() => next,
        };
        match next {
            Ok(SseRead::Event(event)) => {
                if event.terminal().is_none() {
                    if let Ok(value) = serde_json::from_str::<Value>(&event.data)
                        && value.get("type").and_then(Value::as_str)
                            == Some("response.output_item.done")
                        && let Some(item) = value.get("item")
                    {
                        completed_output_items.push(item.clone());
                    }
                    continue;
                }

                let mut terminal = event.into_terminal().expect("checked above");
                let terminal_usage = terminal.usage;
                let terminal_output_is_empty = matches!(
                    terminal.payload.pointer("/response/output"),
                    Some(Value::Array(output)) if output.is_empty()
                );
                if terminal_output_is_empty && !completed_output_items.is_empty() {
                    terminal.payload["response"]["output"] =
                        Value::Array(std::mem::take(&mut completed_output_items));
                }
                match chat::convert_terminal_event(&terminal) {
                    Ok(converted_terminal) => {
                        let status = match converted_terminal.status {
                            TerminalStatus::Completed => FinalStatus::Completed,
                            TerminalStatus::Incomplete => FinalStatus::Incomplete,
                        };
                        if !request
                            .finish_terminal(
                                status,
                                StatusCode::OK,
                                StatusCode::BAD_GATEWAY,
                                Some(billable_usage(converted_terminal.usage, rates)),
                            )
                            .await
                        {
                            return Err(ApiError::gateway(
                                "Upstream terminal response could not be accounted",
                            ));
                        }
                        return Ok(Json(converted_terminal.chat_completion).into_response());
                    }
                    Err(error) if error.kind == ChatErrorKind::UpstreamProtocol => {
                        return fail_chat_terminal(
                            &mut request,
                            rates,
                            terminal_usage,
                            "Upstream terminal response could not be converted",
                        )
                        .await;
                    }
                    Err(_) => unreachable!("terminal conversion cannot be a request error"),
                }
            }
            Ok(SseRead::Eof) | Err(_) => {
                request
                    .finish(
                        FinalStatus::UpstreamError,
                        Some(StatusCode::BAD_GATEWAY),
                        None,
                    )
                    .await?;
                return Err(ApiError::gateway(
                    "Upstream response ended without a terminal",
                ));
            }
        }
    }
}

async fn forward_responses_stream(
    mut request: PendingRequest,
    rates: ModelRates,
    upstream: reqwest::Response,
    sender: mpsc::Sender<Bytes>,
) {
    let mut reader = SseReader::new(upstream.bytes_stream());
    let shutdown = request.shutdown.clone();
    loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                let _ = request
                    .finish(FinalStatus::Canceled, Some(StatusCode::OK), None)
                    .await;
                return;
            }
            _ = sender.closed() => {
                let _ = request
                    .finish(FinalStatus::Canceled, Some(StatusCode::OK), None)
                    .await;
                return;
            }
            next = reader.next() => next,
        };
        match next {
            Ok(SseRead::Event(event)) => {
                if let Some(terminal) = event.terminal() {
                    let status = match terminal.kind {
                        TerminalKind::Completed => FinalStatus::Completed,
                        TerminalKind::Incomplete => FinalStatus::Incomplete,
                        TerminalKind::Failed | TerminalKind::Error => FinalStatus::UpstreamError,
                    };
                    let finalized = request
                        .finish_terminal(
                            status,
                            StatusCode::OK,
                            StatusCode::OK,
                            terminal.usage.map(|usage| billable_usage(usage, rates)),
                        )
                        .await;
                    if !finalized {
                        return;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {}
                        _ = sender.send(event.canonical_bytes()) => {}
                    }
                    return;
                }
                let sent = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => false,
                    result = sender.send(event.canonical_bytes()) => result.is_ok(),
                };
                if !sent {
                    let _ = request
                        .finish(FinalStatus::Canceled, Some(StatusCode::OK), None)
                        .await;
                    return;
                }
            }
            Ok(SseRead::Eof) | Err(_) => {
                let _ = request
                    .finish(FinalStatus::UpstreamError, Some(StatusCode::OK), None)
                    .await;
                return;
            }
        }
    }
}

fn normalize_responses_request(value: Value) -> Result<Value, ApiError> {
    let mut object = match value {
        Value::Object(object) => object,
        _ => {
            return Err(ApiError::invalid(
                "request",
                "request must be a JSON object",
            ));
        }
    };
    match object.get("stream") {
        Some(Value::Bool(true)) => {}
        _ => {
            return Err(ApiError::invalid(
                "stream",
                "stream must be explicitly true",
            ));
        }
    }
    match object.get("store") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => {}
        Some(Value::Bool(true)) => {
            return Err(ApiError::invalid(
                "store",
                "stored responses are not supported",
            ));
        }
        Some(_) => {
            return Err(ApiError::invalid(
                "store",
                "store must be a boolean or null",
            ));
        }
    }
    match object.get("background") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => {}
        Some(Value::Bool(true)) => {
            return Err(ApiError::invalid(
                "background",
                "background responses are not supported",
            ));
        }
        Some(_) => {
            return Err(ApiError::invalid(
                "background",
                "background must be a boolean or null",
            ));
        }
    }
    if object.contains_key("max_output_tokens") {
        return Err(ApiError::invalid(
            "max_output_tokens",
            "max_output_tokens is not supported by the ChatGPT Codex upstream",
        ));
    }
    if let Some(Value::String(input)) = object.get("input") {
        object.insert(
            "input".to_owned(),
            serde_json::json!([{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": input}]
            }]),
        );
    }
    object.insert("store".to_owned(), Value::Bool(false));
    Ok(Value::Object(object))
}

fn requested_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn model_rates(state: &AppState, model: &str) -> Option<ModelRates> {
    state
        .config
        .model_prices
        .get(model)
        .map(|price| ModelRates {
            input_usd_per_million: price.input_usd_per_million,
            cached_input_usd_per_million: price.cached_input_usd_per_million,
            output_usd_per_million: price.output_usd_per_million,
        })
}

async fn admit_request(
    state: &AppState,
    identity: &ClientIdentity,
    model: String,
    reasoning_effort: Option<String>,
    api_protocol: ApiProtocol,
    transport: Transport,
) -> Result<RequestId, ApiError> {
    let metadata = RequestMetadata {
        api_key_id: identity.id.clone(),
        model,
        reasoning_effort,
        api_protocol,
        transport,
    };
    match state
        .store
        .begin_request(&metadata, identity.weekly_limit_usd)
        .await
        .map_err(|_| ApiError::internal())?
    {
        Admission::Admitted(request_id) => Ok(request_id),
        Admission::WeeklyQuotaExceeded(_) => Err(ApiError::quota_exceeded()),
    }
}

async fn reject_request(
    state: &AppState,
    identity: &ClientIdentity,
    model: &str,
    reasoning_effort: Option<String>,
    api_protocol: ApiProtocol,
) -> Result<(), ApiError> {
    let metadata = RequestMetadata {
        api_key_id: identity.id.clone(),
        model: model.to_owned(),
        reasoning_effort,
        api_protocol,
        transport: Transport::HttpSse,
    };
    let request_id = match state
        .store
        .begin_request(&metadata, None)
        .await
        .map_err(|_| ApiError::internal())?
    {
        Admission::Admitted(request_id) => request_id,
        Admission::WeeklyQuotaExceeded(_) => unreachable!("an unlimited ledger entry was rejected"),
    };
    state
        .store
        .finalize_request(
            request_id,
            FinalStatus::Rejected,
            Some(StatusCode::BAD_REQUEST.as_u16()),
            None,
        )
        .await
        .map_err(|_| ApiError::internal())
}

async fn upstream_error_response(
    request: &mut PendingRequest,
    shutdown: CancellationToken,
    upstream: reqwest::Response,
) -> Result<Response, ApiError> {
    let status = upstream.status();
    request.drop_status = FinalStatus::UpstreamError;
    request.drop_http_status = Some(status.as_u16());
    let content_type = upstream.headers().get(CONTENT_TYPE).cloned();
    let body_result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            request
                .finish(FinalStatus::UpstreamError, Some(status), None)
                .await?;
            return Err(ApiError::shutdown());
        }
        result = upstream.bytes() => result,
    };
    let body = match body_result {
        Ok(body) => body,
        Err(_) => {
            request
                .finish(
                    FinalStatus::UpstreamError,
                    Some(StatusCode::BAD_GATEWAY),
                    None,
                )
                .await?;
            return Err(ApiError::gateway("Upstream error body failed"));
        }
    };
    request
        .finish(FinalStatus::UpstreamError, Some(status), None)
        .await?;
    let mut response = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response = response.header(CONTENT_TYPE, content_type);
    }
    response
        .body(Body::from(body))
        .map_err(|_| ApiError::internal())
}

async fn fail_chat_terminal(
    request: &mut PendingRequest,
    rates: ModelRates,
    usage: Option<Usage>,
    message: &'static str,
) -> Result<Response, ApiError> {
    let _ = request
        .finish_terminal(
            FinalStatus::UpstreamError,
            StatusCode::BAD_GATEWAY,
            StatusCode::BAD_GATEWAY,
            usage.map(|usage| billable_usage(usage, rates)),
        )
        .await;
    Err(ApiError::gateway(message))
}

fn billable_usage(usage: Usage, rates: ModelRates) -> BillableUsage {
    BillableUsage {
        input_tokens: usage.input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        output_tokens: usage.output_tokens,
        rates,
    }
}
