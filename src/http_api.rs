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

mod request;

use request::PendingRequest;

use crate::{
    auth::{ClientIdentity, authenticate},
    chat::{self, ChatErrorKind, TerminalStatus},
    error::ApiError,
    responses_terminal::{TerminalKind, Usage},
    sse::{SseRead, SseReader},
    state::AppState,
    store::{
        Admission, ApiProtocol, BillableUsage, FinalStatus, ModelRates, QuotaLimits,
        RequestMetadata, Transport,
    },
};

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
    let mut rates = match state.config.model_rates(&model) {
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

    let mut upstream_body = match normalize_responses_request(parsed) {
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
    let admission = admit_request(
        &state,
        &identity,
        model,
        reasoning_effort,
        ApiProtocol::Responses,
        Transport::HttpSse,
    )
    .await?;
    let request_id = match admission {
        Admission::Admitted(request_id) => request_id,
        Admission::UseFallback(request_id) => {
            if !state
                .config
                .apply_fallback_model(&mut upstream_body, &mut rates)
            {
                return Err(ApiError::internal());
            }
            request_id
        }
        Admission::WeeklyQuotaExceeded(_) => unreachable!("quota rejection becomes an error"),
    };
    let mut request = PendingRequest::new(
        Arc::clone(&state.store),
        request_id,
        state.pending_requests.token(),
    );

    let upstream = send_upstream(&state, &mut request, &upstream_body, &headers).await?;
    if !upstream.status().is_success() {
        return upstream_error_response(&mut request, state.shutdown.clone(), upstream).await;
    }

    request.response_started(StatusCode::OK);
    let (sender, receiver) = mpsc::channel::<Bytes>(16);
    state.pending_requests.spawn(forward_responses_stream(
        request,
        rates,
        upstream,
        sender,
        state.shutdown.clone(),
    ));
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
    if state.config.model_rates(&tentative_model).is_none() {
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
    let mut converted = match chat::convert_request(parsed) {
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
    let mut rates = state
        .config
        .model_rates(&converted.model)
        .expect("model was admitted above");
    let admission = admit_request(
        &state,
        &identity,
        converted.model.clone(),
        converted.reasoning_effort.clone(),
        ApiProtocol::ChatCompletions,
        Transport::HttpSse,
    )
    .await?;
    let request_id = match admission {
        Admission::Admitted(request_id) => request_id,
        Admission::UseFallback(request_id) => {
            if !state
                .config
                .apply_fallback_model(&mut converted.upstream_request, &mut rates)
            {
                return Err(ApiError::internal());
            }
            request_id
        }
        Admission::WeeklyQuotaExceeded(_) => unreachable!("quota rejection becomes an error"),
    };
    let mut request = PendingRequest::new(
        Arc::clone(&state.store),
        request_id,
        state.pending_requests.token(),
    );

    let upstream =
        send_upstream(&state, &mut request, &converted.upstream_request, &headers).await?;
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

    if converted.stream {
        request.response_started(StatusCode::OK);
        let (sender, receiver) = mpsc::channel::<Bytes>(16);
        state.pending_requests.spawn(forward_chat_stream(
            request,
            rates,
            upstream,
            sender,
            state.shutdown.clone(),
            converted.include_usage,
        ));
        let body_stream = stream::unfold(receiver, |mut receiver| async move {
            receiver
                .recv()
                .await
                .map(|bytes| (Ok::<Bytes, Infallible>(bytes), receiver))
        });
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(body_stream))
            .expect("static streaming response is valid"));
    }

    let converted_terminal = match read_chat_terminal(upstream, &state.shutdown).await {
        Ok(terminal) => terminal,
        Err(ChatStreamError::Shutdown) => {
            request.finish(FinalStatus::Canceled, None, None).await?;
            return Err(ApiError::shutdown());
        }
        Err(ChatStreamError::InvalidTerminal(usage)) => {
            let _ = request
                .finish_terminal(
                    FinalStatus::UpstreamError,
                    StatusCode::BAD_GATEWAY,
                    StatusCode::BAD_GATEWAY,
                    usage.map(|usage| billable_usage(usage, rates)),
                )
                .await;
            return Err(ApiError::gateway(
                "Upstream terminal response could not be converted",
            ));
        }
        Err(ChatStreamError::MissingTerminal) => {
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
    };
    let status = match converted_terminal.status {
        TerminalStatus::Completed => FinalStatus::Completed,
        TerminalStatus::Incomplete => FinalStatus::Incomplete,
    };
    if request
        .finish_terminal(
            status,
            StatusCode::OK,
            StatusCode::BAD_GATEWAY,
            Some(billable_usage(converted_terminal.usage, rates)),
        )
        .await
        .is_err()
    {
        return Err(ApiError::gateway(
            "Upstream terminal response could not be accounted",
        ));
    }
    Ok(Json(converted_terminal.chat_completion).into_response())
}

async fn forward_chat_stream(
    mut request: PendingRequest,
    rates: ModelRates,
    upstream: reqwest::Response,
    sender: mpsc::Sender<Bytes>,
    shutdown: CancellationToken,
    include_usage: bool,
) {
    let mut reader = SseReader::new(upstream.bytes_stream());
    let mut converter = chat::ChatStreamConverter::new(include_usage);
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
        let event = match next {
            Ok(SseRead::Event(event)) => event,
            Ok(SseRead::Eof) | Err(_) => {
                let _ = request
                    .finish(FinalStatus::UpstreamError, Some(StatusCode::OK), None)
                    .await;
                return;
            }
        };
        if let Some(terminal) = event.terminal() {
            let converted = match converter.convert_terminal(terminal) {
                Ok(converted) => converted,
                Err(_) => {
                    let _ = request
                        .finish_terminal(
                            FinalStatus::UpstreamError,
                            StatusCode::OK,
                            StatusCode::OK,
                            terminal.usage.map(|usage| billable_usage(usage, rates)),
                        )
                        .await;
                    return;
                }
            };
            let status = match converted.status {
                TerminalStatus::Completed => FinalStatus::Completed,
                TerminalStatus::Incomplete => FinalStatus::Incomplete,
            };
            if request
                .finish_terminal(
                    status,
                    StatusCode::OK,
                    StatusCode::OK,
                    Some(billable_usage(converted.usage, rates)),
                )
                .await
                .is_err()
            {
                return;
            }
            for chunk in converted.chunks {
                if sender.send(chat_sse_bytes(&chunk)).await.is_err() {
                    return;
                }
            }
            let _ = sender.send(Bytes::from_static(b"data: [DONE]\n\n")).await;
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        let chunk = match converter.convert_event(&value) {
            Ok(chunk) => chunk,
            Err(_) => {
                let _ = request
                    .finish(FinalStatus::UpstreamError, Some(StatusCode::OK), None)
                    .await;
                return;
            }
        };
        if let Some(chunk) = chunk
            && sender.send(chat_sse_bytes(&chunk)).await.is_err()
        {
            let _ = request
                .finish(FinalStatus::Canceled, Some(StatusCode::OK), None)
                .await;
            return;
        }
    }
}

fn chat_sse_bytes(value: &Value) -> Bytes {
    Bytes::from(format!("data: {value}\n\n"))
}

enum ChatStreamError {
    Shutdown,
    InvalidTerminal(Option<Usage>),
    MissingTerminal,
}

async fn read_chat_terminal(
    upstream: reqwest::Response,
    shutdown: &CancellationToken,
) -> Result<chat::ConvertedTerminal, ChatStreamError> {
    let mut reader = SseReader::new(upstream.bytes_stream());
    let mut completed_output_items = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(ChatStreamError::Shutdown),
            next = reader.next() => next,
        };
        let event = match next {
            Ok(SseRead::Event(event)) => event,
            Ok(SseRead::Eof) | Err(_) => return Err(ChatStreamError::MissingTerminal),
        };
        if event.terminal().is_none() {
            if let Ok(value) = serde_json::from_str::<Value>(&event.data)
                && value.get("type").and_then(Value::as_str) == Some("response.output_item.done")
                && let Some(item) = value.get("item")
            {
                completed_output_items.push(item.clone());
            }
            continue;
        }
        let mut terminal = event.into_terminal().expect("checked above");

        let usage = terminal.usage;
        let output_is_empty = matches!(
            terminal.payload.pointer("/response/output"),
            Some(Value::Array(output)) if output.is_empty()
        );
        if output_is_empty && !completed_output_items.is_empty() {
            terminal.payload["response"]["output"] = Value::Array(completed_output_items);
        }
        return match chat::convert_terminal_event(&terminal) {
            Ok(converted) => Ok(converted),
            Err(error) if error.kind == ChatErrorKind::UpstreamProtocol => {
                Err(ChatStreamError::InvalidTerminal(usage))
            }
            Err(_) => unreachable!("terminal conversion cannot be a request error"),
        };
    }
}

async fn send_upstream(
    state: &AppState,
    request: &mut PendingRequest,
    body: &Value,
    downstream_headers: &HeaderMap,
) -> Result<reqwest::Response, ApiError> {
    let result = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => {
            request.finish(FinalStatus::Canceled, None, None).await?;
            return Err(ApiError::shutdown());
        }
        result = state.upstream_http.send(body, downstream_headers) => result,
    };
    match result {
        Ok(response) => Ok(response),
        Err(_) => {
            request
                .finish(
                    FinalStatus::UpstreamError,
                    Some(StatusCode::BAD_GATEWAY),
                    None,
                )
                .await?;
            Err(ApiError::gateway("Upstream request failed"))
        }
    }
}

async fn forward_responses_stream(
    mut request: PendingRequest,
    rates: ModelRates,
    upstream: reqwest::Response,
    sender: mpsc::Sender<Bytes>,
    shutdown: CancellationToken,
) {
    let mut reader = SseReader::new(upstream.bytes_stream());
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
                    if finalized.is_err() {
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

fn billable_usage(usage: Usage, rates: ModelRates) -> BillableUsage {
    BillableUsage {
        input_tokens: usage.input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        output_tokens: usage.output_tokens,
        rates,
    }
}

async fn admit_request(
    state: &AppState,
    identity: &ClientIdentity,
    model: String,
    reasoning_effort: Option<String>,
    api_protocol: ApiProtocol,
    transport: Transport,
) -> Result<Admission, ApiError> {
    let metadata = RequestMetadata {
        api_key_id: identity.id.clone(),
        model,
        reasoning_effort,
        api_protocol,
        transport,
    };
    match state
        .store
        .begin_request(
            &metadata,
            identity.quota,
            state.config.fallback_model.as_deref(),
        )
        .await
        .map_err(|_| ApiError::internal())?
    {
        Admission::WeeklyQuotaExceeded(_) => Err(ApiError::quota_exceeded()),
        admission => Ok(admission),
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
        .begin_request(&metadata, QuotaLimits::unlimited(), None)
        .await
        .map_err(|_| ApiError::internal())?
    {
        Admission::Admitted(request_id) => request_id,
        Admission::UseFallback(_) | Admission::WeeklyQuotaExceeded(_) => {
            unreachable!("an unlimited ledger entry was rejected")
        }
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
    request.upstream_error_started(status);
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
