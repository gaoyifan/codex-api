use std::{convert::Infallible, sync::Arc};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt, stream};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tokio_util::sync::CancellationToken;

mod request;
mod responses_session;

pub(crate) use responses_session::ResponsesSessions;

use request::PendingRequest;

use crate::{
    auth::{ClientIdentity, authenticate},
    chat,
    config::apply_effective_request,
    error::ApiError,
    responses_terminal::{TerminalEvent, TerminalKind, Usage, parse_terminal_payload},
    sse::{SseEvent, SseReader},
    state::AppState,
    store::{
        Admission, ApiProtocol, BillableUsage, EffectiveModel, FinalStatus, ModelAccess,
        ModelCandidates, ModelRates, RequestContext, RequestId, Transport,
    },
};

pub(crate) async fn usage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<crate::usage::AccountUsage>, ApiError> {
    authenticate(&headers, &state.config)?;
    tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => Err(ApiError::shutdown()),
        result = state.upstream_http.usage() => {
            result.map(Json).map_err(|_| ApiError::gateway("Upstream usage request failed"))
        }
    }
}

pub(crate) async fn models(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let identity = authenticate(&headers, &state.config)?;
    let data = available_models(&state, &identity).await?;
    Ok(Json(serde_json::json!({
        "object": "list",
        "data": data,
    })))
}

pub(crate) async fn model(
    State(state): State<Arc<AppState>>,
    Path(model): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let identity = authenticate(&headers, &state.config)?;
    available_models(&state, &identity)
        .await?
        .into_iter()
        .find(|candidate| candidate.get("id").and_then(Value::as_str) == Some(model.as_str()))
        .map(Json)
        .ok_or_else(|| ApiError::model_not_found(&model))
}

async fn available_models(
    state: &AppState,
    identity: &ClientIdentity,
) -> Result<Vec<Value>, ApiError> {
    let access = state
        .store
        .model_access(
            &identity.id,
            identity.quota,
            state.config.fallback_model.is_some(),
        )
        .await
        .map_err(|_| ApiError::internal())?;
    if access == ModelAccess::Blocked {
        return Ok(Vec::new());
    }

    let upstream_models = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => return Err(ApiError::shutdown()),
        result = state.upstream_http.models() => {
            result.map_err(|_| ApiError::gateway("Upstream models request failed"))?
        }
    };
    let fallback_model = state.config.fallback_model.as_deref();
    Ok(upstream_models
        .into_iter()
        .filter_map(|model| {
            let Value::Object(mut object) = model else {
                return None;
            };
            let slug = object.get("slug")?.as_str()?.to_owned();
            if object.get("visibility").and_then(Value::as_str) != Some("list")
                || !state.config.model_prices.contains_key(&slug)
                || (access == ModelAccess::FallbackOnly && fallback_model != Some(slug.as_str()))
            {
                return None;
            }
            object.insert("id".to_owned(), Value::String(slug));
            object.insert("object".to_owned(), Value::String("model".to_owned()));
            object.insert("created".to_owned(), Value::from(0));
            object.insert("owned_by".to_owned(), Value::String("openai".to_owned()));
            Some(Value::Object(object))
        })
        .collect())
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
    let validated = (|| {
        let candidates = state
            .config
            .model_candidates(&model, reasoning_effort.as_deref())
            .ok_or_else(|| ApiError::invalid("model", "model is not configured"))?;
        let body = normalize_responses_request(parsed)?;
        let key = responses_session::SessionKey::from_headers(&identity.id, &headers)?;
        Ok((candidates, body, key))
    })();
    let (candidates, mut upstream_body, key) = match validated {
        Ok(validated) => validated,
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
    let permit = state.acquire_request_slot(&identity).await?;
    let (request_id, effective_model) = admit_request(
        &state,
        &identity,
        candidates,
        ApiProtocol::Responses,
        Transport::HttpSse,
    )
    .await?;
    apply_effective_request(&mut upstream_body, &effective_model);
    let rates = effective_model.rates;
    let request = PendingRequest::new(
        Arc::clone(&state.store),
        request_id,
        state.pending_requests.token(),
        permit,
    );

    let upstream_model = state
        .upstream_http
        .model(&effective_model.model)
        .await
        .map_err(|_| ApiError::gateway("Upstream models request failed"))?
        .filter(|model| model.get("use_responses_lite").and_then(Value::as_bool) == Some(true))
        .ok_or_else(|| ApiError::gateway("Upstream model does not support Responses Lite"))?;
    let receiver = state
        .responses_sessions
        .stream(
            Arc::clone(&state),
            key,
            responses_session::Turn {
                body: upstream_body,
                model: upstream_model,
                headers,
                request_key: format!("codex-api-{}", request_id.0),
                pending: request,
                rates,
            },
        )
        .await?;
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

pub(crate) async fn image_generations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    images(&state, headers, body, "generations").await
}

pub(crate) async fn image_edits(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    images(&state, headers, body, "edits").await
}

async fn images(
    state: &AppState,
    headers: HeaderMap,
    body: Bytes,
    operation: &str,
) -> Result<Response, ApiError> {
    let identity = authenticate(&headers, &state.config)?;
    let parsed: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError::invalid("request", "request body must be valid JSON"))?;
    let permit = state.acquire_request_slot(&identity).await?;
    let upstream = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => return Err(ApiError::shutdown()),
        result = state.upstream_http.images(operation, &parsed, &headers) => {
            result.map_err(|_| ApiError::gateway("Upstream image request failed"))?
        }
    };
    let mut response = Response::builder().status(upstream.status());
    for name in ["content-type", "x-codex-imagegen-request-id"] {
        if let Some(value) = upstream.headers().get(name) {
            response = response.header(name, value);
        }
    }
    let body = upstream
        .bytes_stream()
        .take_until(state.shutdown.clone().cancelled_owned());
    let body = stream::unfold((Box::pin(body), permit), |(mut body, permit)| async move {
        body.next().await.map(|chunk| (chunk, (body, permit)))
    });
    Ok(response
        .body(Body::from_stream(body))
        .expect("upstream response headers are valid"))
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
    let validated = (|| {
        let candidates = state
            .config
            .model_candidates(&tentative_model, tentative_reasoning_effort.as_deref())
            .ok_or_else(|| ApiError::invalid("model", "model is not configured"))?;
        let converted = chat::convert_request(parsed).map_err(|error| {
            ApiError::invalid(
                error.param.unwrap_or_else(|| "request".to_owned()),
                error.message,
            )
        })?;
        Ok((candidates, converted))
    })();
    let (candidates, mut converted) = match validated {
        Ok(validated) => validated,
        Err(error) => {
            reject_request(
                &state,
                &identity,
                &tentative_model,
                tentative_reasoning_effort,
                ApiProtocol::ChatCompletions,
            )
            .await?;
            return Err(error);
        }
    };
    let permit = state.acquire_request_slot(&identity).await?;
    let (request_id, effective_model) = admit_request(
        &state,
        &identity,
        candidates,
        ApiProtocol::ChatCompletions,
        Transport::HttpSse,
    )
    .await?;
    apply_effective_request(&mut converted.upstream_request, &effective_model);
    let rates = effective_model.rates;
    let mut request = PendingRequest::new(
        Arc::clone(&state.store),
        request_id,
        state.pending_requests.token(),
        permit,
    );

    let result = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => {
            request.finish(FinalStatus::Canceled, None).await?;
            return Err(ApiError::shutdown());
        }
        result = state.upstream_http.send(&converted.upstream_request, &headers) => result,
    };
    let upstream = match result {
        Ok(response) if response.status().is_success() => response,
        _ => {
            request
                .finish(FinalStatus::UpstreamError, Some(StatusCode::BAD_GATEWAY))
                .await?;
            return Err(ApiError::gateway("Upstream request failed"));
        }
    };

    if converted.stream {
        request.response_started();
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

    read_chat_terminal(request, rates, upstream, &state.shutdown)
        .await
        .map(|value| Json(value).into_response())
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
    let status = loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                break FinalStatus::Canceled;
            }
            _ = sender.closed() => {
                break FinalStatus::Canceled;
            }
            next = reader.next() => next,
        };
        let event = match next {
            Some(event) => event,
            None => break FinalStatus::UpstreamError,
        };
        let value = match event {
            SseEvent::Terminal(terminal) => {
                let converted = converter.convert_terminal(&terminal);
                let status = match (terminal.kind, converted.is_ok()) {
                    (TerminalKind::Completed, true) => FinalStatus::Completed,
                    (TerminalKind::Incomplete, true) => FinalStatus::Incomplete,
                    _ => FinalStatus::UpstreamError,
                };
                let accounted = request
                    .finish_terminal(
                        status,
                        StatusCode::OK,
                        StatusCode::OK,
                        terminal.usage.map(|usage| billable_usage(usage, rates)),
                    )
                    .await
                    .is_ok();
                if let Some(error) = terminal.upstream_error() {
                    let error = ApiError::upstream(error);
                    let _ = sender.send(chat_sse_bytes(&error.body())).await;
                    return;
                }
                let Ok(converted) = converted else { return };
                if !accounted {
                    return;
                }
                for chunk in converted {
                    if sender.send(chat_sse_bytes(&chunk)).await.is_err() {
                        return;
                    }
                }
                let _ = sender.send(Bytes::from_static(b"data: [DONE]\n\n")).await;
                return;
            }
            SseEvent::Event(value) => value,
        };
        let chunk = match converter.convert_event(&value) {
            Ok(chunk) => chunk,
            Err(_) => break FinalStatus::UpstreamError,
        };
        if let Some(chunk) = chunk
            && sender.send(chat_sse_bytes(&chunk)).await.is_err()
        {
            break FinalStatus::Canceled;
        }
    };
    let _ = request.finish(status, Some(StatusCode::OK)).await;
}

fn chat_sse_bytes(value: &Value) -> Bytes {
    Bytes::from(format!("data: {value}\n\n"))
}

async fn read_chat_terminal(
    mut request: PendingRequest,
    rates: ModelRates,
    upstream: reqwest::Response,
    shutdown: &CancellationToken,
) -> Result<Value, ApiError> {
    let mut reader = SseReader::new(upstream.bytes_stream());
    let mut completed_output_items = Vec::new();
    let mut terminal = loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                drop(reader);
                request.finish(FinalStatus::Canceled, None).await?;
                return Err(ApiError::shutdown());
            }
            next = reader.next() => next,
        };
        match next {
            Some(SseEvent::Terminal(terminal)) => break terminal,
            Some(SseEvent::Event(value)) => {
                if value.get("type").and_then(Value::as_str) == Some("response.output_item.done")
                    && let Some(item) = value.get("item")
                {
                    completed_output_items.push(item.clone());
                }
            }
            None => {
                drop(reader);
                request
                    .finish(FinalStatus::UpstreamError, Some(StatusCode::BAD_GATEWAY))
                    .await?;
                return Err(ApiError::gateway(
                    "Upstream response ended without a terminal",
                ));
            }
        }
    };
    drop(reader);

    let converted = if let Some(error) = terminal.upstream_error() {
        Err(ApiError::upstream(error))
    } else {
        let output_is_empty = matches!(
            terminal.payload.pointer("/response/output"),
            Some(Value::Array(output)) if output.is_empty()
        );
        if output_is_empty && !completed_output_items.is_empty() {
            terminal.payload["response"]["output"] = Value::Array(completed_output_items);
        }
        chat::convert_terminal_event(&terminal).map_err(|error| {
            tracing::warn!(%error, "Upstream Chat terminal conversion failed");
            ApiError::gateway("Upstream terminal response could not be converted")
        })
    };
    let usage = terminal.usage.map(|usage| billable_usage(usage, rates));
    let completion = match converted {
        Ok(completion) => completion,
        Err(error) => {
            drop(terminal);
            let _ = request
                .finish_terminal(
                    FinalStatus::UpstreamError,
                    StatusCode::BAD_GATEWAY,
                    StatusCode::BAD_GATEWAY,
                    usage,
                )
                .await;
            return Err(error);
        }
    };
    let status = match terminal.kind {
        TerminalKind::Completed => FinalStatus::Completed,
        TerminalKind::Incomplete => FinalStatus::Incomplete,
        TerminalKind::Failed | TerminalKind::Error => unreachable!("converted completion"),
    };
    drop(terminal);
    request
        .finish_terminal(status, StatusCode::OK, StatusCode::BAD_GATEWAY, usage)
        .await
        .map_err(|_| ApiError::gateway("Upstream terminal response could not be accounted"))?;
    Ok(completion)
}

async fn forward_websocket_responses_stream(
    request: &mut PendingRequest,
    rates: ModelRates,
    upstream: &mut crate::upstream_ws::UpstreamWebSocket,
    sender: &mpsc::Sender<Bytes>,
) -> Option<(TerminalEvent, Bytes)> {
    let mut completed_output_items = Vec::new();
    let status = loop {
        let next = upstream.next().await;
        let Some(Ok(message)) = next else {
            break FinalStatus::Canceled;
        };
        match message {
            UpstreamMessage::Text(text) => {
                let event = match serde_json::from_str::<Value>(text.as_str()) {
                    Ok(event) => event,
                    Err(_) => break FinalStatus::UpstreamError,
                };
                let event_name = match event.get("type").and_then(Value::as_str) {
                    Some(event_name) => event_name.to_owned(),
                    None => {
                        break FinalStatus::UpstreamError;
                    }
                };
                if event_name == "response.output_item.done"
                    && let Some(item) = event.get("item").filter(|item| item.is_object())
                {
                    completed_output_items.push(item.clone());
                }
                let terminal = match parse_terminal_payload(event) {
                    Ok(terminal) => terminal,
                    Err(_) => break FinalStatus::UpstreamError,
                };
                let rewritten = terminal
                    .as_ref()
                    .and_then(TerminalEvent::retryable_capacity_payload)
                    .map(|payload| payload.to_string());
                let event_name = if rewritten.is_some() {
                    "response.failed"
                } else {
                    &event_name
                };
                let downstream_text = rewritten.as_deref().unwrap_or(text.as_str());
                let bytes = Bytes::from(format!(
                    "id: \nevent: {event_name}\ndata: {downstream_text}\n\n"
                ));
                if let Some(mut terminal) = terminal {
                    // Private Responses can omit the final output array. Keep SSE bytes unchanged;
                    // the session snapshot still needs the completed items to rebuild its context.
                    let output = terminal.payload.pointer("/response/output");
                    if terminal.kind == TerminalKind::Completed
                        && !completed_output_items.is_empty()
                        && (matches!(output, None | Some(Value::Null))
                            || output.and_then(Value::as_array).is_some_and(Vec::is_empty))
                    {
                        terminal.payload["response"]["output"] =
                            Value::Array(completed_output_items);
                    }
                    let status = match terminal.kind {
                        TerminalKind::Completed => FinalStatus::Completed,
                        TerminalKind::Incomplete => FinalStatus::Incomplete,
                        TerminalKind::Failed | TerminalKind::Error => FinalStatus::UpstreamError,
                    };
                    if request
                        .finish_terminal(
                            status,
                            StatusCode::OK,
                            StatusCode::OK,
                            terminal.usage.map(|usage| billable_usage(usage, rates)),
                        )
                        .await
                        .is_err()
                    {
                        return None;
                    }
                    return Some((terminal, bytes));
                }
                if sender.send(bytes).await.is_err() {
                    break FinalStatus::Canceled;
                }
            }
            UpstreamMessage::Ping(payload) => {
                if upstream.send(UpstreamMessage::Pong(payload)).await.is_err() {
                    break FinalStatus::UpstreamError;
                }
            }
            UpstreamMessage::Pong(_) => {}
            UpstreamMessage::Close(_) | UpstreamMessage::Binary(_) | UpstreamMessage::Frame(_) => {
                break FinalStatus::UpstreamError;
            }
        }
    };
    let _ = request.finish(status, Some(StatusCode::OK)).await;
    None
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
    candidates: ModelCandidates,
    api_protocol: ApiProtocol,
    transport: Transport,
) -> Result<(RequestId, EffectiveModel), ApiError> {
    let context = RequestContext {
        api_key_id: identity.id.clone(),
        api_protocol,
        transport,
    };
    match state
        .store
        .begin_request(&context, identity.quota, candidates)
        .await
        .map_err(|_| ApiError::internal())?
    {
        Admission::WeeklyQuotaExceeded => Err(ApiError::quota_exceeded()),
        Admission::Admitted {
            request_id,
            effective_model,
        } => Ok((request_id, effective_model)),
    }
}

async fn reject_request(
    state: &AppState,
    identity: &ClientIdentity,
    model: &str,
    reasoning_effort: Option<String>,
    api_protocol: ApiProtocol,
) -> Result<(), ApiError> {
    let context = RequestContext {
        api_key_id: identity.id.clone(),
        api_protocol,
        transport: Transport::HttpSse,
    };
    state
        .store
        .record_rejection(
            &context,
            model,
            reasoning_effort.as_deref(),
            Some(StatusCode::BAD_REQUEST.as_u16()),
        )
        .await
        .map_err(|_| ApiError::internal())
}
