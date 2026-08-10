use std::sync::Arc;

use axum::extract::ws::{CloseFrame as DownstreamCloseFrame, Message as DownstreamMessage};
use axum::extract::{State, WebSocketUpgrade, ws::WebSocket};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame as UpstreamCloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::auth::{ClientIdentity, authenticate};
use crate::error::{ApiError, websocket_error};
use crate::state::AppState;
use crate::store::{
    Admission, ApiProtocol, BillableUsage, FinalStatus, ModelRates, RequestId, RequestMetadata,
    StoreError, Transport,
};
use crate::upstream_ws::{UpstreamWebSocket, connect_upstream_websocket};

struct InFlight {
    request_id: RequestId,
    rates: ModelRates,
}

struct PreparedRequest {
    payload: Value,
    metadata: RequestMetadata,
    rates: ModelRates,
}

struct ValidationError {
    param: &'static str,
    message: &'static str,
}

struct Terminal {
    status: FinalStatus,
    usage: Option<BillableUsage>,
}

pub(crate) async fn responses_websocket(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let identity = match authenticate(&headers, &state.config) {
        Ok(identity) => identity,
        Err(error) => return error.into_response(),
    };
    let upstream_result = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => return ApiError::shutdown().into_response(),
        result = connect_upstream_websocket(
            &state.config.upstream.base_url,
            Arc::clone(&state.credentials),
        ) => result,
    };
    let upstream = match upstream_result {
        Ok(upstream) => upstream,
        Err(_error) => {
            return ApiError::gateway("Failed to connect to the upstream WebSocket")
                .into_response();
        }
    };

    let websocket_task = state.websocket_tasks.token();
    websocket
        .on_upgrade(move |downstream| async move {
            let _websocket_task = websocket_task;
            proxy_connection(downstream, upstream, state, identity).await;
        })
        .into_response()
}

async fn proxy_connection(
    mut downstream: WebSocket,
    mut upstream: UpstreamWebSocket,
    state: Arc<AppState>,
    identity: ClientIdentity,
) {
    let mut in_flight = None;

    loop {
        tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => {
                finalize_active(&state, &mut in_flight, FinalStatus::Canceled).await;
                let _ = upstream
                    .send(UpstreamMessage::Close(Some(UpstreamCloseFrame {
                        code: CloseCode::Away,
                        reason: "Server is shutting down".into(),
                    })))
                    .await;
                send_downstream_close(&mut downstream, 1001, "Server is shutting down").await;
                return;
            }
            downstream_message = downstream.recv() => {
                match downstream_message {
                    Some(Ok(DownstreamMessage::Text(text))) => {
                        let value = match serde_json::from_str::<Value>(text.as_str()) {
                            Ok(value) => value,
                            Err(_) => {
                                if record_rejection(&state, &identity, &Value::Null)
                                    .await
                                    .is_err()
                                {
                                    close_for_internal_error(
                                        &mut downstream,
                                        &mut upstream,
                                        &state,
                                        &mut in_flight,
                                    )
                                    .await;
                                    return;
                                }
                                close_for_client_protocol_error(
                                    &mut downstream,
                                    &mut upstream,
                                    &state,
                                    &mut in_flight,
                                    "response.create must be valid JSON",
                                ).await;
                                return;
                            }
                        };
                        let prepared = match prepare_request(value, &state, &identity) {
                            Ok(prepared) => prepared,
                            Err((value, validation)) => {
                                if record_rejection(&state, &identity, &value).await.is_err() {
                                    close_for_internal_error(
                                        &mut downstream,
                                        &mut upstream,
                                        &state,
                                        &mut in_flight,
                                    ).await;
                                    return;
                                }
                                let error = websocket_error(
                                    "invalid_request_error",
                                    Some(validation.param),
                                    validation.message,
                                );
                                if send_downstream_json(&mut downstream, error).await.is_err() {
                                    close_after_client_disconnect(
                                        &mut upstream,
                                        &state,
                                        &mut in_flight,
                                        None,
                                    ).await;
                                    return;
                                }
                                continue;
                            }
                        };

                        let admission = match state
                            .store
                            .begin_request(&prepared.metadata, identity.weekly_limit_usd)
                            .await
                        {
                            Ok(admission) => admission,
                            Err(_) => {
                                close_for_internal_error(
                                    &mut downstream,
                                    &mut upstream,
                                    &state,
                                    &mut in_flight,
                                ).await;
                                return;
                            }
                        };
                        let request_id = match admission {
                            Admission::Admitted(request_id) => request_id,
                            Admission::WeeklyQuotaExceeded(_) => {
                                let error = websocket_error(
                                    "weekly_quota_exceeded",
                                    None,
                                    "The configured weekly quota has been exceeded",
                                );
                                if send_downstream_json(&mut downstream, error).await.is_err() {
                                    close_after_client_disconnect(
                                        &mut upstream,
                                        &state,
                                        &mut in_flight,
                                        None,
                                    ).await;
                                    return;
                                }
                                continue;
                            }
                        };

                        if in_flight.is_some() {
                            if state
                                .store
                                .finalize_request(request_id, FinalStatus::Rejected, None, None)
                                .await
                                .is_err()
                            {
                                close_for_internal_error(
                                    &mut downstream,
                                    &mut upstream,
                                    &state,
                                    &mut in_flight,
                                )
                                .await;
                                return;
                            }
                            let error = websocket_error(
                                "response_in_progress",
                                None,
                                "A response is already in progress on this connection",
                            );
                            if send_downstream_json(&mut downstream, error).await.is_err() {
                                close_after_client_disconnect(
                                    &mut upstream,
                                    &state,
                                    &mut in_flight,
                                    None,
                                )
                                .await;
                                return;
                            }
                            continue;
                        }

                        let payload = prepared.payload.to_string();
                        if upstream
                            .send(UpstreamMessage::Text(payload.into()))
                            .await
                            .is_err()
                        {
                            let _ = state
                                .store
                                .finalize_request(
                                    request_id,
                                    FinalStatus::UpstreamError,
                                    None,
                                    None,
                                )
                                .await;
                            send_downstream_close(
                                &mut downstream,
                                1011,
                                "Upstream WebSocket failure",
                            ).await;
                            return;
                        }
                        in_flight = Some(InFlight {
                            request_id,
                            rates: prepared.rates,
                        });
                    }
                    Some(Ok(DownstreamMessage::Binary(_))) => {
                        close_for_client_protocol_error(
                            &mut downstream,
                            &mut upstream,
                            &state,
                            &mut in_flight,
                            "Binary application messages are unsupported",
                        ).await;
                        return;
                    }
                    Some(Ok(DownstreamMessage::Ping(payload))) => {
                        if upstream.send(UpstreamMessage::Ping(payload)).await.is_err() {
                            close_for_upstream_failure(
                                &mut downstream,
                                &state,
                                &mut in_flight,
                            ).await;
                            return;
                        }
                    }
                    Some(Ok(DownstreamMessage::Pong(payload))) => {
                        if upstream.send(UpstreamMessage::Pong(payload)).await.is_err() {
                            close_for_upstream_failure(
                                &mut downstream,
                                &state,
                                &mut in_flight,
                            ).await;
                            return;
                        }
                    }
                    Some(Ok(DownstreamMessage::Close(frame))) => {
                        let upstream_frame = frame.map(downstream_close_to_upstream);
                        let _ = downstream.flush().await;
                        close_after_client_disconnect(
                            &mut upstream,
                            &state,
                            &mut in_flight,
                            upstream_frame,
                        ).await;
                        return;
                    }
                    Some(Err(_)) | None => {
                        close_after_client_disconnect(
                            &mut upstream,
                            &state,
                            &mut in_flight,
                            None,
                        ).await;
                        return;
                    }
                }
            }
            upstream_message = upstream.next() => {
                match upstream_message {
                    Some(Ok(UpstreamMessage::Text(text))) => {
                        let event = match serde_json::from_str::<Value>(text.as_str()) {
                            Ok(event) => event,
                            Err(_) => {
                                close_for_upstream_failure(
                                    &mut downstream,
                                    &state,
                                    &mut in_flight,
                                ).await;
                                return;
                            }
                        };
                        let terminal = match inspect_terminal(&event, in_flight.as_ref()) {
                            Ok(terminal) => terminal,
                            Err(()) => {
                                close_for_upstream_failure(
                                    &mut downstream,
                                    &state,
                                    &mut in_flight,
                                ).await;
                                return;
                            }
                        };
                        if let Some(terminal) = terminal {
                            let Some(active) = in_flight.take() else {
                                close_for_upstream_failure(
                                    &mut downstream,
                                    &state,
                                    &mut in_flight,
                                ).await;
                                return;
                            };
                            let request_id = active.request_id;
                            if state
                                .store
                                .finalize_request(
                                    request_id,
                                    terminal.status,
                                    None,
                                    terminal.usage,
                                )
                                .await
                                .is_err()
                            {
                                let _ = state
                                    .store
                                    .finalize_request(
                                        request_id,
                                        FinalStatus::UpstreamError,
                                        None,
                                        None,
                                    )
                                    .await;
                                send_downstream_close(
                                    &mut downstream,
                                    1011,
                                    "Internal WebSocket accounting failure",
                                ).await;
                                let _ = upstream.send(UpstreamMessage::Close(None)).await;
                                return;
                            }
                        }
                        if downstream
                            .send(DownstreamMessage::Text(text.to_string().into()))
                            .await
                            .is_err()
                        {
                            close_after_client_disconnect(
                                &mut upstream,
                                &state,
                                &mut in_flight,
                                None,
                            ).await;
                            return;
                        }
                    }
                    Some(Ok(UpstreamMessage::Binary(_))) | Some(Ok(UpstreamMessage::Frame(_))) => {
                        close_for_upstream_failure(
                            &mut downstream,
                            &state,
                            &mut in_flight,
                        ).await;
                        return;
                    }
                    Some(Ok(UpstreamMessage::Ping(payload))) => {
                        if downstream.send(DownstreamMessage::Ping(payload)).await.is_err() {
                            close_after_client_disconnect(
                                &mut upstream,
                                &state,
                                &mut in_flight,
                                None,
                            ).await;
                            return;
                        }
                    }
                    Some(Ok(UpstreamMessage::Pong(payload))) => {
                        if downstream.send(DownstreamMessage::Pong(payload)).await.is_err() {
                            close_after_client_disconnect(
                                &mut upstream,
                                &state,
                                &mut in_flight,
                                None,
                            ).await;
                            return;
                        }
                    }
                    Some(Ok(UpstreamMessage::Close(frame))) => {
                        let normal_close = frame
                            .as_ref()
                            .is_none_or(|frame| matches!(frame.code, CloseCode::Normal | CloseCode::Away));
                        let _ = upstream.flush().await;
                        if in_flight.is_some() {
                            close_for_upstream_failure(
                                &mut downstream,
                                &state,
                                &mut in_flight,
                            ).await;
                        } else if normal_close {
                            let downstream_frame = frame.map(upstream_close_to_downstream);
                            let _ = downstream
                                .send(DownstreamMessage::Close(downstream_frame))
                                .await;
                        } else {
                            send_downstream_close(
                                &mut downstream,
                                1011,
                                "Upstream WebSocket failure",
                            )
                            .await;
                        }
                        return;
                    }
                    Some(Err(_)) | None => {
                        close_for_upstream_failure(
                            &mut downstream,
                            &state,
                            &mut in_flight,
                        ).await;
                        return;
                    }
                }
            }
        }
    }
}

fn prepare_request(
    mut value: Value,
    state: &AppState,
    identity: &ClientIdentity,
) -> Result<PreparedRequest, (Value, ValidationError)> {
    let validation = (|| {
        let object = value.as_object_mut().ok_or(ValidationError {
            param: "type",
            message: "WebSocket messages must be response.create objects",
        })?;
        if object.get("type").and_then(Value::as_str) != Some("response.create") {
            return Err(ValidationError {
                param: "type",
                message: "Only response.create messages are supported",
            });
        }
        let model = object
            .get("model")
            .and_then(Value::as_str)
            .ok_or(ValidationError {
                param: "model",
                message: "response.create requires a model",
            })?
            .to_owned();
        let price = state
            .config
            .model_prices
            .get(&model)
            .ok_or(ValidationError {
                param: "model",
                message: "The requested model is not configured",
            })?;
        if object.contains_key("stream") {
            return Err(ValidationError {
                param: "stream",
                message: "stream is not used in WebSocket mode",
            });
        }
        if object.contains_key("background") {
            return Err(ValidationError {
                param: "background",
                message: "background is not used in WebSocket mode",
            });
        }
        if object.contains_key("max_output_tokens") {
            return Err(ValidationError {
                param: "max_output_tokens",
                message: "max_output_tokens is not supported by the ChatGPT Codex upstream",
            });
        }
        if object
            .get("store")
            .is_some_and(|store| !store.is_null() && store.as_bool() != Some(false))
        {
            return Err(ValidationError {
                param: "store",
                message: "store must be false in WebSocket mode",
            });
        }

        let reasoning_effort = object
            .get("reasoning")
            .and_then(|reasoning| reasoning.get("effort"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let rates = ModelRates {
            input_usd_per_million: price.input_usd_per_million,
            cached_input_usd_per_million: price.cached_input_usd_per_million,
            output_usd_per_million: price.output_usd_per_million,
        };
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
        object.insert("stream".to_owned(), Value::Bool(true));
        Ok((model, reasoning_effort, rates))
    })();

    match validation {
        Ok((model, reasoning_effort, rates)) => Ok(PreparedRequest {
            payload: value,
            metadata: RequestMetadata {
                api_key_id: identity.id.clone(),
                model,
                reasoning_effort,
                api_protocol: ApiProtocol::Responses,
                transport: Transport::WebSocket,
            },
            rates,
        }),
        Err(error) => Err((value, error)),
    }
}

async fn record_rejection(
    state: &AppState,
    identity: &ClientIdentity,
    value: &Value,
) -> Result<(), StoreError> {
    let metadata = RequestMetadata {
        api_key_id: identity.id.clone(),
        model: value
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        reasoning_effort: value
            .get("reasoning")
            .and_then(|reasoning| reasoning.get("effort"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        api_protocol: ApiProtocol::Responses,
        transport: Transport::WebSocket,
    };
    match state.store.begin_request(&metadata, None).await? {
        Admission::Admitted(request_id) => {
            state
                .store
                .finalize_request(request_id, FinalStatus::Rejected, None, None)
                .await
        }
        Admission::WeeklyQuotaExceeded(_) => Ok(()),
    }
}

fn inspect_terminal(event: &Value, active: Option<&InFlight>) -> Result<Option<Terminal>, ()> {
    let event_type = event.get("type").and_then(Value::as_str).ok_or(())?;
    if event_type == "error" {
        return Ok(Some(Terminal {
            status: FinalStatus::UpstreamError,
            usage: None,
        }));
    }
    let (status, expected_response_status) = match event_type {
        "response.completed" => (FinalStatus::Completed, "completed"),
        "response.incomplete" => (FinalStatus::Incomplete, "incomplete"),
        "response.failed" => (FinalStatus::UpstreamError, "failed"),
        _ => return Ok(None),
    };
    let response = event.get("response").and_then(Value::as_object).ok_or(())?;
    if response.get("status").and_then(Value::as_str) != Some(expected_response_status) {
        return Err(());
    }
    let usage_value = response.get("usage").filter(|usage| !usage.is_null());
    let usage = match usage_value {
        Some(usage) => {
            let active = active.ok_or(())?;
            let input_tokens = usage
                .get("input_tokens")
                .and_then(Value::as_u64)
                .ok_or(())?;
            let cached_input_tokens = match usage.get("input_tokens_details") {
                None | Some(Value::Null) => 0,
                Some(details) => {
                    let details = details.as_object().ok_or(())?;
                    match details.get("cached_tokens") {
                        None | Some(Value::Null) => 0,
                        Some(cached_tokens) => cached_tokens.as_u64().ok_or(())?,
                    }
                }
            };
            if cached_input_tokens > input_tokens {
                return Err(());
            }
            let output_tokens = usage
                .get("output_tokens")
                .and_then(Value::as_u64)
                .ok_or(())?;
            Some(BillableUsage {
                input_tokens,
                cached_input_tokens,
                output_tokens,
                rates: active.rates,
            })
        }
        None if matches!(status, FinalStatus::UpstreamError) => None,
        None => return Err(()),
    };
    Ok(Some(Terminal { status, usage }))
}

async fn send_downstream_json(downstream: &mut WebSocket, value: Value) -> Result<(), ()> {
    downstream
        .send(DownstreamMessage::Text(value.to_string().into()))
        .await
        .map_err(|_| ())
}

async fn finalize_active(state: &AppState, active: &mut Option<InFlight>, status: FinalStatus) {
    if let Some(active) = active.take() {
        let _ = state
            .store
            .finalize_request(active.request_id, status, None, None)
            .await;
    }
}

async fn close_after_client_disconnect(
    upstream: &mut UpstreamWebSocket,
    state: &AppState,
    active: &mut Option<InFlight>,
    frame: Option<UpstreamCloseFrame>,
) {
    finalize_active(state, active, FinalStatus::Canceled).await;
    let _ = upstream.send(UpstreamMessage::Close(frame)).await;
}

async fn close_for_client_protocol_error(
    downstream: &mut WebSocket,
    upstream: &mut UpstreamWebSocket,
    state: &AppState,
    active: &mut Option<InFlight>,
    reason: &'static str,
) {
    finalize_active(state, active, FinalStatus::Canceled).await;
    let upstream_frame = UpstreamCloseFrame {
        code: CloseCode::Unsupported,
        reason: reason.into(),
    };
    let _ = upstream
        .send(UpstreamMessage::Close(Some(upstream_frame)))
        .await;
    send_downstream_close(downstream, 1003, reason).await;
}

async fn close_for_upstream_failure(
    downstream: &mut WebSocket,
    state: &AppState,
    active: &mut Option<InFlight>,
) {
    finalize_active(state, active, FinalStatus::UpstreamError).await;
    send_downstream_close(downstream, 1011, "Upstream WebSocket failure").await;
}

async fn close_for_internal_error(
    downstream: &mut WebSocket,
    upstream: &mut UpstreamWebSocket,
    state: &AppState,
    active: &mut Option<InFlight>,
) {
    finalize_active(state, active, FinalStatus::InternalError).await;
    let _ = upstream.send(UpstreamMessage::Close(None)).await;
    send_downstream_close(downstream, 1011, "Internal WebSocket failure").await;
}

async fn send_downstream_close(downstream: &mut WebSocket, code: u16, reason: &'static str) {
    let _ = downstream
        .send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
}

fn downstream_close_to_upstream(frame: DownstreamCloseFrame) -> UpstreamCloseFrame {
    UpstreamCloseFrame {
        code: CloseCode::from(frame.code),
        reason: frame.reason.to_string().into(),
    }
}

fn upstream_close_to_downstream(frame: UpstreamCloseFrame) -> DownstreamCloseFrame {
    DownstreamCloseFrame {
        code: u16::from(frame.code),
        reason: frame.reason.to_string().into(),
    }
}
