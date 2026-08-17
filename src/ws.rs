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
use crate::responses_terminal::{TerminalKind, parse_terminal_payload};
use crate::state::AppState;
use crate::store::{
    Admission, ApiProtocol, BillableUsage, FinalStatus, ModelRates, QuotaLimits, RequestId,
    RequestMetadata, StoreError, Transport,
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
    downstream: WebSocket,
    upstream: UpstreamWebSocket,
    state: Arc<AppState>,
    identity: ClientIdentity,
) {
    WsSession {
        downstream,
        upstream,
        state,
        identity,
        active: None,
    }
    .run()
    .await;
}

struct WsSession {
    downstream: WebSocket,
    upstream: UpstreamWebSocket,
    state: Arc<AppState>,
    identity: ClientIdentity,
    active: Option<InFlight>,
}

enum ConnectionEvent {
    Shutdown,
    ClientGone,
    Downstream(DownstreamMessage),
    UpstreamGone,
    Upstream(UpstreamMessage),
}

enum ConnectionEnd {
    Shutdown,
    ClientDisconnect(Option<UpstreamCloseFrame>),
    ClientProtocolError,
    UpstreamFailure,
    UpstreamClosed(Option<UpstreamCloseFrame>),
    InternalFailure,
    AccountingFailure,
}

impl WsSession {
    async fn run(mut self) {
        loop {
            let event = tokio::select! {
                biased;
                _ = self.state.shutdown.cancelled() => ConnectionEvent::Shutdown,
                message = self.downstream.recv() => match message {
                    Some(Ok(message)) => ConnectionEvent::Downstream(message),
                    Some(Err(_)) | None => ConnectionEvent::ClientGone,
                },
                message = self.upstream.next() => match message {
                    Some(Ok(message)) => ConnectionEvent::Upstream(message),
                    Some(Err(_)) | None => ConnectionEvent::UpstreamGone,
                },
            };

            let end = match event {
                ConnectionEvent::Shutdown => Some(ConnectionEnd::Shutdown),
                ConnectionEvent::ClientGone => Some(ConnectionEnd::ClientDisconnect(None)),
                ConnectionEvent::Downstream(message) => self.handle_downstream(message).await,
                ConnectionEvent::UpstreamGone => Some(ConnectionEnd::UpstreamFailure),
                ConnectionEvent::Upstream(message) => self.handle_upstream(message).await,
            };
            if let Some(end) = end {
                self.finish(end).await;
                return;
            }
        }
    }

    async fn handle_downstream(&mut self, message: DownstreamMessage) -> Option<ConnectionEnd> {
        match message {
            DownstreamMessage::Text(text) => self.handle_create(text.as_str()).await,
            DownstreamMessage::Binary(_) => Some(ConnectionEnd::ClientProtocolError),
            DownstreamMessage::Ping(payload) => self
                .upstream
                .send(UpstreamMessage::Ping(payload))
                .await
                .err()
                .map(|_| ConnectionEnd::UpstreamFailure),
            DownstreamMessage::Pong(payload) => self
                .upstream
                .send(UpstreamMessage::Pong(payload))
                .await
                .err()
                .map(|_| ConnectionEnd::UpstreamFailure),
            DownstreamMessage::Close(frame) => {
                let _ = self.downstream.flush().await;
                Some(ConnectionEnd::ClientDisconnect(
                    frame.map(downstream_close_to_upstream),
                ))
            }
        }
    }

    async fn handle_create(&mut self, text: &str) -> Option<ConnectionEnd> {
        let value = match serde_json::from_str::<Value>(text) {
            Ok(value) => value,
            Err(_) => {
                return self
                    .reject(
                        &Value::Null,
                        websocket_error(
                            "invalid_request_error",
                            None,
                            "response.create must be valid JSON",
                        ),
                    )
                    .await;
            }
        };
        let mut prepared = match prepare_request(value, &self.state, &self.identity) {
            Ok(prepared) => prepared,
            Err((value, validation)) => {
                return self
                    .reject(
                        &value,
                        websocket_error(
                            "invalid_request_error",
                            Some(validation.param),
                            validation.message,
                        ),
                    )
                    .await;
            }
        };

        let admission = match self
            .state
            .store
            .begin_request(
                &prepared.metadata,
                self.identity.quota,
                self.state.config.fallback_model.as_deref(),
            )
            .await
        {
            Ok(admission) => admission,
            Err(_) => return Some(ConnectionEnd::InternalFailure),
        };
        let request_id = match admission {
            Admission::Admitted(request_id) => request_id,
            Admission::UseFallback(request_id) => {
                if !self
                    .state
                    .config
                    .apply_fallback_model(&mut prepared.payload, &mut prepared.rates)
                {
                    return Some(ConnectionEnd::InternalFailure);
                }
                request_id
            }
            Admission::WeeklyQuotaExceeded(_) => {
                return self
                    .send_error(websocket_error(
                        "weekly_quota_exceeded",
                        None,
                        "The configured weekly quota has been exceeded",
                    ))
                    .await;
            }
        };

        if self.active.is_some() {
            if self
                .state
                .store
                .finalize_request(request_id, FinalStatus::Rejected, None, None)
                .await
                .is_err()
            {
                return Some(ConnectionEnd::InternalFailure);
            }
            return self
                .send_error(websocket_error(
                    "response_in_progress",
                    None,
                    "A response is already in progress on this connection",
                ))
                .await;
        }

        if self
            .upstream
            .send(UpstreamMessage::Text(prepared.payload.to_string().into()))
            .await
            .is_err()
        {
            let _ = self
                .state
                .store
                .finalize_request(request_id, FinalStatus::UpstreamError, None, None)
                .await;
            return Some(ConnectionEnd::UpstreamFailure);
        }
        self.active = Some(InFlight {
            request_id,
            rates: prepared.rates,
        });
        None
    }

    async fn reject(&mut self, value: &Value, error: Value) -> Option<ConnectionEnd> {
        if record_rejection(&self.state, &self.identity, value)
            .await
            .is_err()
        {
            return Some(ConnectionEnd::InternalFailure);
        }
        self.send_error(error).await
    }

    async fn send_error(&mut self, error: Value) -> Option<ConnectionEnd> {
        self.downstream
            .send(DownstreamMessage::Text(error.to_string().into()))
            .await
            .err()
            .map(|_| ConnectionEnd::ClientDisconnect(None))
    }

    async fn handle_upstream(&mut self, message: UpstreamMessage) -> Option<ConnectionEnd> {
        match message {
            UpstreamMessage::Text(text) => self.handle_upstream_text(text.as_str()).await,
            UpstreamMessage::Binary(_) | UpstreamMessage::Frame(_) => {
                Some(ConnectionEnd::UpstreamFailure)
            }
            UpstreamMessage::Ping(payload) => self
                .downstream
                .send(DownstreamMessage::Ping(payload))
                .await
                .err()
                .map(|_| ConnectionEnd::ClientDisconnect(None)),
            UpstreamMessage::Pong(payload) => self
                .downstream
                .send(DownstreamMessage::Pong(payload))
                .await
                .err()
                .map(|_| ConnectionEnd::ClientDisconnect(None)),
            UpstreamMessage::Close(frame) => {
                let _ = self.upstream.flush().await;
                Some(ConnectionEnd::UpstreamClosed(frame))
            }
        }
    }

    async fn handle_upstream_text(&mut self, text: &str) -> Option<ConnectionEnd> {
        let event = match serde_json::from_str::<Value>(text) {
            Ok(event) if event.get("type").and_then(Value::as_str).is_some() => event,
            Ok(_) | Err(_) => return Some(ConnectionEnd::UpstreamFailure),
        };
        let terminal = match parse_terminal_payload(event) {
            Ok(terminal) => terminal,
            Err(_) => return Some(ConnectionEnd::UpstreamFailure),
        };
        if let Some(terminal) = terminal {
            let Some(active) = self.active.take() else {
                return Some(ConnectionEnd::UpstreamFailure);
            };
            let status = match terminal.kind {
                TerminalKind::Completed => FinalStatus::Completed,
                TerminalKind::Incomplete => FinalStatus::Incomplete,
                TerminalKind::Failed | TerminalKind::Error => FinalStatus::UpstreamError,
            };
            let usage = terminal.usage.map(|usage| BillableUsage {
                input_tokens: usage.input_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                output_tokens: usage.output_tokens,
                rates: active.rates,
            });
            if self
                .state
                .store
                .finalize_request(active.request_id, status, None, usage)
                .await
                .is_err()
            {
                let _ = self
                    .state
                    .store
                    .finalize_request(active.request_id, FinalStatus::UpstreamError, None, None)
                    .await;
                return Some(ConnectionEnd::AccountingFailure);
            }
        }
        self.downstream
            .send(DownstreamMessage::Text(text.to_owned().into()))
            .await
            .err()
            .map(|_| ConnectionEnd::ClientDisconnect(None))
    }

    async fn finish(&mut self, end: ConnectionEnd) {
        match end {
            ConnectionEnd::Shutdown => {
                self.finalize_active(FinalStatus::Canceled).await;
                let _ = self
                    .upstream
                    .send(UpstreamMessage::Close(Some(UpstreamCloseFrame {
                        code: CloseCode::Away,
                        reason: "Server is shutting down".into(),
                    })))
                    .await;
                self.close_downstream(1001, "Server is shutting down").await;
            }
            ConnectionEnd::ClientDisconnect(frame) => {
                self.finalize_active(FinalStatus::Canceled).await;
                let _ = self.upstream.send(UpstreamMessage::Close(frame)).await;
            }
            ConnectionEnd::ClientProtocolError => {
                self.finalize_active(FinalStatus::Canceled).await;
                let reason = "Binary application messages are unsupported";
                let _ = self
                    .upstream
                    .send(UpstreamMessage::Close(Some(UpstreamCloseFrame {
                        code: CloseCode::Unsupported,
                        reason: reason.into(),
                    })))
                    .await;
                self.close_downstream(1003, reason).await;
            }
            ConnectionEnd::UpstreamFailure => {
                self.finalize_active(FinalStatus::UpstreamError).await;
                self.close_downstream(1011, "Upstream WebSocket failure")
                    .await;
            }
            ConnectionEnd::UpstreamClosed(frame) => {
                let normal = frame
                    .as_ref()
                    .is_none_or(|frame| matches!(frame.code, CloseCode::Normal | CloseCode::Away));
                if self.active.is_some() || !normal {
                    self.finalize_active(FinalStatus::UpstreamError).await;
                    self.close_downstream(1011, "Upstream WebSocket failure")
                        .await;
                } else {
                    let _ = self
                        .downstream
                        .send(DownstreamMessage::Close(
                            frame.map(upstream_close_to_downstream),
                        ))
                        .await;
                }
            }
            ConnectionEnd::InternalFailure => {
                self.finalize_active(FinalStatus::InternalError).await;
                let _ = self.upstream.send(UpstreamMessage::Close(None)).await;
                self.close_downstream(1011, "Internal WebSocket failure")
                    .await;
            }
            ConnectionEnd::AccountingFailure => {
                self.close_downstream(1011, "Internal WebSocket accounting failure")
                    .await;
                let _ = self.upstream.send(UpstreamMessage::Close(None)).await;
            }
        }
    }

    async fn finalize_active(&mut self, status: FinalStatus) {
        if let Some(active) = self.active.take() {
            let _ = self
                .state
                .store
                .finalize_request(active.request_id, status, None, None)
                .await;
        }
    }

    async fn close_downstream(&mut self, code: u16, reason: &'static str) {
        let _ = self
            .downstream
            .send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
                code,
                reason: reason.into(),
            })))
            .await;
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
        let rates = state.config.model_rates(&model).ok_or(ValidationError {
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
    match state
        .store
        .begin_request(&metadata, QuotaLimits::unlimited(), None)
        .await?
    {
        Admission::Admitted(request_id) => {
            state
                .store
                .finalize_request(request_id, FinalStatus::Rejected, None, None)
                .await
        }
        Admission::UseFallback(_) | Admission::WeeklyQuotaExceeded(_) => Ok(()),
    }
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
