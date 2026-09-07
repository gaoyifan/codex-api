use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_tungstenite::tungstenite::Message;

use super::{forward_websocket_responses_stream, request::PendingRequest};
use crate::{
    error::ApiError,
    responses_terminal::TerminalKind,
    state::AppState,
    store::{FinalStatus, ModelRates},
    upstream_headers::codex_request_headers,
    upstream_ws::{
        UpstreamWebSocket, connect_upstream_websocket, responses_lite_prewarm,
        send_responses_prewarm,
    },
};

const IDLE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_IDLE_SESSIONS: usize = 64;
const MAX_CONTEXT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct SessionKey {
    client: String,
    header: &'static str,
    value: String,
}

impl SessionKey {
    pub(super) fn from_headers(
        client: &str,
        headers: &HeaderMap,
    ) -> Result<Option<Self>, ApiError> {
        for header in ["thread_id", "session_id"] {
            if let Some(value) = headers.get(header) {
                let value = value
                    .to_str()
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        ApiError::invalid(header, "session header must be a nonempty string")
                    })?;
                return Ok(Some(Self {
                    client: client.to_owned(),
                    header,
                    value: value.to_owned(),
                }));
            }
        }
        Ok(None)
    }
}

#[derive(Default)]
pub(crate) struct ResponsesSessions(Mutex<Cache>);

#[derive(Default)]
struct Cache {
    entries: HashMap<SessionKey, Entry>,
    next_id: u64,
}

struct Entry {
    id: u64,
    sender: mpsc::Sender<Job>,
    busy: bool,
    last_used: Instant,
    bytes: usize,
}

pub(super) struct Turn {
    pub body: Value,
    pub model: Value,
    pub headers: HeaderMap,
    pub request_key: String,
    pub pending: PendingRequest,
    pub rates: ModelRates,
}

struct Job {
    turn: Turn,
    reply: oneshot::Sender<Result<mpsc::Receiver<Bytes>, ApiError>>,
}

enum TurnEnd {
    Error {
        reply: oneshot::Sender<Result<mpsc::Receiver<Bytes>, ApiError>>,
        error: ApiError,
    },
    Terminal(mpsc::OwnedPermit<Bytes>, Bytes),
    Canceled,
}

impl ResponsesSessions {
    pub(super) async fn stream(
        &self,
        state: Arc<AppState>,
        key: Option<SessionKey>,
        turn: Turn,
    ) -> Result<mpsc::Receiver<Bytes>, ApiError> {
        let (reply, result) = oneshot::channel();
        let mut job = Job { turn, reply };
        let target = {
            let mut cache = self.0.lock().expect("Responses session cache poisoned");
            if let Some(entry) = key.as_ref().and_then(|key| cache.entries.get_mut(key)) {
                if entry.busy {
                    Err(ApiError::session_busy())
                } else {
                    entry.busy = true;
                    Ok(entry.sender.clone())
                }
            } else {
                let (sender, receiver) = mpsc::channel(1);
                let id = cache.next_id;
                cache.next_id += 1;
                if let Some(key) = &key {
                    cache.entries.insert(
                        key.clone(),
                        Entry {
                            id,
                            sender: sender.clone(),
                            busy: true,
                            last_used: Instant::now(),
                            bytes: 0,
                        },
                    );
                }
                state.websocket_tasks.spawn(
                    Session {
                        state: Arc::clone(&state),
                        key,
                        id,
                        receiver,
                        request_key: job.turn.request_key.clone(),
                        upstream: None,
                        handshake_headers: HeaderMap::new(),
                        upstream_response_id: None,
                        snapshot: None,
                        bytes: 0,
                    }
                    .run(),
                );
                Ok(sender)
            }
        };
        match target {
            Err(error) => {
                job.turn
                    .pending
                    .finish(FinalStatus::Rejected, Some(error.status), None)
                    .await?;
                return Err(error);
            }
            Ok(sender) => {
                if let Err(error) = sender.try_send(job) {
                    job = error.into_inner();
                    job.turn
                        .pending
                        .finish(
                            FinalStatus::Canceled,
                            Some(StatusCode::SERVICE_UNAVAILABLE),
                            None,
                        )
                        .await?;
                    return Err(ApiError::shutdown());
                }
            }
        }
        result.await.unwrap_or_else(|_| Err(ApiError::shutdown()))
    }

    fn ready(&self, key: &SessionKey, id: u64, bytes: usize) {
        let mut cache = self.0.lock().expect("Responses session cache poisoned");
        if bytes > MAX_CONTEXT_BYTES && cache.entries.get(key).is_some_and(|entry| entry.id == id) {
            cache.entries.remove(key);
            return;
        }
        if let Some(entry) = cache.entries.get_mut(key).filter(|entry| entry.id == id) {
            entry.busy = false;
            entry.last_used = Instant::now();
            entry.bytes = bytes;
        }
        // Active turns cannot be evicted. Once they finish, their snapshots join this budget.
        loop {
            let idle = cache.entries.values().filter(|entry| !entry.busy).count();
            let bytes: usize = cache.entries.values().map(|entry| entry.bytes).sum();
            if idle <= MAX_IDLE_SESSIONS && bytes <= MAX_CONTEXT_BYTES {
                break;
            }
            let oldest = cache
                .entries
                .iter()
                .filter(|(_, entry)| !entry.busy)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else { break };
            cache.entries.remove(&oldest);
        }
    }

    fn remove_idle(&self, key: &SessionKey, id: u64) -> bool {
        let mut cache = self.0.lock().expect("Responses session cache poisoned");
        match cache.entries.get(key) {
            Some(entry) if entry.id == id && entry.busy => false,
            Some(entry) if entry.id == id => {
                cache.entries.remove(key);
                true
            }
            _ => true,
        }
    }
}

// One materialized history supports both prefix comparison and rebuilding a lost connection.
#[derive(Serialize)]
struct Snapshot {
    response_id: String,
    input: Vec<Value>,
    parameters: Value,
}

struct Session {
    state: Arc<AppState>,
    key: Option<SessionKey>,
    id: u64,
    receiver: mpsc::Receiver<Job>,
    request_key: String,
    upstream: Option<UpstreamWebSocket>,
    handshake_headers: HeaderMap,
    upstream_response_id: Option<String>,
    snapshot: Option<Snapshot>,
    bytes: usize,
}

impl Session {
    async fn run(mut self) {
        let mut idle_since = Instant::now();
        loop {
            tokio::select! {
                biased;
                _ = self.state.shutdown.cancelled() => break,
                message = async {
                    match self.upstream.as_mut() {
                        Some(upstream) => upstream.next().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match message {
                        Some(Ok(Message::Ping(payload))) => {
                            let result = tokio::select! {
                                _ = self.state.shutdown.cancelled() => break,
                                result = self.upstream.as_mut().expect("connected socket").send(Message::Pong(payload)) => result,
                            };
                            if result.is_err() { self.upstream = None; }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Text(text))) if serde_json::from_str::<Value>(&text).ok()
                            .is_some_and(|event| event["type"] == "rate_limits.updated") => {}
                        _ => self.upstream = None,
                    }
                }
                job = self.receiver.recv() => {
                    let Some(job) = job else { break };
                    let end = self.respond(job).await;
                    if let Some(key) = &self.key {
                        self.state.responses_sessions.ready(key, self.id, self.bytes);
                    }
                    // Publish the snapshot and release the session before the client sees completion.
                    match end {
                        TurnEnd::Error { reply, error } => { let _ = reply.send(Err(error)); }
                        TurnEnd::Terminal(permit, bytes) => { permit.send(bytes); }
                        TurnEnd::Canceled => {}
                    }
                    idle_since = Instant::now();
                }
                _ = tokio::time::sleep_until(idle_since + IDLE_TTL) => {
                    if self.key.as_ref().is_none_or(|key| self.state.responses_sessions.remove_idle(key, self.id)) {
                        break;
                    }
                }
            }
        }
        if let Some(key) = &self.key {
            // Shutdown can race a queued job; removing this actor's entry must not remove a replacement.
            let mut cache = self
                .state
                .responses_sessions
                .0
                .lock()
                .expect("Responses session cache poisoned");
            if cache
                .entries
                .get(key)
                .is_some_and(|entry| entry.id == self.id)
            {
                cache.entries.remove(key);
            }
        }
    }

    async fn respond(&mut self, mut job: Job) -> TurnEnd {
        let shutdown = self.state.shutdown.clone();
        let prepared = tokio::select! {
            biased;
            _ = shutdown.cancelled() => Err(ApiError::shutdown()),
            _ = job.reply.closed() => { self.upstream = None; return TurnEnd::Canceled; }
            result = self.prepare(&mut job.turn) => result,
        };
        let (input, parameters) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                if error.status != StatusCode::BAD_REQUEST {
                    self.upstream = None;
                }
                let status = if error.status == StatusCode::BAD_REQUEST {
                    FinalStatus::Rejected
                } else if shutdown.is_cancelled() {
                    FinalStatus::Canceled
                } else {
                    FinalStatus::UpstreamError
                };
                let _ = job
                    .turn
                    .pending
                    .finish(status, Some(error.status), None)
                    .await;
                return TurnEnd::Error {
                    reply: job.reply,
                    error,
                };
            }
        };
        let (sender, receiver) = mpsc::channel(16);
        job.turn.pending.response_started(StatusCode::OK);
        if job.reply.send(Ok(receiver)).is_err() {
            self.upstream = None;
            return TurnEnd::Canceled;
        }
        let terminal = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            _ = sender.closed() => None,
            terminal = forward_websocket_responses_stream(
            &mut job.turn.pending,
            job.turn.rates,
            self.upstream.as_mut().expect("prepared connection"),
            &sender,
            shutdown.clone(),
            ) => terminal,
        };
        let Some((terminal, bytes)) = terminal else {
            self.upstream = None;
            return TurnEnd::Canceled;
        };
        let permit = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            permit = sender.reserve_owned() => permit.ok(),
        };
        let Some(permit) = permit else {
            self.upstream = None;
            return TurnEnd::Canceled;
        };
        if self.key.is_some()
            && terminal.kind == TerminalKind::Completed
            && let Some(response_id) = terminal
                .payload
                .pointer("/response/id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            && let Some(output) = terminal
                .payload
                .pointer("/response/output")
                .and_then(Value::as_array)
        {
            let mut input = input;
            input.extend_from_slice(output);
            let snapshot = Snapshot {
                response_id: response_id.to_owned(),
                input,
                parameters,
            };
            self.bytes = serde_json::to_vec(&snapshot)
                .expect("JSON snapshot serializes")
                .len();
            self.upstream_response_id = Some(snapshot.response_id.clone());
            self.snapshot = Some(snapshot);
        } else {
            self.upstream = None;
        }
        TurnEnd::Terminal(permit, bytes)
    }

    async fn prepare(&mut self, turn: &mut Turn) -> Result<(Vec<Value>, Value), ApiError> {
        let previous_id = match turn.body.get("previous_response_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(id)) if !id.is_empty() => Some(id.clone()),
            _ => {
                return Err(ApiError::invalid(
                    "previous_response_id",
                    "previous_response_id must be a nonempty string or null",
                ));
            }
        };
        let mut input = match turn.body.get("input") {
            None => Vec::new(),
            Some(Value::Array(input)) => input.clone(),
            _ => {
                return Err(ApiError::invalid(
                    "input",
                    "input must be a string or an array",
                ));
            }
        };
        if let Some(previous_id) = &previous_id {
            if self.key.is_none() {
                return Err(ApiError::invalid(
                    "previous_response_id",
                    "Provide thread_id or session_id and resend the full history to start a session",
                ));
            }
            let snapshot = self.snapshot.as_ref().filter(|snapshot| snapshot.response_id == *previous_id)
                .ok_or_else(|| ApiError::invalid("previous_response_id", "The response is not the latest cached response in this session; resend the full history without previous_response_id"))?;
            let mut history = snapshot.input.clone();
            history.append(&mut input);
            input = history;
        }
        turn.body
            .as_object_mut()
            .expect("normalized object")
            .remove("previous_response_id");
        let mut headers = codex_request_headers(&turn.headers, &turn.body);
        let metadata = turn
            .body
            .as_object_mut()
            .expect("normalized object")
            .entry("client_metadata")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| {
                ApiError::invalid("client_metadata", "client_metadata must be an object")
            })?;
        for name in [
            "session_id",
            "thread_id",
            "x-codex-turn-state",
            "x-codex-turn-metadata",
            "x-codex-window-id",
            "x-codex-parent-thread-id",
            "x-openai-subagent",
            "x-openai-memgen-request",
            "x-client-request-id",
        ] {
            if let Some(value) = headers.get(name).cloned() {
                let value = std::str::from_utf8(value.as_bytes())
                    .map_err(|_| ApiError::invalid(name, "request metadata must be text"))?;
                metadata.insert(name.to_owned(), Value::String(value.to_owned()));
                if !matches!(name, "session_id" | "thread_id") {
                    headers.remove(name);
                }
            }
        }
        let mut prewarm = responses_lite_prewarm(&mut turn.body, &turn.model, &self.request_key)
            .map_err(|_| ApiError::gateway("Failed to prepare the upstream Responses session"))?;
        let mut properties = turn.body.as_object().expect("normalized object").clone();
        properties.remove("input");
        properties.remove("client_metadata");
        let parameters = json!([properties, prewarm["input"]]);
        let connection_reused = self.upstream.is_some() && self.handshake_headers == headers;
        if !connection_reused {
            self.upstream = Some(
                connect_upstream_websocket(
                    &self.state.config.upstream.base_url,
                    Arc::clone(&self.state.credentials),
                    &headers,
                )
                .await
                .map_err(|_| ApiError::gateway("Failed to connect to the upstream WebSocket"))?,
            );
            self.handshake_headers = headers;
            self.upstream_response_id = None;
        }
        let incremental = self.snapshot.as_ref().filter(|snapshot| {
            self.upstream_response_id.as_ref() == Some(&snapshot.response_id)
                && snapshot.parameters == parameters
                && input.starts_with(&snapshot.input)
        });
        let context_reused = incremental.is_some();
        if let Some(snapshot) = incremental {
            turn.body["previous_response_id"] = Value::String(snapshot.response_id.clone());
            turn.body["input"] = Value::Array(input[snapshot.input.len()..].to_vec());
        } else {
            // A fresh warmup starts a new context even if the transport is still healthy.
            prewarm
                .as_object_mut()
                .expect("prewarm object")
                .remove("previous_response_id");
            self.upstream_response_id = None;
            let response_id =
                send_responses_prewarm(self.upstream.as_mut().expect("connected socket"), &prewarm)
                    .await
                    .map_err(|_| {
                        ApiError::gateway("Failed to prepare the upstream Responses session")
                    })?;
            turn.body["previous_response_id"] = Value::String(response_id);
            turn.body["input"] = Value::Array(input.clone());
        }
        tracing::debug!(request_key = %turn.request_key, connection_reused, context_reused,
            "Prepared Responses WebSocket");
        self.upstream
            .as_mut()
            .expect("connected socket")
            .send(Message::Text(turn.body.to_string().into()))
            .await
            .map_err(|_| ApiError::gateway("Failed to send the upstream WebSocket request"))?;
        Ok((input, parameters))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_budget_evicts_idle_history_without_evicting_active_turns() {
        let sessions = ResponsesSessions::default();
        let keys: Vec<_> = ["active", "old-idle", "finishing"]
            .into_iter()
            .map(|value| SessionKey {
                client: "client".to_owned(),
                header: "thread_id",
                value: value.to_owned(),
            })
            .collect();
        {
            let mut cache = sessions.0.lock().unwrap();
            for (id, key) in keys.iter().enumerate() {
                cache.entries.insert(
                    key.clone(),
                    Entry {
                        id: id as u64,
                        sender: mpsc::channel(1).0,
                        busy: id != 1,
                        last_used: Instant::now() - Duration::from_secs(3 - id as u64),
                        bytes: MAX_CONTEXT_BYTES / 2,
                    },
                );
            }
        }
        sessions.ready(&keys[2], 2, MAX_CONTEXT_BYTES / 2);
        let cache = sessions.0.lock().unwrap();
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.entries[&keys[0]].busy);
        assert!(!cache.entries.contains_key(&keys[1]));
        assert!(!cache.entries[&keys[2]].busy);
    }

    #[test]
    fn oversized_snapshot_does_not_flush_other_sessions_or_remove_a_replacement_actor() {
        let sessions = ResponsesSessions::default();
        let keys: Vec<_> = ["small", "large"]
            .into_iter()
            .map(|value| SessionKey {
                client: "client".to_owned(),
                header: "thread_id",
                value: value.to_owned(),
            })
            .collect();
        {
            let mut cache = sessions.0.lock().unwrap();
            for (id, key) in keys.iter().enumerate() {
                cache.entries.insert(
                    key.clone(),
                    Entry {
                        id: id as u64,
                        sender: mpsc::channel(1).0,
                        busy: id == 1,
                        last_used: Instant::now(),
                        bytes: 1,
                    },
                );
            }
        }
        sessions.ready(&keys[1], 1, MAX_CONTEXT_BYTES + 1);
        assert_eq!(sessions.0.lock().unwrap().entries.len(), 1);
        assert!(sessions.0.lock().unwrap().entries.contains_key(&keys[0]));
        // A stale actor cannot release or remove a new request for the same key.
        sessions
            .0
            .lock()
            .unwrap()
            .entries
            .get_mut(&keys[0])
            .unwrap()
            .busy = true;
        sessions.ready(&keys[0], 99, MAX_CONTEXT_BYTES + 1);
        assert!(sessions.remove_idle(&keys[0], 99));
        assert!(sessions.0.lock().unwrap().entries[&keys[0]].busy);
        assert!(!sessions.remove_idle(&keys[0], 0));
    }
}
