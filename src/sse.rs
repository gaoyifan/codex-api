//! Incremental parsing and validation for upstream Responses SSE streams.
//!
//! The reader directly owns the upstream byte stream. Dropping it propagates
//! cancellation without an intermediate task or channel.

use std::pin::Pin;

use eventsource_stream::{EventStream, Eventsource};
use futures_util::{Stream, StreamExt};
use serde_json::Value;

use crate::responses_terminal::{TerminalEvent, TerminalKind, parse_terminal_payload};

pub(crate) enum SseEvent {
    Event(Value),
    Terminal(TerminalEvent),
}

pub(crate) struct SseReader<S> {
    events: Pin<Box<EventStream<S>>>,
}

impl<S> SseReader<S> {
    pub(crate) fn new<B, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        Self {
            events: Box::pin(stream.eventsource()),
        }
    }

    /// Returns decoded events until EOF or a transport/protocol failure.
    /// Non-JSON informational records are ignored; malformed terminals end the stream.
    pub(crate) async fn next<B, E>(&mut self) -> Option<SseEvent>
    where
        S: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        loop {
            let event = self.events.next().await?.ok()?;
            let event_kind = TerminalKind::from_name(&event.event);
            let payload: Value = match serde_json::from_str(&event.data) {
                Ok(payload) => payload,
                Err(_) if event_kind.is_none() => continue,
                Err(_) => return None,
            };
            let payload_kind = payload
                .get("type")
                .and_then(Value::as_str)
                .and_then(TerminalKind::from_name);
            let Some(payload_kind) = payload_kind else {
                return event_kind.is_none().then_some(SseEvent::Event(payload));
            };
            let terminal = parse_terminal_payload(payload).ok()??;
            if event_kind.is_some_and(|kind| kind != payload_kind) {
                return None;
            }
            return Some(SseEvent::Terminal(terminal));
        }
    }
}
