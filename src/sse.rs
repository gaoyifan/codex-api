//! Incremental parsing and validation for upstream Responses SSE streams.
//!
//! The reader owns no task or channel: each call to [`SseReader::next`] polls
//! the upstream byte stream directly. Dropping the reader therefore drops the
//! upstream stream, which lets downstream cancellation propagate naturally.

use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use eventsource_stream::{Event, EventStream, EventStreamError, Eventsource};
use futures_util::{Stream, StreamExt};
use serde_json::Value;
use thiserror::Error;

use crate::responses_terminal::{
    ProtocolError as TerminalProtocolError, TerminalEvent, TerminalKind, parse_terminal_payload,
};

/// One parsed SSE record, retaining the upstream event semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SseEvent {
    pub event: String,
    pub id: String,
    pub data: String,
    pub retry: Option<Duration>,
    terminal: Option<TerminalEvent>,
}

impl SseEvent {
    /// Returns validated terminal metadata when this record ends a response.
    pub fn terminal(&self) -> Option<&TerminalEvent> {
        self.terminal.as_ref()
    }

    /// Consumes this record and returns its validated terminal payload.
    pub fn into_terminal(self) -> Option<TerminalEvent> {
        self.terminal
    }

    /// Encodes this record using canonical LF-delimited SSE framing.
    ///
    /// The effective event ID is always emitted, including an empty ID. This
    /// preserves both inherited IDs and the SSE operation that resets an ID.
    /// Every logical data line receives its own `data:` prefix.
    pub fn canonical_bytes(&self) -> Bytes {
        let mut encoded = String::new();

        encoded.push_str("id: ");
        encoded.push_str(&self.id);
        encoded.push('\n');
        encoded.push_str("event: ");
        encoded.push_str(&self.event);
        encoded.push('\n');
        if let Some(retry) = self.retry {
            encoded.push_str("retry: ");
            encoded.push_str(&retry.as_millis().to_string());
            encoded.push('\n');
        }
        for line in self.data.split('\n') {
            encoded.push_str("data: ");
            encoded.push_str(line);
            encoded.push('\n');
        }
        encoded.push('\n');

        Bytes::from(encoded)
    }
}

/// The result of advancing an upstream SSE stream once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SseRead {
    Event(SseEvent),
    Eof,
}

/// A streaming parser over an upstream byte stream.
pub struct SseReader<S> {
    events: Pin<Box<EventStream<S>>>,
}

impl<S> SseReader<S> {
    pub fn new<B, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        Self {
            events: Box::pin(stream.eventsource()),
        }
    }

    /// Reads exactly one parsed record, or reports the end of the byte stream.
    ///
    /// Informational records are not required to contain JSON. A record is
    /// terminal when either its SSE event name or its JSON `type` says so. A
    /// terminal record is returned only after its type, response status, and
    /// usage have all been validated.
    pub async fn next<B, E>(&mut self) -> Result<SseRead, SseReadError<E>>
    where
        S: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        match self.events.next().await {
            Some(Ok(event)) => {
                let terminal = validate_event(&event)?;
                Ok(SseRead::Event(SseEvent {
                    event: event.event,
                    id: event.id,
                    data: event.data,
                    retry: event.retry,
                    terminal,
                }))
            }
            Some(Err(error)) => Err(SseReadError::Stream(error)),
            None => Ok(SseRead::Eof),
        }
    }
}

/// Errors surfaced while consuming the upstream event stream.
#[derive(Debug)]
pub enum SseReadError<E> {
    Stream(EventStreamError<E>),
    Protocol(ProtocolError),
}

impl<E> From<ProtocolError> for SseReadError<E> {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl<E> fmt::Display for SseReadError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(error) => write!(formatter, "upstream SSE stream error: {error}"),
            Self::Protocol(error) => error.fmt(formatter),
        }
    }
}

impl<E> std::error::Error for SseReadError<E> where
    E: fmt::Debug + fmt::Display + Send + Sync + 'static
{
}

/// A terminal Responses record violated the required wire contract.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("terminal SSE event `{event}` contains invalid JSON")]
    InvalidTerminalJson {
        event: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("terminal SSE event `{event}` has invalid `{field}`; expected {expected}")]
    InvalidTerminalField {
        event: String,
        field: &'static str,
        expected: &'static str,
    },
    #[error("terminal SSE event name `{event_name}` disagrees with JSON type `{payload_type}`")]
    TerminalTypeMismatch {
        event_name: &'static str,
        payload_type: &'static str,
    },
    #[error(transparent)]
    Terminal(#[from] TerminalProtocolError),
}

fn validate_event(event: &Event) -> Result<Option<TerminalEvent>, ProtocolError> {
    let event_kind = TerminalKind::from_name(&event.event);
    let payload = match serde_json::from_str::<Value>(&event.data) {
        Ok(payload) => payload,
        Err(_source) if event_kind.is_none() => return Ok(None),
        Err(source) => {
            return Err(ProtocolError::InvalidTerminalJson {
                event: event.event.clone(),
                source,
            });
        }
    };

    let terminal = parse_terminal_payload(payload)?;
    let payload_kind = terminal.as_ref().map(|terminal| terminal.kind);
    match (event_kind, payload_kind) {
        (None, None) => Ok(None),
        (Some(event_kind), Some(payload_kind)) if event_kind != payload_kind => {
            Err(ProtocolError::TerminalTypeMismatch {
                event_name: event_kind.event_name(),
                payload_type: payload_kind.event_name(),
            })
        }
        (Some(_), Some(_)) | (None, Some(_)) => Ok(terminal),
        (Some(event_kind), None) => Err(ProtocolError::InvalidTerminalField {
            event: event_kind.event_name().to_owned(),
            field: "type",
            expected: event_kind.event_name(),
        }),
    }
}
