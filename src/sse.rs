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
    /// The byte stream ended. `terminal_seen` distinguishes a normal end after
    /// a terminal record from an incomplete upstream response.
    Eof {
        terminal_seen: bool,
    },
}

/// Terminal Responses event types understood by the relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalKind {
    Completed,
    Incomplete,
    Failed,
}

impl TerminalKind {
    pub fn event_name(self) -> &'static str {
        match self {
            Self::Completed => "response.completed",
            Self::Incomplete => "response.incomplete",
            Self::Failed => "response.failed",
        }
    }

    pub fn response_status(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Incomplete => "incomplete",
            Self::Failed => "failed",
        }
    }

    fn from_name(value: &str) -> Option<Self> {
        match value {
            "response.completed" => Some(Self::Completed),
            "response.incomplete" => Some(Self::Incomplete),
            "response.failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Validated information extracted from a terminal event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalEvent {
    pub kind: TerminalKind,
    /// The parsed terminal SSE data object used by protocol conversion.
    pub payload: Value,
    pub usage: Option<Usage>,
}

/// Token accounting required by pricing and Chat usage conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Usage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub total_tokens: u64,
}

/// A streaming parser over an upstream byte stream.
pub struct SseReader<S> {
    events: Pin<Box<EventStream<S>>>,
    terminal_seen: bool,
}

impl<S> SseReader<S> {
    pub fn new<B, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        Self {
            events: Box::pin(stream.eventsource()),
            terminal_seen: false,
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
                let terminal = validate_terminal(&event)?;
                if terminal.is_some() {
                    self.terminal_seen = true;
                }
                Ok(SseRead::Event(SseEvent {
                    event: event.event,
                    id: event.id,
                    data: event.data,
                    retry: event.retry,
                    terminal,
                }))
            }
            Some(Err(error)) => Err(SseReadError::Stream(error)),
            None => Ok(SseRead::Eof {
                terminal_seen: self.terminal_seen,
            }),
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
    #[error("terminal SSE event `{event}` has response status `{actual}`; expected `{expected}`")]
    TerminalStatusMismatch {
        event: &'static str,
        actual: String,
        expected: &'static str,
    },
    #[error("terminal SSE event `{event}` reports more cached input tokens than input tokens")]
    CachedInputExceedsInput { event: &'static str },
    #[error("terminal SSE event `{event}` reports more reasoning tokens than output tokens")]
    ReasoningExceedsOutput { event: &'static str },
}

fn validate_terminal(event: &Event) -> Result<Option<TerminalEvent>, ProtocolError> {
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

    let payload_type = payload.get("type").and_then(Value::as_str);
    let payload_kind = payload_type.and_then(TerminalKind::from_name);
    let kind = match (event_kind, payload_kind) {
        (None, None) => return Ok(None),
        (Some(event_kind), Some(payload_kind)) if event_kind != payload_kind => {
            return Err(ProtocolError::TerminalTypeMismatch {
                event_name: event_kind.event_name(),
                payload_type: payload_kind.event_name(),
            });
        }
        (Some(event_kind), Some(_)) => event_kind,
        (Some(event_kind), None) => {
            return Err(ProtocolError::InvalidTerminalField {
                event: event_kind.event_name().to_owned(),
                field: "type",
                expected: event_kind.event_name(),
            });
        }
        (None, Some(payload_kind)) => payload_kind,
    };

    let response = payload
        .get("response")
        .filter(|value| value.is_object())
        .ok_or_else(|| ProtocolError::InvalidTerminalField {
            event: kind.event_name().to_owned(),
            field: "response",
            expected: "an object",
        })?;
    let status = response
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| ProtocolError::InvalidTerminalField {
            event: kind.event_name().to_owned(),
            field: "response.status",
            expected: "a string",
        })?;
    if status != kind.response_status() {
        return Err(ProtocolError::TerminalStatusMismatch {
            event: kind.event_name(),
            actual: status.to_owned(),
            expected: kind.response_status(),
        });
    }

    let usage = parse_usage(response, kind)?;
    Ok(Some(TerminalEvent {
        kind,
        payload,
        usage,
    }))
}

fn parse_usage(response: &Value, kind: TerminalKind) -> Result<Option<Usage>, ProtocolError> {
    let Some(usage) = response.get("usage").filter(|usage| !usage.is_null()) else {
        return match kind {
            TerminalKind::Failed => Ok(None),
            TerminalKind::Completed | TerminalKind::Incomplete => {
                Err(ProtocolError::InvalidTerminalField {
                    event: kind.event_name().to_owned(),
                    field: "response.usage",
                    expected: "an object",
                })
            }
        };
    };
    if !usage.is_object() {
        return Err(ProtocolError::InvalidTerminalField {
            event: kind.event_name().to_owned(),
            field: "response.usage",
            expected: "an object or null",
        });
    }

    let input_tokens = required_u64(usage, "input_tokens", "response.usage.input_tokens", kind)?;
    let output_tokens = required_u64(usage, "output_tokens", "response.usage.output_tokens", kind)?;
    let total_tokens = required_u64(usage, "total_tokens", "response.usage.total_tokens", kind)?;
    let cached_input_tokens = optional_detail_u64(
        usage,
        "input_tokens_details",
        "response.usage.input_tokens_details",
        "cached_tokens",
        "response.usage.input_tokens_details.cached_tokens",
        kind,
    )?;
    let reasoning_tokens = optional_detail_u64(
        usage,
        "output_tokens_details",
        "response.usage.output_tokens_details",
        "reasoning_tokens",
        "response.usage.output_tokens_details.reasoning_tokens",
        kind,
    )?;

    if cached_input_tokens > input_tokens {
        return Err(ProtocolError::CachedInputExceedsInput {
            event: kind.event_name(),
        });
    }
    if reasoning_tokens > output_tokens {
        return Err(ProtocolError::ReasoningExceedsOutput {
            event: kind.event_name(),
        });
    }
    Ok(Some(Usage {
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
    }))
}

fn required_u64(
    object: &Value,
    field: &'static str,
    field_path: &'static str,
    kind: TerminalKind,
) -> Result<u64, ProtocolError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| ProtocolError::InvalidTerminalField {
            event: kind.event_name().to_owned(),
            field: field_path,
            expected: "a non-negative integer",
        })
}

fn optional_detail_u64(
    usage: &Value,
    details_field: &'static str,
    details_path: &'static str,
    token_field: &'static str,
    token_path: &'static str,
    kind: TerminalKind,
) -> Result<u64, ProtocolError> {
    let Some(details) = usage
        .get(details_field)
        .filter(|details| !details.is_null())
    else {
        return Ok(0);
    };
    let details = details
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidTerminalField {
            event: kind.event_name().to_owned(),
            field: details_path,
            expected: "an object",
        })?;
    let Some(value) = details.get(token_field) else {
        return Ok(0);
    };
    value
        .as_u64()
        .ok_or_else(|| ProtocolError::InvalidTerminalField {
            event: kind.event_name().to_owned(),
            field: token_path,
            expected: "a non-negative integer",
        })
}
