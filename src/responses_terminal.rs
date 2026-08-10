//! Transport-independent validation of terminal Responses event payloads.

use serde_json::Value;
use thiserror::Error;

/// Terminal Responses event types understood by the relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalKind {
    Completed,
    Incomplete,
    Failed,
    Error,
}

impl TerminalKind {
    pub(crate) fn event_name(self) -> &'static str {
        match self {
            Self::Completed => "response.completed",
            Self::Incomplete => "response.incomplete",
            Self::Failed => "response.failed",
            Self::Error => "error",
        }
    }

    pub(crate) fn from_name(value: &str) -> Option<Self> {
        match value {
            "response.completed" => Some(Self::Completed),
            "response.incomplete" => Some(Self::Incomplete),
            "response.failed" => Some(Self::Failed),
            "error" => Some(Self::Error),
            _ => None,
        }
    }

    fn response_status(self) -> Option<&'static str> {
        match self {
            Self::Completed => Some("completed"),
            Self::Incomplete => Some("incomplete"),
            Self::Failed => Some("failed"),
            Self::Error => None,
        }
    }
}

/// Validated information extracted from a terminal Responses payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TerminalEvent {
    pub(crate) kind: TerminalKind,
    pub(crate) payload: Value,
    pub(crate) usage: Option<Usage>,
}

/// Token accounting and Chat usage fields from a terminal response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Usage {
    pub(crate) input_tokens: u64,
    pub(crate) cached_input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) reasoning_tokens: u64,
    pub(crate) total_tokens: u64,
}

/// A terminal Responses payload violated the upstream wire contract.
#[derive(Debug, Error)]
pub(crate) enum ProtocolError {
    #[error("terminal Responses event `{event}` has invalid `{field}`; expected {expected}")]
    InvalidTerminalField {
        event: &'static str,
        field: &'static str,
        expected: &'static str,
    },
    #[error(
        "terminal Responses event `{event}` has response status `{actual}`; expected `{expected}`"
    )]
    TerminalStatusMismatch {
        event: &'static str,
        actual: String,
        expected: &'static str,
    },
    #[error(
        "terminal Responses event `{event}` reports more cached input tokens than input tokens"
    )]
    CachedInputExceedsInput { event: &'static str },
    #[error("terminal Responses event `{event}` reports more reasoning tokens than output tokens")]
    ReasoningExceedsOutput { event: &'static str },
}

/// Classifies and validates one already-decoded Responses event payload.
pub(crate) fn parse_terminal_payload(
    payload: Value,
) -> Result<Option<TerminalEvent>, ProtocolError> {
    let Some(kind) = payload
        .get("type")
        .and_then(Value::as_str)
        .and_then(TerminalKind::from_name)
    else {
        return Ok(None);
    };

    if kind == TerminalKind::Error {
        return Ok(Some(TerminalEvent {
            kind,
            payload,
            usage: None,
        }));
    }

    let response = payload
        .get("response")
        .filter(|value| value.is_object())
        .ok_or(ProtocolError::InvalidTerminalField {
            event: kind.event_name(),
            field: "response",
            expected: "an object",
        })?;
    let expected_status = kind
        .response_status()
        .expect("only response terminal kinds reach status validation");
    let status = response.get("status").and_then(Value::as_str).ok_or(
        ProtocolError::InvalidTerminalField {
            event: kind.event_name(),
            field: "response.status",
            expected: "a string",
        },
    )?;
    if status != expected_status {
        return Err(ProtocolError::TerminalStatusMismatch {
            event: kind.event_name(),
            actual: status.to_owned(),
            expected: expected_status,
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
                    event: kind.event_name(),
                    field: "response.usage",
                    expected: "an object",
                })
            }
            TerminalKind::Error => unreachable!("error events do not contain response usage"),
        };
    };
    if !usage.is_object() {
        return Err(ProtocolError::InvalidTerminalField {
            event: kind.event_name(),
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
        .ok_or(ProtocolError::InvalidTerminalField {
            event: kind.event_name(),
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
        .ok_or(ProtocolError::InvalidTerminalField {
            event: kind.event_name(),
            field: details_path,
            expected: "an object",
        })?;
    let Some(value) = details.get(token_field) else {
        return Ok(0);
    };
    value.as_u64().ok_or(ProtocolError::InvalidTerminalField {
        event: kind.event_name(),
        field: token_path,
        expected: "a non-negative integer",
    })
}
