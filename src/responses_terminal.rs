//! Terminal Responses validation and client-facing capacity retry advice.

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

impl TerminalEvent {
    pub(crate) fn upstream_error(&self) -> Option<&Value> {
        match self.kind {
            TerminalKind::Error => self.payload.get("error"),
            TerminalKind::Failed => self.payload.pointer("/response/error"),
            TerminalKind::Completed | TerminalKind::Incomplete => None,
        }
        .filter(|error| error.is_object())
    }

    /// Makes capacity failures actionable by Responses clients without changing accounting.
    pub(crate) fn retryable_capacity_payload(&self) -> Option<Value> {
        let error = self.upstream_error()?;
        if !matches!(
            error["code"].as_str(),
            Some("server_is_overloaded" | "slow_down")
        ) {
            return None;
        }

        // Both streamed and wrapped WebSocket failures accept nested retry advice.
        // Preserve valid upstream advice, including zero seconds and HTTP dates.
        let retry_after = [&error["headers"], &self.payload["headers"]]
            .into_iter()
            .filter_map(Value::as_object)
            .flat_map(|headers| headers.iter())
            .find_map(|(name, value)| {
                if !name.eq_ignore_ascii_case("retry-after") {
                    return None;
                }
                let valid = match value {
                    Value::String(value) => {
                        let value = value.trim();
                        (!value.is_empty()
                            && value.bytes().all(|byte| byte.is_ascii_digit())
                            && value.parse::<u64>().is_ok())
                            || httpdate::parse_http_date(value).is_ok()
                    }
                    Value::Number(value) => value.as_u64().is_some(),
                    _ => false,
                };
                valid.then(|| value.clone())
            })
            .unwrap_or_else(|| Value::String("30".to_owned()));

        let mut error = error.clone();
        let mut headers = error["headers"].as_object().cloned().unwrap_or_default();
        headers.retain(|name, _| !name.eq_ignore_ascii_case("retry-after"));
        headers.insert("Retry-After".to_owned(), retry_after);
        error["headers"] = Value::Object(headers);

        let mut payload = self.payload.clone();
        if self.kind == TerminalKind::Error {
            // A bare `error` without an HTTP status is otherwise ignored by Codex.
            payload.as_object_mut()?.remove("error");
            payload["type"] = Value::String("response.failed".to_owned());
            payload["response"] = serde_json::json!({"status": "failed", "error": error});
        } else {
            payload["response"]["error"] = error;
        }
        Some(payload)
    }
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

    // Log only failure metadata, never the surrounding response or its output.
    if matches!(kind, TerminalKind::Error | TerminalKind::Failed) {
        let error = if kind == TerminalKind::Error {
            &payload["error"]
        } else {
            &payload["response"]["error"]
        };
        tracing::warn!(
            event = kind.event_name(),
            error_code = error["code"].as_str(),
            error_type = error["type"].as_str(),
            error_message = error["message"].as_str(),
            response_id = payload
                .pointer("/response/id")
                .and_then(|value| value.as_str()),
            "Upstream Responses request failed"
        );
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn capacity_retry_advice_preserves_valid_headers_and_replaces_invalid_values() {
        for (value, expected) in [
            (json!("7"), json!("7")),
            (json!(0), json!(0)),
            (json!("0"), json!("0")),
            (
                json!("Wed, 21 Oct 2037 07:28:00 GMT"),
                json!("Wed, 21 Oct 2037 07:28:00 GMT"),
            ),
            (Value::Null, json!("30")),
            (json!("invalid"), json!("30")),
            (json!("+1"), json!("30")),
            (json!(-1), json!("30")),
        ] {
            let original = json!({
                "type": "response.failed",
                "response": {"id": "resp-failed", "status": "failed", "error": {
                    "code": "server_is_overloaded", "message": "At capacity.",
                    "headers": {"retry-after": value, "x-request-id": "upstream-request"}
                }}
            });
            let terminal = parse_terminal_payload(original.clone()).unwrap().unwrap();
            let mut expected_payload = original.clone();
            expected_payload["response"]["error"]["headers"] = json!({
                "Retry-After": expected, "x-request-id": "upstream-request"
            });
            assert_eq!(
                terminal.retryable_capacity_payload(),
                Some(expected_payload)
            );
            assert_eq!(terminal.payload, original);
        }
    }

    #[test]
    fn wrapped_capacity_errors_keep_upstream_advice_and_error_details() {
        for code in ["server_is_overloaded", "slow_down"] {
            let error = json!({"code": code, "type": "service_unavailable_error",
                "message": "At capacity.", "param": null,
                "headers": {"Retry-After": "invalid"}});
            let original = json!({"type": "error", "status": 429, "error": error,
                "headers": {"retry-after": "12"}, "sequence_number": 3});
            let terminal = parse_terminal_payload(original).unwrap().unwrap();
            let mut expected_error = error;
            expected_error["headers"] = json!({"Retry-After": "12"});
            assert_eq!(
                terminal.retryable_capacity_payload(),
                Some(json!({
                    "type": "response.failed", "status": 429,
                    "headers": {"retry-after": "12"}, "sequence_number": 3,
                    "response": {"status": "failed", "error": expected_error}
                }))
            );
        }
    }

    #[test]
    fn unrelated_upstream_errors_are_not_rewritten() {
        let terminal = parse_terminal_payload(json!({"type": "error", "error": {
            "code": "insufficient_quota", "message": "Quota exhausted."
        }}))
        .unwrap()
        .unwrap();
        assert_eq!(terminal.retryable_capacity_payload(), None);
    }
}
