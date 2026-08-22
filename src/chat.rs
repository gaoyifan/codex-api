//! Strict, transport-independent Chat Completions compatibility.
//!
//! This module owns the deliberately small public Chat request subset and the
//! conversion of a terminal Responses event back into one non-streaming Chat
//! Completion. It does not perform model admission, I/O, retries, accounting,
//! or persistence.

use std::{error::Error, fmt};

use serde_json::{Map, Value, json};

use crate::responses_terminal::{TerminalEvent, TerminalKind, Usage};

/// Identifies whether an error belongs to the downstream request or to the
/// upstream Responses protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatErrorKind {
    InvalidRequest,
    UpstreamProtocol,
}

/// A compact protocol error which the HTTP layer can map to either 400 or 502.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatError {
    pub kind: ChatErrorKind,
    pub param: Option<String>,
    pub message: String,
}

impl ChatError {
    fn invalid(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ChatErrorKind::InvalidRequest,
            param: Some(param.into()),
            message: message.into(),
        }
    }

    fn upstream(message: impl Into<String>) -> Self {
        Self {
            kind: ChatErrorKind::UpstreamProtocol,
            param: None,
            message: message.into(),
        }
    }
}

impl fmt::Display for ChatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ChatError {}

/// A validated Chat request and its Responses wire representation.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvertedRequest {
    pub upstream_request: Value,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub stream: bool,
    pub include_usage: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStatus {
    Completed,
    Incomplete,
}

pub struct ChatStreamConverter {
    id: Option<String>,
    created: Option<i64>,
    model: Option<String>,
    include_usage: bool,
}

pub struct ConvertedStreamTerminal {
    pub chunks: Vec<Value>,
    pub usage: Usage,
    pub status: TerminalStatus,
}

impl ChatStreamConverter {
    pub fn new(include_usage: bool) -> Self {
        Self {
            id: None,
            created: None,
            model: None,
            include_usage,
        }
    }

    pub fn convert_event(&mut self, event: &Value) -> Result<Option<Value>, ChatError> {
        match event.get("type").and_then(Value::as_str) {
            Some("response.created") => {
                let response = event
                    .get("response")
                    .and_then(Value::as_object)
                    .ok_or_else(|| ChatError::upstream("response.created is missing response"))?;
                self.capture_metadata(response)?;
                Ok(Some(self.chunk(
                    json!({"role": "assistant", "content": ""}),
                    None,
                )?))
            }
            Some("response.output_text.delta") => {
                let delta = event.get("delta").and_then(Value::as_str).ok_or_else(|| {
                    ChatError::upstream("response.output_text.delta.delta must be a string")
                })?;
                Ok(Some(self.chunk(json!({"content": delta}), None)?))
            }
            _ => Ok(None),
        }
    }

    pub fn convert_terminal(
        &mut self,
        terminal: &TerminalEvent,
    ) -> Result<ConvertedStreamTerminal, ChatError> {
        let status = match terminal.kind {
            TerminalKind::Completed => TerminalStatus::Completed,
            TerminalKind::Incomplete => TerminalStatus::Incomplete,
            TerminalKind::Failed | TerminalKind::Error => {
                return Err(ChatError::upstream(format!(
                    "upstream emitted {} instead of a completion",
                    terminal.kind.event_name()
                )));
            }
        };
        let response = terminal
            .payload
            .get("response")
            .and_then(Value::as_object)
            .ok_or_else(|| ChatError::upstream("terminal event is missing its response object"))?;
        self.capture_metadata(response)?;
        let usage = terminal
            .usage
            .ok_or_else(|| ChatError::upstream("terminal response is missing usage"))?;
        let finish_reason = match status {
            TerminalStatus::Completed => "stop",
            TerminalStatus::Incomplete => incomplete_finish_reason(response)?,
        };
        let mut chunks = vec![self.chunk(json!({}), Some(finish_reason))?];
        if self.include_usage {
            chunks.push(self.usage_chunk(usage)?);
        }
        Ok(ConvertedStreamTerminal {
            chunks,
            usage,
            status,
        })
    }

    fn capture_metadata(&mut self, response: &Map<String, Value>) -> Result<(), ChatError> {
        self.id = Some(upstream_string(response, "id", "response")?.to_owned());
        self.created = Some(upstream_integer(response, "created_at", "response")?);
        self.model = Some(upstream_string(response, "model", "response")?.to_owned());
        Ok(())
    }

    fn chunk(&self, delta: Value, finish_reason: Option<&str>) -> Result<Value, ChatError> {
        let (id, created, model) = self.metadata()?;
        Ok(json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "logprobs": null,
                "finish_reason": finish_reason
            }]
        }))
    }

    fn usage_chunk(&self, usage: Usage) -> Result<Value, ChatError> {
        let (id, created, model) = self.metadata()?;
        Ok(json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [],
            "usage": chat_usage(usage)
        }))
    }

    fn metadata(&self) -> Result<(&str, i64, &str), ChatError> {
        match (&self.id, self.created, &self.model) {
            (Some(id), Some(created), Some(model)) => Ok((id, created, model)),
            _ => Err(ChatError::upstream(
                "upstream emitted Chat content before response metadata",
            )),
        }
    }
}

/// The public Chat Completion plus accounting metadata from the same terminal
/// Responses event.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvertedTerminal {
    pub chat_completion: Value,
    pub usage: Usage,
    pub status: TerminalStatus,
}

/// Strictly validates the supported Chat Completions request subset and
/// converts it to a streaming, non-stored Responses request.
pub fn convert_request(request: Value) -> Result<ConvertedRequest, ChatError> {
    let mut request = into_request_object(request, "request")?;
    let model = take_request_string(&mut request, "model", "model")?;
    if model.is_empty() {
        return Err(ChatError::invalid("model", "model must not be empty"));
    }

    let messages = request
        .remove("messages")
        .ok_or_else(|| ChatError::invalid("messages", "messages is required"))?;

    let stream = match request.remove("stream") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        Some(_) => {
            return Err(ChatError::invalid(
                "stream",
                "stream must be a boolean or null",
            ));
        }
    };
    let include_usage = request
        .remove("stream_options")
        .map(|value| convert_stream_options(value, stream))
        .transpose()?
        .unwrap_or(false);

    match request.remove("n") {
        None | Some(Value::Null) => {}
        Some(value) if value.as_u64() == Some(1) => {}
        Some(_) => {
            return Err(ChatError::invalid("n", "n must be one or null"));
        }
    }

    let tools = request.remove("tools").map(convert_tools).transpose()?;
    if stream && tools.is_some() {
        return Err(ChatError::invalid(
            "tools",
            "tools are not supported with streaming Chat Completions",
        ));
    }
    let tool_choice = request
        .remove("tool_choice")
        .map(convert_tool_choice)
        .transpose()?;
    let parallel_tool_calls = request
        .remove("parallel_tool_calls")
        .map(|value| request_bool(value, "parallel_tool_calls"))
        .transpose()?;
    let reasoning_effort = request
        .remove("reasoning_effort")
        .map(convert_reasoning_effort)
        .transpose()?
        .flatten();
    if let Some(value) = request.remove("max_completion_tokens") {
        validate_ignored_completion_limit(value)?;
    }
    let text = request
        .remove("response_format")
        .map(convert_response_format)
        .transpose()?
        .flatten();
    reject_unknown_request_fields(&request, "request")?;
    let (instructions, input) = convert_messages(messages)?;

    let mut upstream = Map::new();
    upstream.insert("model".into(), Value::String(model.clone()));
    upstream.insert("input".into(), Value::Array(input));
    upstream.insert("stream".into(), Value::Bool(true));
    upstream.insert("store".into(), Value::Bool(false));
    if let Some(instructions) = instructions {
        upstream.insert("instructions".into(), Value::String(instructions));
    }
    if let Some(tools) = tools {
        upstream.insert("tools".into(), tools);
    }
    if let Some(tool_choice) = tool_choice {
        upstream.insert("tool_choice".into(), tool_choice);
    }
    if let Some(parallel_tool_calls) = parallel_tool_calls {
        upstream.insert(
            "parallel_tool_calls".into(),
            Value::Bool(parallel_tool_calls),
        );
    }
    if let Some(reasoning_effort) = reasoning_effort.as_ref() {
        upstream.insert("reasoning".into(), json!({"effort": reasoning_effort}));
    }
    if let Some(text) = text {
        upstream.insert("text".into(), text);
    }

    Ok(ConvertedRequest {
        upstream_request: Value::Object(upstream),
        model,
        reasoning_effort,
        stream,
        include_usage,
    })
}

/// Converts a validated completed or incomplete Responses terminal into one
/// non-streaming Chat Completion.
pub fn convert_terminal_event(terminal: &TerminalEvent) -> Result<ConvertedTerminal, ChatError> {
    let terminal_status = match terminal.kind {
        TerminalKind::Completed => TerminalStatus::Completed,
        TerminalKind::Incomplete => TerminalStatus::Incomplete,
        TerminalKind::Failed | TerminalKind::Error => {
            return Err(ChatError::upstream(format!(
                "upstream emitted {} instead of a completion",
                terminal.kind.event_name()
            )));
        }
    };
    let event = terminal
        .payload
        .as_object()
        .ok_or_else(|| ChatError::upstream("terminal Responses event must be an object"))?;

    let response = event
        .get("response")
        .and_then(Value::as_object)
        .ok_or_else(|| ChatError::upstream("terminal event is missing its response object"))?;
    if upstream_string(response, "object", "terminal response")? != "response" {
        return Err(ChatError::upstream(
            "terminal response has an invalid object type",
        ));
    }

    let id = upstream_string(response, "id", "terminal response")?;
    if id.is_empty() {
        return Err(ChatError::upstream(
            "terminal response id must not be empty",
        ));
    }
    let model = upstream_string(response, "model", "terminal response")?;
    if model.is_empty() {
        return Err(ChatError::upstream(
            "terminal response model must not be empty",
        ));
    }
    let created = upstream_integer(response, "created_at", "terminal response")?;
    let usage = terminal
        .usage
        .ok_or_else(|| ChatError::upstream("terminal response is missing usage"))?;
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| ChatError::upstream("terminal response output must be an array"))?;
    let converted_output = convert_output(output)?;

    let finish_reason = match terminal_status {
        TerminalStatus::Completed if !converted_output.tool_calls.is_empty() => "tool_calls",
        TerminalStatus::Completed => "stop",
        TerminalStatus::Incomplete => incomplete_finish_reason(response)?,
    };

    let mut message = Map::new();
    message.insert("role".into(), Value::String("assistant".into()));
    message.insert(
        "content".into(),
        converted_output.text.map_or(Value::Null, Value::String),
    );
    message.insert(
        "refusal".into(),
        converted_output.refusal.map_or(Value::Null, Value::String),
    );
    if !converted_output.tool_calls.is_empty() {
        message.insert(
            "tool_calls".into(),
            Value::Array(converted_output.tool_calls),
        );
    }

    let chat_completion = json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "message": Value::Object(message),
            "logprobs": null,
            "finish_reason": finish_reason
        }],
        "usage": chat_usage(usage)
    });

    Ok(ConvertedTerminal {
        chat_completion,
        usage,
        status: terminal_status,
    })
}

fn chat_usage(usage: Usage) -> Value {
    json!({
        "prompt_tokens": usage.input_tokens,
        "prompt_tokens_details": {
            "cached_tokens": usage.cached_input_tokens
        },
        "completion_tokens": usage.output_tokens,
        "completion_tokens_details": {
            "reasoning_tokens": usage.reasoning_tokens
        },
        "total_tokens": usage.total_tokens
    })
}

fn into_request_object(value: Value, param: &str) -> Result<Map<String, Value>, ChatError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ChatError::invalid(param, format!("{param} must be a JSON object")))
}

fn take_request_string(
    object: &mut Map<String, Value>,
    field: &str,
    param: &str,
) -> Result<String, ChatError> {
    object
        .remove(field)
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| ChatError::invalid(param, format!("{param} must be a string")))
}

fn request_bool(value: Value, param: &str) -> Result<bool, ChatError> {
    value
        .as_bool()
        .ok_or_else(|| ChatError::invalid(param, format!("{param} must be a boolean")))
}

fn convert_stream_options(value: Value, stream: bool) -> Result<bool, ChatError> {
    if !stream {
        return Err(ChatError::invalid(
            "stream_options",
            "stream_options requires stream to be true",
        ));
    }
    let mut options = into_request_object(value, "stream_options")?;
    let include_usage = options
        .remove("include_usage")
        .map(|value| request_bool(value, "stream_options.include_usage"))
        .transpose()?
        .unwrap_or(false);
    reject_unknown_request_fields(&options, "stream_options")?;
    Ok(include_usage)
}

fn validate_ignored_completion_limit(value: Value) -> Result<(), ChatError> {
    if value.is_null() {
        return Ok(());
    }
    if value.as_u64().is_some_and(|limit| limit > 0) {
        return Ok(());
    }
    Err(ChatError::invalid(
        "max_completion_tokens",
        "max_completion_tokens must be a positive integer or null",
    ))
}

fn convert_response_format(value: Value) -> Result<Option<Value>, ChatError> {
    if value.is_null() {
        return Ok(None);
    }
    let mut response_format = into_request_object(value, "response_format")?;
    let format_type = take_request_string(&mut response_format, "type", "response_format.type")?;
    let format = match format_type.as_str() {
        "text" | "json_object" => {
            reject_unknown_request_fields(&response_format, "response_format")?;
            json!({"type": format_type})
        }
        "json_schema" => {
            let schema = response_format.remove("json_schema").ok_or_else(|| {
                ChatError::invalid(
                    "response_format.json_schema",
                    "response_format.json_schema is required",
                )
            })?;
            reject_unknown_request_fields(&response_format, "response_format")?;
            let mut schema = into_request_object(schema, "response_format.json_schema")?;
            schema.insert("type".into(), Value::String("json_schema".into()));
            Value::Object(schema)
        }
        _ => {
            return Err(ChatError::invalid(
                "response_format.type",
                "unsupported response_format.type",
            ));
        }
    };
    Ok(Some(json!({"format": format})))
}

fn reject_unknown_request_fields(
    object: &Map<String, Value>,
    context: &str,
) -> Result<(), ChatError> {
    if let Some(field) = object.keys().next() {
        let param = if context == "request" {
            field.clone()
        } else {
            format!("{context}.{field}")
        };
        return Err(ChatError::invalid(
            &param,
            format!("unsupported field {param}"),
        ));
    }
    Ok(())
}

fn convert_reasoning_effort(value: Value) -> Result<Option<String>, ChatError> {
    if value.is_null() {
        return Ok(None);
    }
    let effort = value.as_str().ok_or_else(|| {
        ChatError::invalid(
            "reasoning_effort",
            "reasoning_effort must be a string or null",
        )
    })?;
    match effort {
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" => {
            Ok(Some(effort.to_owned()))
        }
        _ => Err(ChatError::invalid(
            "reasoning_effort",
            "unsupported reasoning_effort",
        )),
    }
}

fn convert_messages(value: Value) -> Result<(Option<String>, Vec<Value>), ChatError> {
    let messages = value
        .as_array()
        .ok_or_else(|| ChatError::invalid("messages", "messages must be an array"))?;
    if messages.is_empty() {
        return Err(ChatError::invalid(
            "messages",
            "messages must contain at least one message",
        ));
    }

    let mut system_messages = Vec::new();
    let mut input = Vec::new();
    for (index, value) in messages.iter().enumerate() {
        let context = format!("messages[{index}]");
        let mut message = value
            .as_object()
            .cloned()
            .ok_or_else(|| ChatError::invalid(&context, format!("{context} must be an object")))?;
        let role = take_request_string(&mut message, "role", &format!("{context}.role"))?;
        match role.as_str() {
            "system" => {
                let content_param = format!("{context}.content");
                let content = message.remove("content").ok_or_else(|| {
                    ChatError::invalid(&content_param, format!("{content_param} is required"))
                })?;
                reject_unknown_request_fields(&message, &context)?;
                system_messages.push(parse_text_parts(content, &content_param)?.join("\n"));
            }
            "developer" | "user" => {
                let content_param = format!("{context}.content");
                let content = message.remove("content").ok_or_else(|| {
                    ChatError::invalid(&content_param, format!("{content_param} is required"))
                })?;
                reject_unknown_request_fields(&message, &context)?;
                let parts = convert_text_content(content, &content_param, "input_text")?;
                input.push(json!({
                    "type": "message",
                    "role": role,
                    "content": parts
                }));
            }
            "assistant" => convert_assistant_message(message, &context, &mut input)?,
            "tool" => convert_tool_message(message, &context, &mut input)?,
            "function" => {
                return Err(ChatError::invalid(
                    format!("{context}.role"),
                    "legacy function-role messages are not supported",
                ));
            }
            _ => {
                return Err(ChatError::invalid(
                    format!("{context}.role"),
                    format!("unsupported message role {role}"),
                ));
            }
        }
    }
    let instructions = (!system_messages.is_empty()).then(|| system_messages.join("\n"));
    Ok((instructions, input))
}

fn convert_assistant_message(
    mut message: Map<String, Value>,
    context: &str,
    input: &mut Vec<Value>,
) -> Result<(), ChatError> {
    let content = message.remove("content");
    let tool_calls = message.remove("tool_calls");
    reject_unknown_request_fields(&message, context)?;

    let mut contributed_input = false;
    if let Some(content) = content.filter(|content| !content.is_null()) {
        let content_param = format!("{context}.content");
        let parts = convert_text_content(content, &content_param, "output_text")?;
        input.push(json!({
            "type": "message",
            "role": "assistant",
            "content": parts
        }));
        contributed_input = true;
    }

    if let Some(tool_calls) = tool_calls.filter(|tool_calls| !tool_calls.is_null()) {
        let calls = tool_calls.as_array().ok_or_else(|| {
            ChatError::invalid(
                format!("{context}.tool_calls"),
                format!("{context}.tool_calls must be an array"),
            )
        })?;
        if calls.is_empty() {
            return Err(ChatError::invalid(
                format!("{context}.tool_calls"),
                format!("{context}.tool_calls must not be empty"),
            ));
        }
        for (index, call) in calls.iter().enumerate() {
            input.push(convert_assistant_tool_call(
                call,
                &format!("{context}.tool_calls[{index}]"),
            )?);
        }
        contributed_input = true;
    }

    if !contributed_input {
        return Err(ChatError::invalid(
            format!("{context}.content"),
            "assistant message must contain text or function tool calls",
        ));
    }
    Ok(())
}

fn convert_assistant_tool_call(value: &Value, context: &str) -> Result<Value, ChatError> {
    let mut call = value
        .as_object()
        .cloned()
        .ok_or_else(|| ChatError::invalid(context, format!("{context} must be an object")))?;
    let id = take_request_string(&mut call, "id", &format!("{context}.id"))?;
    let call_type = take_request_string(&mut call, "type", &format!("{context}.type"))?;
    if call_type != "function" {
        return Err(ChatError::invalid(
            format!("{context}.type"),
            "only function tool calls are supported",
        ));
    }
    let function_value = call.remove("function").ok_or_else(|| {
        ChatError::invalid(
            format!("{context}.function"),
            format!("{context}.function is required"),
        )
    })?;
    reject_unknown_request_fields(&call, context)?;

    let function_context = format!("{context}.function");
    let mut function = into_request_object(function_value, &function_context)?;
    let name = take_request_string(&mut function, "name", &format!("{function_context}.name"))?;
    let arguments = take_request_string(
        &mut function,
        "arguments",
        &format!("{function_context}.arguments"),
    )?;
    reject_unknown_request_fields(&function, &function_context)?;

    Ok(json!({
        "type": "function_call",
        "call_id": id,
        "name": name,
        "arguments": arguments
    }))
}

fn convert_tool_message(
    mut message: Map<String, Value>,
    context: &str,
    input: &mut Vec<Value>,
) -> Result<(), ChatError> {
    let call_id = take_request_string(
        &mut message,
        "tool_call_id",
        &format!("{context}.tool_call_id"),
    )?;
    let content_param = format!("{context}.content");
    let content = message.remove("content").ok_or_else(|| {
        ChatError::invalid(&content_param, format!("{content_param} is required"))
    })?;
    reject_unknown_request_fields(&message, context)?;
    let output = parse_text_parts(content, &content_param)?.concat();
    input.push(json!({
        "type": "function_call_output",
        "call_id": call_id,
        "output": output
    }));
    Ok(())
}

fn convert_text_content(
    value: Value,
    param: &str,
    responses_type: &str,
) -> Result<Vec<Value>, ChatError> {
    Ok(parse_text_parts(value, param)?
        .into_iter()
        .map(|text| json!({"type": responses_type, "text": text}))
        .collect())
}

fn parse_text_parts(value: Value, param: &str) -> Result<Vec<String>, ChatError> {
    match value {
        Value::String(text) => Ok(vec![text]),
        Value::Array(parts) => {
            if parts.is_empty() {
                return Err(ChatError::invalid(
                    param,
                    format!("{param} must contain at least one text part"),
                ));
            }
            parts
                .into_iter()
                .enumerate()
                .map(|(index, part)| {
                    let context = format!("{param}[{index}]");
                    let mut part = into_request_object(part, &context)?;
                    let part_type =
                        take_request_string(&mut part, "type", &format!("{context}.type"))?;
                    if part_type != "text" {
                        return Err(ChatError::invalid(
                            format!("{context}.type"),
                            format!("unsupported content type {part_type}"),
                        ));
                    }
                    let text = take_request_string(&mut part, "text", &format!("{context}.text"))?;
                    reject_unknown_request_fields(&part, &context)?;
                    Ok(text)
                })
                .collect()
        }
        _ => Err(ChatError::invalid(
            param,
            format!("{param} must be a string or an array of text parts"),
        )),
    }
}

fn convert_tools(value: Value) -> Result<Value, ChatError> {
    let tools = value
        .as_array()
        .ok_or_else(|| ChatError::invalid("tools", "tools must be an array"))?;
    let converted = tools
        .iter()
        .enumerate()
        .map(|(index, tool)| convert_tool(tool, &format!("tools[{index}]")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(converted))
}

fn convert_tool(value: &Value, context: &str) -> Result<Value, ChatError> {
    let mut tool = value
        .as_object()
        .cloned()
        .ok_or_else(|| ChatError::invalid(context, format!("{context} must be an object")))?;
    let tool_type = take_request_string(&mut tool, "type", &format!("{context}.type"))?;
    if tool_type != "function" {
        return Err(ChatError::invalid(
            format!("{context}.type"),
            "only function tools are supported",
        ));
    }
    let function_value = tool.remove("function").ok_or_else(|| {
        ChatError::invalid(
            format!("{context}.function"),
            format!("{context}.function is required"),
        )
    })?;
    reject_unknown_request_fields(&tool, context)?;

    let function_context = format!("{context}.function");
    let mut function = into_request_object(function_value, &function_context)?;
    let name = take_request_string(&mut function, "name", &format!("{function_context}.name"))?;
    let parameters = match function.remove("parameters") {
        None | Some(Value::Null) => Value::Null,
        Some(parameters @ Value::Object(_)) => parameters,
        Some(_) => {
            return Err(ChatError::invalid(
                format!("{function_context}.parameters"),
                format!("{function_context}.parameters must be an object or null"),
            ));
        }
    };
    let description = function
        .remove("description")
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                ChatError::invalid(
                    format!("{function_context}.description"),
                    format!("{function_context}.description must be a string"),
                )
            })
        })
        .transpose()?;
    let strict = match function.remove("strict") {
        None | Some(Value::Null) => false,
        Some(value) => request_bool(value, &format!("{function_context}.strict"))?,
    };
    reject_unknown_request_fields(&function, &function_context)?;

    let mut converted = Map::new();
    converted.insert("type".into(), Value::String("function".into()));
    converted.insert("name".into(), Value::String(name));
    if let Some(description) = description {
        converted.insert("description".into(), Value::String(description));
    }
    converted.insert("parameters".into(), parameters);
    converted.insert("strict".into(), Value::Bool(strict));
    Ok(Value::Object(converted))
}

fn convert_tool_choice(value: Value) -> Result<Value, ChatError> {
    match value {
        Value::String(choice) => match choice.as_str() {
            "none" | "auto" | "required" => Ok(Value::String(choice)),
            _ => Err(ChatError::invalid(
                "tool_choice",
                "tool_choice must be none, auto, required, or a named function",
            )),
        },
        Value::Object(mut choice) => {
            let choice_type = take_request_string(&mut choice, "type", "tool_choice.type")?;
            if choice_type != "function" {
                return Err(ChatError::invalid(
                    "tool_choice.type",
                    "only named function tool_choice objects are supported",
                ));
            }
            let function_value = choice.remove("function").ok_or_else(|| {
                ChatError::invalid("tool_choice.function", "tool_choice.function is required")
            })?;
            reject_unknown_request_fields(&choice, "tool_choice")?;
            let mut function = into_request_object(function_value, "tool_choice.function")?;
            let name = take_request_string(&mut function, "name", "tool_choice.function.name")?;
            reject_unknown_request_fields(&function, "tool_choice.function")?;
            Ok(json!({"type": "function", "name": name}))
        }
        _ => Err(ChatError::invalid(
            "tool_choice",
            "tool_choice must be a string or named function object",
        )),
    }
}

struct ConvertedOutput {
    text: Option<String>,
    refusal: Option<String>,
    tool_calls: Vec<Value>,
}

fn convert_output(output: &[Value]) -> Result<ConvertedOutput, ChatError> {
    let mut converted = ConvertedOutput {
        text: None,
        refusal: None,
        tool_calls: Vec::new(),
    };

    for (index, item) in output.iter().enumerate() {
        let item = item.as_object().ok_or_else(|| {
            ChatError::upstream(format!("response output item {index} must be an object"))
        })?;
        let item_type = upstream_string(item, "type", "response output item")?;
        match item_type {
            "reasoning" => {}
            "message" => convert_output_message(item, index, &mut converted)?,
            "function_call" => {
                let call_id = upstream_string(item, "call_id", "function call output item")?;
                let name = upstream_string(item, "name", "function call output item")?;
                let arguments = upstream_string(item, "arguments", "function call output item")?;
                converted.tool_calls.push(json!({
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": arguments
                    }
                }));
            }
            unsupported => {
                return Err(ChatError::upstream(format!(
                    "unsupported Responses output item type {unsupported}"
                )));
            }
        }
    }
    Ok(converted)
}

fn convert_output_message(
    item: &Map<String, Value>,
    item_index: usize,
    converted: &mut ConvertedOutput,
) -> Result<(), ChatError> {
    if upstream_string(item, "role", "message output item")? != "assistant" {
        return Err(ChatError::upstream(
            "Responses message output role must be assistant",
        ));
    }
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| ChatError::upstream("Responses message content must be an array"))?;
    for (content_index, part) in content.iter().enumerate() {
        let part = part.as_object().ok_or_else(|| {
            ChatError::upstream(format!(
                "response output[{item_index}].content[{content_index}] must be an object"
            ))
        })?;
        match upstream_string(part, "type", "message content part")? {
            "output_text" => {
                converted
                    .text
                    .get_or_insert_with(String::new)
                    .push_str(upstream_string(part, "text", "output_text content part")?);
            }
            "refusal" => {
                converted
                    .refusal
                    .get_or_insert_with(String::new)
                    .push_str(upstream_string(part, "refusal", "refusal content part")?);
            }
            unsupported => {
                return Err(ChatError::upstream(format!(
                    "unsupported Responses message content type {unsupported}"
                )));
            }
        }
    }
    Ok(())
}

fn incomplete_finish_reason(response: &Map<String, Value>) -> Result<&'static str, ChatError> {
    let details = response
        .get("incomplete_details")
        .and_then(Value::as_object)
        .ok_or_else(|| ChatError::upstream("incomplete response is missing incomplete_details"))?;
    match upstream_string(details, "reason", "incomplete response details")? {
        "max_output_tokens" => Ok("length"),
        "content_filter" => Ok("content_filter"),
        reason => Err(ChatError::upstream(format!(
            "unsupported incomplete response reason {reason}"
        ))),
    }
}

fn upstream_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
) -> Result<&'a str, ChatError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ChatError::upstream(format!("{context}.{field} must be a string")))
}

fn upstream_integer(
    object: &Map<String, Value>,
    field: &str,
    context: &str,
) -> Result<i64, ChatError> {
    object
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ChatError::upstream(format!("{context}.{field} must be an integer")))
}
