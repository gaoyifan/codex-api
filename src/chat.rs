//! Strict, transport-independent Chat Completions compatibility.
//!
//! This module owns the deliberately small public Chat request subset and the
//! conversion of a terminal Responses event back into one non-streaming Chat
//! Completion. It does not perform model admission, I/O, retries, accounting,
//! or persistence.

use serde_json::{Map, Value, json};

use crate::{
    config::REASONING_EFFORTS,
    responses_terminal::{TerminalEvent, TerminalKind, Usage},
};

/// A compact protocol error which the HTTP layer can map to either 400 or 502.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ChatError {
    pub param: Option<String>,
    pub message: String,
}

impl ChatError {
    fn invalid(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            param: Some(param.into()),
            message: message.into(),
        }
    }

    fn upstream(message: impl Into<String>) -> Self {
        Self {
            param: None,
            message: message.into(),
        }
    }
}

/// A validated Chat request and its Responses wire representation.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvertedRequest {
    pub upstream_request: Value,
    pub stream: bool,
    pub include_usage: bool,
}

pub struct ChatStreamConverter {
    metadata: Option<(String, i64, String)>,
    include_usage: bool,
}

impl ChatStreamConverter {
    pub fn new(include_usage: bool) -> Self {
        Self {
            metadata: None,
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

    pub fn convert_terminal(&mut self, terminal: &TerminalEvent) -> Result<Vec<Value>, ChatError> {
        if matches!(terminal.kind, TerminalKind::Failed | TerminalKind::Error) {
            return Err(ChatError::upstream(format!(
                "upstream emitted {} instead of a completion",
                terminal.kind.event_name()
            )));
        }
        let response = terminal.payload["response"]
            .as_object()
            .expect("response terminal payload was validated");
        self.capture_metadata(response)?;
        let usage = terminal
            .usage
            .expect("completion terminal usage was validated");
        let finish_reason = if terminal.kind == TerminalKind::Completed {
            "stop"
        } else {
            incomplete_finish_reason(response)?
        };
        let mut chunks = vec![self.chunk(json!({}), Some(finish_reason))?];
        if self.include_usage {
            chunks.push(self.usage_chunk(usage)?);
        }
        Ok(chunks)
    }

    fn capture_metadata(&mut self, response: &Map<String, Value>) -> Result<(), ChatError> {
        self.metadata = Some((
            upstream_string(response, "id", "response")?.to_owned(),
            upstream_integer(response, "created_at", "response")?,
            upstream_string(response, "model", "response")?.to_owned(),
        ));
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
        self.metadata
            .as_ref()
            .map(|(id, created, model)| (id.as_str(), *created, model.as_str()))
            .ok_or_else(|| {
                ChatError::upstream("upstream emitted Chat content before response metadata")
            })
    }
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
    for field in ["max_completion_tokens", "max_tokens"] {
        if let Some(value) = request.remove(field) {
            validate_ignored_completion_limit(value, field)?;
        }
    }
    let text = request
        .remove("response_format")
        .map(convert_response_format)
        .transpose()?
        .flatten();
    reject_unknown_request_fields(&request, "request")?;
    let (instructions, input) = convert_messages(messages)?;

    let mut upstream = Map::new();
    upstream.insert("model".into(), Value::String(model));
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
    if let Some(reasoning_effort) = reasoning_effort {
        upstream.insert("reasoning".into(), json!({"effort": reasoning_effort}));
    }
    if let Some(text) = text {
        upstream.insert("text".into(), text);
    }

    Ok(ConvertedRequest {
        upstream_request: Value::Object(upstream),
        stream,
        include_usage,
    })
}

/// Converts a validated completed or incomplete Responses terminal into one
/// non-streaming Chat Completion.
pub fn convert_terminal_event(terminal: &TerminalEvent) -> Result<Value, ChatError> {
    if matches!(terminal.kind, TerminalKind::Failed | TerminalKind::Error) {
        return Err(ChatError::upstream(format!(
            "upstream emitted {} instead of a completion",
            terminal.kind.event_name()
        )));
    }
    let response = terminal.payload["response"]
        .as_object()
        .expect("response terminal payload was validated");
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
        .expect("completion terminal usage was validated");
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| ChatError::upstream("terminal response output must be an array"))?;
    let message = convert_output(output)?;

    let finish_reason = if terminal.kind == TerminalKind::Incomplete {
        incomplete_finish_reason(response)?
    } else if message.contains_key("tool_calls") {
        "tool_calls"
    } else {
        "stop"
    };

    Ok(json!({
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
    }))
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
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(ChatError::invalid(
            param,
            format!("{param} must be a JSON object"),
        )),
    }
}

fn take_request_string(
    object: &mut Map<String, Value>,
    field: &str,
    param: &str,
) -> Result<String, ChatError> {
    match object.remove(field) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(ChatError::invalid(
            param,
            format!("{param} must be a string"),
        )),
    }
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

fn validate_ignored_completion_limit(value: Value, field: &str) -> Result<(), ChatError> {
    if value.is_null() {
        return Ok(());
    }
    if value.as_u64().is_some_and(|limit| limit > 0) {
        return Ok(());
    }
    Err(ChatError::invalid(
        field,
        format!("{field} must be a positive integer or null"),
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
    let Value::String(effort) = value else {
        return Err(ChatError::invalid(
            "reasoning_effort",
            "reasoning_effort must be a string or null",
        ));
    };
    if REASONING_EFFORTS.contains(&effort.as_str()) {
        Ok(Some(effort))
    } else {
        Err(ChatError::invalid(
            "reasoning_effort",
            "unsupported reasoning_effort",
        ))
    }
}

fn convert_messages(value: Value) -> Result<(Option<String>, Vec<Value>), ChatError> {
    let Value::Array(messages) = value else {
        return Err(ChatError::invalid("messages", "messages must be an array"));
    };
    if messages.is_empty() {
        return Err(ChatError::invalid(
            "messages",
            "messages must contain at least one message",
        ));
    }

    let mut system_messages = Vec::new();
    let mut input = Vec::new();
    for (index, value) in messages.into_iter().enumerate() {
        let context = format!("messages[{index}]");
        let Value::Object(mut message) = value else {
            return Err(ChatError::invalid(
                &context,
                format!("{context} must be an object"),
            ));
        };
        let role = take_request_string(&mut message, "role", &format!("{context}.role"))?;
        match role.as_str() {
            "system" | "developer" | "user" => {
                let content_param = format!("{context}.content");
                let content = message.remove("content").ok_or_else(|| {
                    ChatError::invalid(&content_param, format!("{content_param} is required"))
                })?;
                reject_unknown_request_fields(&message, &context)?;
                if role == "system" {
                    system_messages.push(parse_text_parts(content, &content_param)?.join("\n"));
                } else {
                    let parts = convert_text_content(content, &content_param, "input_text")?;
                    input.push(json!({
                        "type": "message",
                        "role": role,
                        "content": parts
                    }));
                }
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

    let input_start = input.len();
    if let Some(content) = content.filter(|content| !content.is_null()) {
        let content_param = format!("{context}.content");
        let parts = convert_text_content(content, &content_param, "output_text")?;
        input.push(json!({
            "type": "message",
            "role": "assistant",
            "content": parts
        }));
    }

    if let Some(tool_calls) = tool_calls.filter(|tool_calls| !tool_calls.is_null()) {
        let Value::Array(calls) = tool_calls else {
            return Err(ChatError::invalid(
                format!("{context}.tool_calls"),
                format!("{context}.tool_calls must be an array"),
            ));
        };
        if calls.is_empty() {
            return Err(ChatError::invalid(
                format!("{context}.tool_calls"),
                format!("{context}.tool_calls must not be empty"),
            ));
        }
        for (index, call) in calls.into_iter().enumerate() {
            input.push(convert_assistant_tool_call(
                call,
                &format!("{context}.tool_calls[{index}]"),
            )?);
        }
    }

    if input.len() == input_start {
        return Err(ChatError::invalid(
            format!("{context}.content"),
            "assistant message must contain text or function tool calls",
        ));
    }
    Ok(())
}

fn convert_assistant_tool_call(value: Value, context: &str) -> Result<Value, ChatError> {
    let Value::Object(mut call) = value else {
        return Err(ChatError::invalid(
            context,
            format!("{context} must be an object"),
        ));
    };
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
    let Value::Array(tools) = value else {
        return Err(ChatError::invalid("tools", "tools must be an array"));
    };
    let converted = tools
        .into_iter()
        .enumerate()
        .map(|(index, tool)| convert_tool(tool, &format!("tools[{index}]")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(converted))
}

fn convert_tool(value: Value, context: &str) -> Result<Value, ChatError> {
    let Value::Object(mut tool) = value else {
        return Err(ChatError::invalid(
            context,
            format!("{context} must be an object"),
        ));
    };
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
        .map(|value| match value {
            Value::String(description) => Ok(description),
            _ => Err(ChatError::invalid(
                format!("{function_context}.description"),
                format!("{function_context}.description must be a string"),
            )),
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

fn convert_output(output: &[Value]) -> Result<Map<String, Value>, ChatError> {
    let mut text = None::<String>;
    let mut refusal = None::<String>;
    let mut tool_calls = Vec::new();

    for (index, item) in output.iter().enumerate() {
        let item = item.as_object().ok_or_else(|| {
            ChatError::upstream(format!("response output item {index} must be an object"))
        })?;
        let item_type = upstream_string(item, "type", "response output item")?;
        match item_type {
            "reasoning" => {}
            "message" => {
                if upstream_string(item, "role", "message output item")? != "assistant" {
                    return Err(ChatError::upstream(
                        "Responses message output role must be assistant",
                    ));
                }
                let content = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ChatError::upstream("Responses message content must be an array")
                    })?;
                for (content_index, part) in content.iter().enumerate() {
                    let part = part.as_object().ok_or_else(|| {
                        ChatError::upstream(format!(
                            "response output[{index}].content[{content_index}] must be an object"
                        ))
                    })?;
                    match upstream_string(part, "type", "message content part")? {
                        "output_text" => {
                            text.get_or_insert_with(String::new)
                                .push_str(upstream_string(
                                    part,
                                    "text",
                                    "output_text content part",
                                )?);
                        }
                        "refusal" => {
                            refusal
                                .get_or_insert_with(String::new)
                                .push_str(upstream_string(
                                    part,
                                    "refusal",
                                    "refusal content part",
                                )?);
                        }
                        unsupported => {
                            return Err(ChatError::upstream(format!(
                                "unsupported Responses message content type {unsupported}"
                            )));
                        }
                    }
                }
            }
            "function_call" => {
                let call_id = upstream_string(item, "call_id", "function call output item")?;
                let name = upstream_string(item, "name", "function call output item")?;
                let arguments = upstream_string(item, "arguments", "function call output item")?;
                tool_calls.push(json!({
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
    let mut message = Map::new();
    message.insert("role".into(), Value::String("assistant".into()));
    message.insert("content".into(), text.map_or(Value::Null, Value::String));
    message.insert("refusal".into(), refusal.map_or(Value::Null, Value::String));
    if !tool_calls.is_empty() {
        message.insert("tool_calls".into(), Value::Array(tool_calls));
    }
    Ok(message)
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
