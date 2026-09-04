#![forbid(unsafe_code)]

//! OpenRouter / OpenAI-compatible model client with SSE streaming.

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("api error: {0}")]
    Api(String),
}

pub type Result<T> = std::result::Result<T, ModelError>;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Reasoning {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default)]
    pub exclude: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutingProfile {
    Cost,
    #[default]
    Performance,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RoutingPreferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_min_throughput: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_max_latency: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<String>>,
}

impl RoutingPreferences {
    pub fn is_empty(&self) -> bool {
        self.sort.is_none()
            && self.preferred_min_throughput.is_none()
            && self.preferred_max_latency.is_none()
            && self.order.as_ref().is_none_or(Vec::is_empty)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderRouting {
    #[serde(default)]
    pub profile: RoutingProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_fallbacks: Option<bool>,
    #[serde(default)]
    pub cost: RoutingPreferences,
    #[serde(default)]
    pub performance: RoutingPreferences,
    #[serde(default)]
    pub manual: RoutingPreferences,
    #[serde(flatten)]
    pub legacy: RoutingPreferences,
}

impl ProviderRouting {
    pub fn active_preferences(&self) -> &RoutingPreferences {
        let selected = match self.profile {
            RoutingProfile::Cost => &self.cost,
            RoutingProfile::Performance => &self.performance,
            RoutingProfile::Manual => &self.manual,
        };
        if selected.is_empty() {
            &self.legacy
        } else {
            selected
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub temperature: f32,
    pub max_completion_tokens: Option<u32>,
    pub context_length: Option<u32>,
    pub parallel_tool_calls: bool,
    pub reasoning: Reasoning,
    pub routing: Option<ProviderRouting>,
    pub debug: bool,
    pub debug_log: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Role {
    fn as_str(&self) -> &'static str {
        match self {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Content {
    Text(String),
    ToolCall(ToolCall),
    ToolResult { id: String, output: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: Role,
    pub content: Vec<Content>,
}

impl ChatMessage {
    pub fn new(role: Role, text: impl Into<String>) -> Self {
        Self {
            role,
            content: vec![Content::Text(text.into())],
        }
    }

    pub fn plain(&self) -> String {
        self.content
            .iter()
            .filter_map(|c| match c {
                Content::Text(t) => Some(t.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Serialize)]
struct ToolCallRequest {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str,
    function: FunctionRequest,
}

#[derive(Serialize)]
struct FunctionRequest {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct WireMessage {
    role: &'static str,
    content: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ToolCallRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

/// Compile a ChatMessage into the OpenAI wire shape.
fn to_wire(m: &ChatMessage) -> WireMessage {
    // Tool result: role=tool, content = the tool output, tool_call_id = the
    // original call's id. If the output is empty (e.g. `touch`), send a
    // placeholder so the model knows the tool ran.
    if m.role == Role::Tool {
        let (id, output) = match m.content.first() {
            Some(Content::ToolResult { id, output }) => (id.clone(), output.clone()),
            _ => {
                return WireMessage {
                    role: m.role.as_str(),
                    content: serde_json::Value::String(String::new()),
                    tool_calls: None,
                    tool_call_id: None,
                }
            }
        };
        let content = if output.is_empty() {
            "(no output)".to_string()
        } else {
            output
        };
        return WireMessage {
            role: m.role.as_str(),
            content: serde_json::Value::String(content),
            tool_calls: None,
            tool_call_id: Some(id),
        };
    }

    // Text-only message (system/user/assistant with no tool calls): content is
    // a plain string.
    let tool_calls: Vec<Content> = m
        .content
        .iter()
        .filter(|c| matches!(c, Content::ToolCall(_)))
        .cloned()
        .collect();

    if tool_calls.is_empty() {
        return WireMessage {
            role: m.role.as_str(),
            content: serde_json::Value::String(m.plain()),
            tool_calls: None,
            tool_call_id: None,
        };
    }

    // Assistant message with tool calls: content = text (optional), tool_calls
    // = full array of all calls (not just the first).
    let calls: Vec<ToolCallRequest> = tool_calls
        .iter()
        .filter_map(|c| match c {
            Content::ToolCall(tc) => Some(ToolCallRequest {
                id: tc.id.clone(),
                kind: "function",
                function: FunctionRequest {
                    name: tc.name.clone(),
                    arguments: tc.arguments.clone(),
                },
            }),
            _ => None,
        })
        .collect();

    WireMessage {
        role: m.role.as_str(),
        content: serde_json::Value::String(m.plain()),
        tool_calls: Some(calls),
        tool_call_id: None,
    }
}

/// A completed assistant message distilled from the stream.
pub struct Completion {
    pub text: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: TokenUsage,
    /// Non-empty when the provider ended the turn instead of completing it
    /// (e.g. "length"). Lets the harness warn instead of trusting partial text.
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct TokenUsage {
    #[serde(default)]
    pub prompt_tokens: u32,
    #[serde(default)]
    pub completion_tokens: u32,
    #[serde(default)]
    pub total_tokens: u32,
    /// Largest single prompt observed while aggregating model calls.
    #[serde(default)]
    pub context_tokens: u32,
    /// Configured context window used for the request.
    #[serde(default)]
    pub context_window: Option<u32>,
}

impl std::ops::AddAssign for TokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(rhs.prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(rhs.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(rhs.total_tokens);
        self.context_tokens = self.context_tokens.max(rhs.context_tokens);
        self.context_window = rhs.context_window.or(self.context_window);
    }
}

impl Completion {
    pub fn has_tool(&self) -> bool {
        !self.tool_calls.is_empty()
    }
    pub fn to_message(&self) -> ChatMessage {
        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(Content::Text(self.text.clone()));
        }
        for tc in &self.tool_calls {
            content.push(Content::ToolCall(tc.clone()));
        }
        ChatMessage {
            role: Role::Assistant,
            content,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SseChunk {
    #[serde(default)]
    choices: Vec<SseChoice>,
    #[serde(default)]
    usage: Option<TokenUsage>,
}

#[derive(Debug, Deserialize)]
struct SseChoice {
    #[serde(default)]
    delta: SseDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct SseDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<SseToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct SseToolCallDelta {
    index: usize,
    id: Option<String>,
    function: Option<SseFunctionDelta>,
}

#[derive(Debug, Deserialize, Default)]
struct SseFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

struct ToolArgumentStream {
    expected_tool: String,
    expected_argument: String,
    name: String,
    arguments: String,
    emitted_bytes: usize,
    disabled: bool,
}

impl ToolArgumentStream {
    fn new(tool_name: &str, argument_name: &str) -> Self {
        Self {
            expected_tool: tool_name.into(),
            expected_argument: argument_name.into(),
            name: String::new(),
            arguments: String::new(),
            emitted_bytes: 0,
            disabled: false,
        }
    }

    fn push(&mut self, name: Option<&str>, arguments: Option<&str>) -> Option<String> {
        if self.disabled {
            return None;
        }
        if let Some(name) = name {
            self.name.push_str(name);
        }
        if let Some(arguments) = arguments {
            self.arguments.push_str(arguments);
        }
        if !self.expected_tool.starts_with(&self.name) {
            self.disabled = true;
            return None;
        }
        let decoded = match partial_argument_value(&self.arguments, &self.expected_argument) {
            Some(decoded) => decoded,
            None => {
                self.disabled = true;
                return None;
            }
        };
        if self.name != self.expected_tool || decoded.len() <= self.emitted_bytes {
            return None;
        }
        let delta = decoded[self.emitted_bytes..].to_string();
        self.emitted_bytes = decoded.len();
        Some(delta)
    }

    fn finish(&mut self) -> Option<String> {
        if self.disabled || self.name != self.expected_tool {
            return None;
        }
        let decoded = partial_argument_value(&self.arguments, &self.expected_argument)?;
        (decoded.len() > self.emitted_bytes).then(|| decoded[self.emitted_bytes..].to_string())
    }
}

fn partial_argument_value(input: &str, expected_argument: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut index = 0;
    skip_json_whitespace(bytes, &mut index);
    if index == bytes.len() {
        return Some(String::new());
    }
    if bytes[index] != b'{' {
        return None;
    }
    index += 1;
    skip_json_whitespace(bytes, &mut index);
    let (key, key_complete) = partial_json_string(input, &mut index)?;
    if !key_complete {
        return Some(String::new());
    }
    if key != expected_argument {
        return None;
    }
    skip_json_whitespace(bytes, &mut index);
    if index == bytes.len() {
        return Some(String::new());
    }
    if bytes[index] != b':' {
        return None;
    }
    index += 1;
    skip_json_whitespace(bytes, &mut index);
    partial_json_string(input, &mut index).map(|(value, _complete)| value)
}

fn partial_json_string(input: &str, index: &mut usize) -> Option<(String, bool)> {
    let bytes = input.as_bytes();
    if *index == bytes.len() {
        return Some((String::new(), false));
    }
    if bytes[*index] != b'"' {
        return None;
    }
    *index += 1;
    let mut output = String::new();
    while *index < bytes.len() {
        match bytes[*index] {
            b'"' => {
                *index += 1;
                return Some((output, true));
            }
            b'\\' => {
                *index += 1;
                if *index == bytes.len() {
                    return Some((output, false));
                }
                match bytes[*index] {
                    b'"' => output.push('"'),
                    b'\\' => output.push('\\'),
                    b'/' => output.push('/'),
                    b'b' => output.push('\u{0008}'),
                    b'f' => output.push('\u{000c}'),
                    b'n' => output.push('\n'),
                    b'r' => output.push('\r'),
                    b't' => output.push('\t'),
                    b'u' => {
                        let escape_start = *index - 1;
                        let Some(unit) = parse_hex_unit(bytes, *index + 1) else {
                            if bytes.len().saturating_sub(*index + 1) < 4 {
                                return Some((output, false));
                            }
                            return None;
                        };
                        *index += 4;
                        let scalar = if (0xD800..=0xDBFF).contains(&unit) {
                            if bytes.len().saturating_sub(*index + 1) < 6 {
                                *index = escape_start;
                                return Some((output, false));
                            }
                            if bytes.get(*index + 1..*index + 3) != Some(b"\\u") {
                                return None;
                            }
                            let Some(low) = parse_hex_unit(bytes, *index + 3) else {
                                return None;
                            };
                            if !(0xDC00..=0xDFFF).contains(&low) {
                                return None;
                            }
                            *index += 6;
                            0x10000 + (((unit as u32 - 0xD800) << 10) | (low as u32 - 0xDC00))
                        } else if (0xDC00..=0xDFFF).contains(&unit) {
                            return None;
                        } else {
                            unit as u32
                        };
                        output.push(char::from_u32(scalar)?);
                    }
                    _ => return None,
                }
                *index += 1;
            }
            byte if byte < 0x20 => return None,
            _ => {
                let rest = &input[*index..];
                let character = rest.chars().next()?;
                output.push(character);
                *index += character.len_utf8();
            }
        }
    }
    Some((output, false))
}

fn parse_hex_unit(bytes: &[u8], start: usize) -> Option<u16> {
    let value = std::str::from_utf8(bytes.get(start..start + 4)?).ok()?;
    u16::from_str_radix(value, 16).ok()
}

fn skip_json_whitespace(bytes: &[u8], index: &mut usize) {
    while bytes
        .get(*index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        *index += 1;
    }
}

fn take_sse_event(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    let (position, delimiter_len) = match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf <= crlf => (lf, 2),
        (Some(_), Some(crlf)) => (crlf, 4),
        (Some(lf), None) => (lf, 2),
        (None, Some(crlf)) => (crlf, 4),
        (None, None) => return None,
    };
    let event = buffer[..position].to_vec();
    buffer.drain(..position + delimiter_len);
    Some(event)
}

fn sse_data(event: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    for line in event.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(value) = line.strip_prefix(b"data:") else {
            continue;
        };
        let value = value.strip_prefix(b" ").unwrap_or(value);
        if !data.is_empty() {
            data.push(b'\n');
        }
        data.extend_from_slice(value);
    }
    data
}

fn reasoning_wire(reasoning: &Reasoning) -> serde_json::Value {
    let mut wire = serde_json::Map::new();
    wire.insert("enabled".into(), json!(reasoning.enabled));
    if let Some(effort) = &reasoning.effort {
        wire.insert("effort".into(), json!(effort));
    }
    if reasoning.exclude {
        wire.insert("exclude".into(), json!(true));
    }
    serde_json::Value::Object(wire)
}

#[derive(Debug)]
pub struct Model {
    base_url: String,
    api_key: String,
    model: String,
    temperature: f32,
    max_completion_tokens: Option<u32>,
    context_length: Option<u32>,
    parallel_tool_calls: bool,
    reasoning: Reasoning,
    routing: Option<ProviderRouting>,
    debug: bool,
    debug_log: Option<PathBuf>,
    http: reqwest::Client,
}

impl Model {
    pub fn new(config: ModelConfig) -> Self {
        Self {
            base_url: config.base_url,
            api_key: config.api_key,
            model: config.model,
            temperature: config.temperature,
            max_completion_tokens: config.max_completion_tokens,
            context_length: config.context_length,
            parallel_tool_calls: config.parallel_tool_calls,
            reasoning: config.reasoning,
            routing: config.routing,
            debug: config.debug,
            debug_log: config.debug_log,
            http: reqwest::Client::new(),
        }
    }

    /// Stream a chat completion. Calls `on_delta(text)` for each content
    /// token as it arrives, and accumulates the full result.
    pub async fn chat(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[ToolSpec]>,
        on_delta: &mut (dyn FnMut(&str) + Send),
    ) -> Result<Completion> {
        self.chat_with_tool_choice(messages, tools, None, on_delta)
            .await
    }

    /// Require the model to select one supplied tool and stream the selected
    /// argument from the first tool call.
    pub async fn chat_requiring_tool(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
        streamed_argument: (&str, &str),
        on_delta: &mut (dyn FnMut(&str) + Send),
    ) -> Result<Completion> {
        self.chat_with_tool_choice(messages, Some(tools), Some(streamed_argument), on_delta)
            .await
    }

    async fn chat_with_tool_choice(
        &self,
        messages: &[ChatMessage],
        tools: Option<&[ToolSpec]>,
        streamed_argument: Option<(&str, &str)>,
        on_delta: &mut (dyn FnMut(&str) + Send),
    ) -> Result<Completion> {
        let context = fit_context(messages, self.context_length);
        let wire: Vec<WireMessage> = context.iter().map(to_wire).collect();
        let tools_wire: Vec<serde_json::Value> = tools
            .map(|ts| ts.iter().map(|t| t.to_json()).collect())
            .unwrap_or_default();

        let mut body = json!({
            "model": self.model,
            "messages": wire,
            "temperature": self.temperature,
            "stream": true,
            "stream_options": {"include_usage": true},
            "parallel_tool_calls": self.parallel_tool_calls && streamed_argument.is_none(),
        });
        if let Some(max_tokens) = self.max_completion_tokens {
            body["max_completion_tokens"] = json!(max_tokens);
        }
        body["reasoning"] = reasoning_wire(&self.reasoning);
        if let Some(routing) = self.routing.as_ref().and_then(routing_wire) {
            body["provider"] = routing;
        }
        if !tools_wire.is_empty() {
            body["tools"] = json!(tools_wire);
            if streamed_argument.is_some() {
                body["tool_choice"] = json!("required");
            }
        }

        if self.debug {
            if let Some(log_path) = &self.debug_log {
                if let Some(dir) = log_path.parent() {
                    let _ = std::fs::create_dir_all(dir);
                }
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_path)
                {
                    use std::io::Write;
                    let _ = writeln!(f, "=== POST {} model={} ===", self.base_url, self.model);
                    let _ = writeln!(
                        f,
                        "{}",
                        serde_json::to_string_pretty(&body).unwrap_or_default()
                    );
                    let _ = f.flush();
                }
            }
        }

        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ModelError::Api(format!("status {status}: {text}")));
        }

        let mut text = String::new();
        let mut tool_calls: Vec<ToolCall> = Vec::new();
        let mut finish_reason: Option<String> = None;
        let mut usage = TokenUsage::default();
        let mut argument_stream = streamed_argument
            .map(|(tool_name, argument_name)| ToolArgumentStream::new(tool_name, argument_name));

        let mut stream = resp.bytes_stream();
        let mut buf = Vec::new();
        'stream: while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buf.extend_from_slice(&chunk);
            while let Some(event) = take_sse_event(&mut buf) {
                let data = sse_data(&event);
                if data == b"[DONE]" {
                    break 'stream;
                }
                if let Ok(chunk) = serde_json::from_slice::<SseChunk>(&data) {
                    if let Some(chunk_usage) = chunk.usage {
                        usage = chunk_usage;
                    }
                    for choice in chunk.choices {
                        if let Some(fr) = &choice.finish_reason {
                            if !fr.is_empty() {
                                finish_reason = Some(fr.clone());
                            }
                        }
                        if let Some(content) = choice.delta.content {
                            if streamed_argument.is_none() {
                                on_delta(&content);
                            }
                            text.push_str(&content);
                        }
                        if let Some(tcs) = choice.delta.tool_calls {
                            for tc in tcs {
                                // Pad the vec by index so partial args append.
                                while tool_calls.len() <= tc.index {
                                    tool_calls.push(ToolCall {
                                        id: String::new(),
                                        name: String::new(),
                                        arguments: String::new(),
                                    });
                                }
                                let idx = tc.index;
                                if let Some(id) = tc.id {
                                    tool_calls[idx].id = id;
                                }
                                if let Some(f) = tc.function {
                                    if idx == 0 {
                                        if let Some(stream) = argument_stream.as_mut() {
                                            if let Some(delta) = stream
                                                .push(f.name.as_deref(), f.arguments.as_deref())
                                            {
                                                on_delta(&delta);
                                            }
                                        }
                                    }
                                    if let Some(name) = f.name {
                                        tool_calls[idx].name.push_str(&name);
                                    }
                                    if let Some(args) = f.arguments {
                                        tool_calls[idx].arguments.push_str(&args);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(stream) = argument_stream.as_mut() {
            if let Some(delta) = stream.finish() {
                on_delta(&delta);
            }
        }

        usage.context_tokens = usage.prompt_tokens;
        usage.context_window = self.context_length;
        Ok(Completion {
            text,
            tool_calls,
            usage,
            finish_reason,
        })
    }
}

fn routing_wire(routing: &ProviderRouting) -> Option<serde_json::Value> {
    let preferences = routing.active_preferences();
    let mut provider = serde_json::Map::new();
    if let Some(sort) = &preferences.sort {
        provider.insert("sort".into(), json!(sort));
    }
    if let Some(throughput) = preferences.preferred_min_throughput {
        provider.insert("preferred_min_throughput".into(), json!(throughput));
    }
    if let Some(latency) = preferences.preferred_max_latency {
        provider.insert("preferred_max_latency".into(), json!(latency));
    }
    if let Some(order) = &preferences.order {
        if !order.is_empty() {
            provider.insert("order".into(), json!(order));
        }
    }
    if let Some(allow_fallbacks) = routing.allow_fallbacks {
        provider.insert("allow_fallbacks".into(), json!(allow_fallbacks));
    }
    (!provider.is_empty()).then_some(serde_json::Value::Object(provider))
}

fn fit_context(messages: &[ChatMessage], limit: Option<u32>) -> Vec<ChatMessage> {
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return messages.to_vec();
    };
    let estimate = |message: &ChatMessage| -> u32 {
        (serde_json::to_string(message)
            .map(|text| text.len())
            .unwrap_or(0) as u32
            / 4)
            + 1
    };
    let mut remaining = limit;
    let mut selected = Vec::with_capacity(messages.len());
    if let Some(system) = messages
        .first()
        .filter(|message| message.role == Role::System)
    {
        let cost = estimate(system).min(remaining);
        selected.push(system.clone());
        remaining = remaining.saturating_sub(cost);
    }
    let start = usize::from(
        messages
            .first()
            .is_some_and(|message| message.role == Role::System),
    );
    let mut recent = Vec::new();
    for message in messages[start..].iter().rev() {
        let cost = estimate(message);
        if cost > remaining {
            break;
        }
        remaining -= cost;
        recent.push(message.clone());
    }
    recent.reverse();
    selected.extend(recent);
    selected
}

/// A tool definition sent to the model.
#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

impl ToolSpec {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: serde_json::Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_wire_uses_only_the_selected_profile() {
        let routing = ProviderRouting {
            profile: RoutingProfile::Cost,
            allow_fallbacks: Some(true),
            cost: RoutingPreferences {
                order: Some(vec!["Makora".into(), "DeepInfra".into()]),
                ..RoutingPreferences::default()
            },
            performance: RoutingPreferences {
                sort: Some("throughput".into()),
                preferred_max_latency: Some(0.5),
                preferred_min_throughput: Some(100.0),
                ..RoutingPreferences::default()
            },
            ..ProviderRouting::default()
        };

        assert_eq!(
            routing_wire(&routing),
            Some(json!({
                "order": ["Makora", "DeepInfra"],
                "allow_fallbacks": true,
            }))
        );
    }

    #[test]
    fn context_budget_keeps_system_and_newest_messages() {
        let messages = vec![
            ChatMessage::new(Role::System, "system instructions"),
            ChatMessage::new(Role::User, "old context that should be trimmed"),
            ChatMessage::new(Role::Assistant, "old response"),
            ChatMessage::new(Role::User, "latest request"),
        ];
        let fitted = fit_context(&messages, Some(40));
        assert_eq!(
            fitted.first().map(|message| message.role),
            Some(Role::System)
        );
        assert_eq!(
            fitted.last().map(ChatMessage::plain).as_deref(),
            Some("latest request")
        );
        assert!(fitted.len() < messages.len());
    }

    #[test]
    fn no_context_budget_preserves_history() {
        let messages = vec![ChatMessage::new(Role::User, "keep this")];
        assert_eq!(fit_context(&messages, None).len(), messages.len());
    }

    #[test]
    fn default_reasoning_is_explicitly_disabled_on_the_wire() {
        let wire = reasoning_wire(&Reasoning::default());
        assert_eq!(wire, serde_json::json!({ "enabled": false }));
    }

    #[test]
    fn selected_tool_arguments_stream_decoded_text_incrementally() {
        let mut stream = ToolArgumentStream::new("publish", "text");
        assert_eq!(stream.push(Some("pub"), Some(r#"{"te"#)), None);
        assert_eq!(
            stream.push(Some("lish"), Some(r#"xt":"Hel"#)),
            Some("Hel".into())
        );
        assert_eq!(
            stream.push(None, Some(r#"lo \"there\"\n\uD83D\uDE00"}"#)),
            Some("lo \"there\"\n😀".into())
        );
    }

    #[test]
    fn selected_tool_arguments_wait_for_exact_tool_name() {
        let mut stream = ToolArgumentStream::new("publish", "text");
        assert_eq!(stream.push(None, Some(r#"{"text":"Hello"}"#)), None);
        assert_eq!(stream.push(Some("pub"), None), None);
        assert_eq!(stream.push(Some("lish"), None), Some("Hello".into()));

        let mut delegation = ToolArgumentStream::new("publish", "text");
        assert_eq!(
            delegation.push(Some("delegate"), Some(r#"{"text":"secret"}"#)),
            None
        );
    }

    #[test]
    fn sse_framer_preserves_utf8_and_crlf_boundaries() {
        let event = "data: {\"value\":\"café 😀\"}\r\n\r\n".as_bytes();
        let mut buffer = Vec::new();
        let mut framed = None;
        for byte in event {
            buffer.push(*byte);
            framed = take_sse_event(&mut buffer).or(framed);
        }
        let data = sse_data(&framed.expect("complete event"));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&data).unwrap()["value"],
            "café 😀"
        );
        assert!(buffer.is_empty());
    }
}
