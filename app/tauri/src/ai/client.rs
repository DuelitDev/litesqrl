use serde::{Deserialize, Serialize};

use super::{AiSettings, ChatMessage};

// ---- Wire types ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct WireMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(rename = "tool_call_id", skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(rename = "tool_calls", skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
}

impl WireMessage {
    pub fn tool_result(tool_call_id: String, content: String) -> Self {
        Self {
            role: "tool".to_string(),
            content: Some(content),
            tool_call_id: Some(tool_call_id),
            tool_calls: None,
        }
    }
}

impl From<ChatMessage> for WireMessage {
    fn from(value: ChatMessage) -> Self {
        Self {
            role: value.role,
            content: Some(value.content),
            tool_call_id: None,
            tool_calls: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ToolSpec {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub function: ToolFunctionSpec,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ToolFunctionSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct Payload<'a> {
    model: &'a str,
    messages: &'a [WireMessage],
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(rename = "max_tokens", skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a [ToolSpec]>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Response {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    pub choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Choice {
    pub index: usize,
    pub message: WireMessage,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

// ---- Client ----

pub(super) struct Client {
    http: reqwest::Client,
    pub settings: AiSettings,
}

impl Client {
    pub fn new(settings: AiSettings) -> Self {
        Self { http: reqwest::Client::new(), settings }
    }

    pub async fn chat_completion(
        &self,
        model: &str,
        messages: &[WireMessage],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
        tools: Option<&[ToolSpec]>,
    ) -> Result<Response, String> {
        if self.settings.api_key.trim().is_empty() {
            return Err("AI API key is not configured.".to_string());
        }
        if model.trim().is_empty() {
            return Err("Chat completion model is required.".to_string());
        }
        if messages.is_empty() {
            return Err("At least one chat message is required.".to_string());
        }

        let endpoint = self.settings.endpoint.trim();
        if endpoint.is_empty() {
            return Err("AI endpoint is not configured.".to_string());
        }

        let payload = Payload { model, messages, temperature, max_tokens, tools };

        let response = self
            .http
            .post(endpoint)
            .bearer_auth(self.settings.api_key.trim())
            .json(&payload)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.map_err(|e| e.to_string())?;
            let message = serde_json::from_str::<ErrorEnvelope>(&body)
                .map(|parsed| parsed.error.message)
                .unwrap_or(body);
            return Err(format!("AI request failed with {}: {}", status, message));
        }

        response.json::<Response>().await.map_err(|e| e.to_string())
    }
}
