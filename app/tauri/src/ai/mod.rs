mod client;
mod tools;

use client::{Client, WireMessage};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager, Runtime, State};

use crate::DbState;

// ---- Public types ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSettings {
    pub api_key: String,
    pub endpoint: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub lang: String,
}

impl Default for AiSettings {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            endpoint: String::new(),
            model: String::new(),
            lang: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionRequest {
    #[serde(default)]
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionChoice {
    pub index: usize,
    pub message: ChatMessage,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionResponse {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    pub choices: Vec<ChatCompletionChoice>,
}

// ---- Settings persistence ----

fn settings_path<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<std::path::PathBuf> {
    let data_dir = app.path().app_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;
    Ok(data_dir.join("ai-settings.json"))
}

fn read_settings<R: Runtime>(app: &AppHandle<R>) -> Result<Option<AiSettings>, String> {
    let path = settings_path(app).map_err(|e| e.to_string())?;
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str::<AiSettings>(&raw).map(Some).map_err(|e| e.to_string())
}

fn write_settings<R: Runtime>(
    app: &AppHandle<R>,
    settings: &AiSettings,
) -> Result<(), String> {
    let path = settings_path(app).map_err(|e| e.to_string())?;
    let normalized = AiSettings {
        api_key: settings.api_key.trim().to_string(),
        endpoint: settings.endpoint.trim().to_string(),
        model: settings.model.trim().to_string(),
        lang: settings.lang.trim().to_string(),
    };
    let raw = serde_json::to_string_pretty(&normalized).map_err(|e| e.to_string())?;
    std::fs::write(path, raw).map_err(|e| e.to_string())
}

// ---- Tauri commands ----

#[tauri::command]
pub fn load_ai_settings<R: Runtime>(
    app: AppHandle<R>,
) -> Result<Option<AiSettings>, String> {
    read_settings(&app)
}

#[tauri::command]
pub fn save_ai_settings<R: Runtime>(
    app: AppHandle<R>,
    settings: AiSettings,
) -> Result<(), String> {
    write_settings(&app, &settings)
}

#[tauri::command]
pub async fn complete_ai_chat<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, DbState>,
    request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, String> {
    let settings = read_settings(&app)?
        .filter(|s| !s.api_key.trim().is_empty())
        .ok_or_else(|| "AI API key is not configured.".to_string())?;

    let client = Client::new(settings);
    let tool_specs = tools::specs();
    let model = if request.model.trim().is_empty() {
        client.settings.model.trim().to_string()
    } else {
        request.model.trim().to_string()
    };
    if model.is_empty() {
        return Err("AI model is not configured.".to_string());
    }

    let mut messages =
        request.messages.into_iter().map(WireMessage::from).collect::<Vec<_>>();

    for _ in 0..4 {
        let response = client
            .chat_completion(
                &model,
                &messages,
                request.temperature,
                request.max_tokens,
                Some(&tool_specs),
            )
            .await?;

        let needs_tools = response.choices.iter().any(|c| {
            c.message.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty())
        });

        if !needs_tools {
            return Ok(into_public_response(response));
        }

        let mut new_messages = Vec::new();
        for choice in &response.choices {
            if let Some(tool_calls) = &choice.message.tool_calls {
                new_messages.push(choice.message.clone());
                for tool_call in tool_calls {
                    let content = tools::execute(tool_call, state.inner())?;
                    new_messages
                        .push(WireMessage::tool_result(tool_call.id.clone(), content));
                }
            }
        }
        messages.extend(new_messages);
    }

    Err("AI tool execution exceeded the maximum number of round trips.".to_string())
}

// ---- Helpers ----

fn into_public_response(response: client::Response) -> ChatCompletionResponse {
    ChatCompletionResponse {
        id: response.id,
        model: response.model,
        choices: response
            .choices
            .into_iter()
            .map(|c| ChatCompletionChoice {
                index: c.index,
                message: ChatMessage {
                    role: c.message.role,
                    content: c.message.content.unwrap_or_default(),
                },
                finish_reason: c.finish_reason,
            })
            .collect(),
    }
}
