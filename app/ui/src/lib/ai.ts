import { invoke } from '@tauri-apps/api/core';

export type AiSettings = {
  apiKey: string;
  endpoint: string;
};

export type ChatMessage = {
  role: string;
  content: string;
};

export type ChatCompletionRequest = {
  model: string;
  messages: ChatMessage[];
  temperature?: number;
  maxTokens?: number;
};

export type ChatCompletionChoice = {
  index: number;
  message: ChatMessage;
  finishReason?: string;
};

export type ChatCompletionResponse = {
  id?: string;
  model?: string;
  choices: ChatCompletionChoice[];
};

export async function loadAiSettings(): Promise<AiSettings | null> {
  return invoke<AiSettings | null>('load_ai_settings');
}

export async function saveAiSettings(settings: AiSettings): Promise<void> {
  await invoke('save_ai_settings', { settings });
}

export async function completeAiChat(
  request: ChatCompletionRequest
): Promise<ChatCompletionResponse> {
  return invoke<ChatCompletionResponse>('complete_ai_chat', { request });
}