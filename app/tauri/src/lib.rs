mod ai;

use ai::{AiSettings, ChatCompletionRequest, ChatCompletionResponse};
use litesqrl::executor::{Executor, QueryResult};
use litesqrl::query::{Lexer, Parser};
use litesqrl::storage::Storage;
use std::sync::Mutex;
use tauri::{Manager, State};

type DbState = Mutex<Executor>;

#[tauri::command]
fn run_query(state: State<DbState>, src: String) -> Vec<QueryResult> {
    let lexer = Lexer::new(src.as_str());
    let mut parser = match Parser::new(lexer) {
        Ok(parser) => parser,
        Err(e) => return vec![QueryResult::Err(e.to_string())],
    };
    let stmts = match parser.parse() {
        Ok(stmts) => stmts,
        Err(e) => return vec![QueryResult::Err(e.to_string())],
    };
    let mut exec = state.lock().unwrap();
    let mut results = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        let result = match exec.run(stmt.stmt) {
            Ok(res) => res,
            Err(e) => QueryResult::Err(e.to_string()),
        };
        results.push(result);
    }
    results
}

#[tauri::command]
fn load_ai_settings<R: tauri::Runtime>(
    app: tauri::AppHandle<R>,
) -> Result<Option<AiSettings>, String> {
    ai::load_ai_settings(&app)
}

#[tauri::command]
fn save_ai_settings<R: tauri::Runtime>(
    app: tauri::AppHandle<R>,
    settings: AiSettings,
) -> Result<(), String> {
    ai::save_ai_settings(&app, settings)
}

#[tauri::command]
async fn list_ai_models<R: tauri::Runtime>(
    app: tauri::AppHandle<R>,
    settings: AiSettings,
) -> Result<Vec<String>, String> {
    ai::list_models_with_saved_settings(&app, settings).await
}

#[tauri::command]
async fn complete_ai_chat<R: tauri::Runtime>(
    app: tauri::AppHandle<R>,
    state: State<'_, DbState>,
    request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, String> {
    ai::complete_chat_with_saved_settings(&app, state.inner(), request).await
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir().expect("no app data dir");
            std::fs::create_dir_all(&data_dir)?;
            let db_path = data_dir.join("database.sqrl");
            let storage = Storage::open(db_path).expect("failed to open storage");
            app.manage(Mutex::new(Executor::new(storage)));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            run_query,
            load_ai_settings,
            save_ai_settings,
            list_ai_models,
            complete_ai_chat
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
