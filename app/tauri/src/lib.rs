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
        .invoke_handler(tauri::generate_handler![run_query])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
