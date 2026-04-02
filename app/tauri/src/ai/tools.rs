use litesqrl::executor::{Executor, QueryResult};
use litesqrl::query::{Lexer, Parser, Stmt};
use serde::Deserialize;
use serde_json::json;
use std::sync::Mutex;

use super::client::{ToolCall, ToolFunctionSpec, ToolSpec};

pub(super) fn specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            kind: "function",
            function: ToolFunctionSpec {
                name: "get_database_ddl",
                description: "Return the current database schema as CREATE TABLE DDL statements.",
                parameters: json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
            },
        },
        ToolSpec {
            kind: "function",
            function: ToolFunctionSpec {
                name: "run_diagnostic_query",
                description: "Execute a read-only LiteSQRL SELECT query to inspect the current database for debugging. Only SELECT statements are allowed.",
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "A LiteSQRL SELECT query used only for diagnostics. Keep it small and focused."
                        }
                    },
                    "required": ["sql"],
                    "additionalProperties": false
                }),
            },
        },
    ]
}

pub(super) fn execute(
    tool_call: &ToolCall,
    executor: &Mutex<Executor>,
) -> Result<String, String> {
    if tool_call.kind != "function" {
        return Err(format!("Unsupported tool call type: {}", tool_call.kind));
    }

    match tool_call.function.name.as_str() {
        "get_database_ddl" => {
            if !tool_call.function.arguments.trim().is_empty() {
                serde_json::from_str::<serde_json::Value>(
                    &tool_call.function.arguments,
                )
                .map_err(|e| format!("Invalid tool arguments: {e}"))?;
            }
            let executor = executor.lock().map_err(|e| e.to_string())?;
            Ok(executor.schema_ddl())
        }
        "run_diagnostic_query" => {
            let args = serde_json::from_str::<RunDiagnosticQueryArgs>(
                &tool_call.function.arguments,
            )
            .map_err(|e| format!("Invalid tool arguments: {e}"))?;
            run_diagnostic_query(&args.sql, executor)
        }
        name => Err(format!("Unknown AI tool: {}", name)),
    }
}

#[derive(Debug, Deserialize)]
struct RunDiagnosticQueryArgs {
    sql: String,
}

fn run_diagnostic_query(
    sql: &str,
    executor: &Mutex<Executor>,
) -> Result<String, String> {
    if sql.trim().is_empty() {
        return Err("Diagnostic SQL is required.".to_string());
    }

    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer).map_err(|e| e.to_string())?;
    let statements = parser.parse().map_err(|e| e.to_string())?;

    if statements.is_empty() {
        return Err("Diagnostic SQL did not produce any statements.".to_string());
    }

    if statements.iter().any(|s| !matches!(s.stmt, Stmt::Select { .. })) {
        return Err(
            "Only SELECT statements are allowed in diagnostic queries.".to_string()
        );
    }

    let mut executor = executor.lock().map_err(|e| e.to_string())?;
    let mut results = Vec::with_capacity(statements.len());
    for statement in statements {
        results.push(executor.run(statement.stmt).map_err(|e| e.to_string())?);
    }

    format_results(results)
}

fn format_results(results: Vec<QueryResult>) -> Result<String, String> {
    let rendered = results
        .into_iter()
        .map(|result| match result {
            QueryResult::Success => json!({ "type": "success" }),
            QueryResult::Count(count) => json!({ "type": "count", "count": count }),
            QueryResult::Rows { columns, rows } => {
                let total_rows = rows.len();
                let preview_rows = rows.into_iter().take(50).collect::<Vec<_>>();
                json!({
                    "type": "rows",
                    "columns": columns,
                    "rowCount": total_rows,
                    "truncated": total_rows > preview_rows.len(),
                    "rows": preview_rows,
                })
            }
            QueryResult::Err(message) => json!({ "type": "error", "message": message }),
        })
        .collect::<Vec<_>>();

    serde_json::to_string_pretty(&rendered).map_err(|e| e.to_string())
}
