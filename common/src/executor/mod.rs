use crate::query::{Expr, Lexer, Parser, Stmt};

pub enum QueryResult {
    Rows(Vec<Vec<String>>),
    Count(usize),
    Success,
    Error(String),
}

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(&mut self, src: String) -> QueryResult {
        QueryResult::Success
    }
}
