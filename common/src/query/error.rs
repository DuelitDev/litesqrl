use std::fmt;

pub type Result<T> = std::result::Result<T, QueryErr>;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryErr {
    UnexpectedEof,
    InvalidNum(String),
    UnterminatedText,
    InvalidToken(char),
    InvalidIdent(String),
    InvalidExpr(String),
    UnexpectedToken { expected: String, found: String },
}

impl fmt::Display for QueryErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "Unexpected end of file while parsing"),
            Self::InvalidNum(s) => write!(f, "Invalid number format: '{}'", s),
            Self::UnterminatedText => write!(f, "Unterminated text literal"),
            Self::InvalidToken(c) => write!(f, "Invalid character: '{}'", c),
            Self::InvalidIdent(i) => write!(f, "Invalid identifier: '{}'", i),
            Self::InvalidExpr(e) => write!(f, "Invalid expression: {}", e),
            Self::UnexpectedToken { expected, found } => {
                write!(f, "Expected {}, but found {}", expected, found)
            }
        }
    }
}

impl std::error::Error for QueryErr {}
