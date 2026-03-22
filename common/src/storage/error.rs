use super::{ColumnId, RowId, TableId};

pub type Result<T> = std::result::Result<T, StorageErr>;

#[derive(Debug)]
pub enum StorageErr {
    Io(std::io::Error),
    Corrupted(String),
    TableNotFound(TableId),
    ColumnNotFound(ColumnId),
    RowNotFound(RowId),
    InvalidSchema(&'static str),
    InvalidRow(&'static str),
}

impl std::fmt::Display for StorageErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Corrupted(msg) => write!(f, "corrupted: {msg}"),
            Self::TableNotFound(id) => write!(f, "table not found: {}", id.0),
            Self::ColumnNotFound(id) => write!(f, "column not found: {}", id.0),
            Self::RowNotFound(id) => write!(f, "row not found: {}", id.0),
            Self::InvalidSchema(msg) => write!(f, "invalid schema: {msg}"),
            Self::InvalidRow(msg) => write!(f, "invalid row: {msg}"),
        }
    }
}

impl std::error::Error for StorageErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageErr {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
