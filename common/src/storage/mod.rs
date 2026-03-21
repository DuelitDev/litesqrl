use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    Real,
    Bool,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(Box<str>),
}

pub type Result<T> = std::result::Result<T, StorageErr>;

#[derive(Debug, Clone)]
pub struct Storage {
    pub path: PathBuf,
}

impl Storage {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn create_table(&mut self, name: &str) -> Result<TableId> {
        todo!("create_table")
    }

    pub fn drop_table(&mut self, table: TableId) -> Result<()> {
        todo!("drop_table")
    }

    pub fn create_column(
        &mut self,
        table: TableId,
        name: &str,
        data_type: DataType,
    ) -> Result<ColumnId> {
        todo!("create_column")
    }

    pub fn alter_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        new_name: Option<&str>,
        new_data_type: Option<DataType>,
    ) -> Result<()> {
        todo!("alter_column")
    }

    pub fn drop_column(&mut self, table: TableId, column: ColumnId) -> Result<()> {
        todo!("drop_column")
    }

    pub fn insert_row(
        &mut self,
        table: TableId,
        values: Vec<DataValue>,
    ) -> Result<RowId> {
        todo!("insert_row")
    }

    pub fn update_row(
        &mut self,
        table: TableId,
        row: RowId,
        values: Vec<(ColumnId, DataValue)>,
    ) -> Result<()> {
        todo!("update_row")
    }

    pub fn delete_row(&mut self, table: TableId, row: RowId) -> Result<()> {
        todo!("delete_row")
    }

    pub fn rows(&self, table: TableId) -> Result<Vec<RowId>> {
        todo!("rows")
    }
}

#[derive(Debug)]
pub enum StorageErr {
    Io(std::io::Error),
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
