use super::types::{ColumnId, DataType, DataValue, RowId, TableId};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ColumnMeta {
    pub id: ColumnId,
    pub name: Box<str>,
    pub data_type: DataType,
    pub alive: bool,
}

#[derive(Debug)]
pub struct RowState {
    pub alive: bool,
    pub values: HashMap<ColumnId, DataValue>,
}

#[derive(Debug)]
pub struct TableMeta {
    pub id: TableId,
    pub name: Box<str>,
    pub alive: bool,
    pub columns: Vec<ColumnMeta>,
    pub rows: HashMap<RowId, RowState>,
}
