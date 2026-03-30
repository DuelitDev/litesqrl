use super::error::{Result, StorageErr};
use super::record::*;
use super::{ColId, RowId, SeqNo, TableId};
use crate::schema::{DataType, DataValue};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RowState {
    pub id: RowId,
    pub alive: bool,
    pub values: HashMap<ColId, DataValue>,
}

#[derive(Debug, Clone)]
pub struct ColState {
    pub id: ColId,
    pub name: Box<str>,
    pub alive: bool,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct TableState {
    pub id: TableId,
    pub name: Box<str>,
    pub alive: bool,
    pub cols: Vec<ColState>,
    pub rows: HashMap<RowId, RowState>,
}

impl TableState {
    pub fn get_col(&self, id: &ColId) -> Option<&ColState> {
        self.cols.iter().find(|c| &c.id == id)
    }

    pub fn get_col_mut(&mut self, id: &ColId) -> Option<&mut ColState> {
        self.cols.iter_mut().find(|c| &c.id == id)
    }

    pub fn get_col_by_name(&self, name: &str) -> Option<&ColState> {
        self.cols.iter().find(|c| c.alive && &*c.name == name)
    }

    pub fn get_col_by_name_mut(&mut self, name: &str) -> Option<&mut ColState> {
        self.cols.iter_mut().find(|c| c.alive && &*c.name == name)
    }

    pub fn live_cols(&self) -> impl Iterator<Item = &ColState> {
        self.cols.iter().filter(|c| c.alive)
    }
}

#[derive(Debug)]
pub struct DbState {
    pub tables: HashMap<TableId, TableState>,
    next_table_id: TableId,
    next_col_id: ColId,
    next_row_id: RowId,
    next_seq_no: SeqNo,
}

impl Default for DbState {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            next_table_id: 1.into(),
            next_col_id: 1.into(),
            next_row_id: 1.into(),
            next_seq_no: 1.into(),
        }
    }
}

impl DbState {
    pub fn get_table(&self, id: &TableId) -> Option<&TableState> {
        self.tables.get(id)
    }

    pub fn get_table_mut(&mut self, id: &TableId) -> Option<&mut TableState> {
        self.tables.get_mut(id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableState> {
        self.tables.values().find(|t| t.alive && &*t.name == name)
    }

    pub fn get_table_by_name_mut(&mut self, name: &str) -> Option<&mut TableState> {
        self.tables.values_mut().find(|t| t.alive && &*t.name == name)
    }

    pub(super) fn alloc_table(&mut self) -> TableId {
        let id = self.next_table_id;
        self.next_table_id.0 += 1;
        id
    }

    pub(super) fn alloc_col(&mut self) -> ColId {
        let id = self.next_col_id;
        self.next_col_id.0 += 1;
        id
    }

    pub(super) fn alloc_row(&mut self) -> RowId {
        let id = self.next_row_id;
        self.next_row_id.0 += 1;
        id
    }

    pub fn apply(&mut self, record: Record) -> Result<()> {
        match record {
            Record::TableCreate(rec) => self.apply_table_create(rec)?,
            Record::TableDrop(rec) => self.apply_table_drop(rec)?,
            // ── 컬럼 ──────────────────────────────────────────────────────
            Record::ColumnCreate(rec) => self.apply_column_create(rec)?,

            Record::ColumnAlter(rec) => {
                let table = self.tables.get_mut(&rec.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "column_alter unknown table: {}",
                        rec.table_id.0
                    ))
                })?;
                let col =
                    table.cols.iter_mut().find(|c| c.id == rec.col_id).ok_or_else(
                        || {
                            StorageErr::Corrupted(format!(
                                "column_alter unknown col: {}",
                                rec.col_id.0
                            ))
                        },
                    )?;
                col.name = rec.new_col_name;
                col.data_type = rec.new_col_type;
            }

            Record::ColumnDrop(rec) => self.apply_column_drop(rec)?,

            // ── 행 ────────────────────────────────────────────────────────
            Record::RowInsert(r) => {
                self.next_row_id = self.next_row_id.max(RowId(r.row_id.0 + 1));
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_insert unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                // live_cols 순서대로 values를 매핑
                let live_cols: Vec<ColId> =
                    table.cols.iter().filter(|c| c.alive).map(|c| c.id).collect();
                let values: HashMap<ColId, DataValue> =
                    live_cols.into_iter().zip(r.values).collect();
                table
                    .rows
                    .insert(r.row_id, RowState { id: r.row_id, values, alive: true });
            }

            Record::RowUpdate(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_update unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                let row = table.rows.get_mut(&r.row_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_update unknown row: {}",
                        r.row_id.0
                    ))
                })?;
                for (col_id, value) in r.patches {
                    row.values.insert(col_id, value);
                }
            }

            Record::RowDelete(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_delete unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                let row = table.rows.get_mut(&r.row_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_delete unknown row: {}",
                        r.row_id.0
                    ))
                })?;
                row.alive = false;
            }
        }
        Ok(())
    }

    pub fn apply_table_create(&mut self, rec: TableCreate) -> Result<()> {
        self.next_table_id = self.next_table_id.max(TableId(rec.table_id.0 + 1));
        for (id, table) in self.tables.iter() {
            if table.name == rec.table_name {
                return Err(StorageErr::TableNameAlreadyExists(rec.table_name));
            } else if *id == rec.table_id {
                return Err(StorageErr::TableIdAlreadyExists(rec.table_id));
            }
        }
        self.tables.insert(
            rec.table_id,
            TableState {
                id: rec.table_id,
                name: rec.table_name,
                alive: true,
                cols: Vec::new(),
                rows: HashMap::new(),
            },
        );
        Ok(())
    }

    pub fn apply_table_drop(&mut self, rec: TableDrop) -> Result<()> {
        let table = self
            .get_table_mut(&rec.table_id)
            .ok_or_else(|| StorageErr::TableNotFound(rec.table_id))?;
        table.alive = false;
        Ok(())
    }

    pub fn apply_column_create(&mut self, rec: ColumnCreate) -> Result<()> {
        self.next_col_id = self.next_col_id.max(ColId(rec.col_id.0 + 1));
        let table = self
            .get_table_mut(&rec.table_id)
            .ok_or_else(|| StorageErr::TableNotFound(rec.table_id))?;
        table.cols.push(ColState {
            id: rec.col_id,
            name: rec.col_name,
            alive: true,
            data_type: rec.col_type,
        });
        Ok(())
    }

    pub fn apply_column_alter(&mut self, rec: ColumnAlter) -> Result<()> {
        let table = self
            .get_table_mut(&rec.table_id)
            .ok_or_else(|| StorageErr::TableNotFound(rec.table_id))?;
        let col = table
            .get_col_mut(&rec.col_id)
            .ok_or_else(|| StorageErr::ColumnNotFound(rec.col_id))?;
        col.name = rec.new_col_name;
        col.data_type = rec.new_col_type;
        Ok(())
    }

    pub fn apply_column_drop(&mut self, rec: ColumnDrop) -> Result<()> {
        let table = self
            .get_table_mut(&rec.table_id)
            .ok_or_else(|| StorageErr::TableNotFound(rec.table_id))?;
        let col = table
            .get_col_mut(&rec.col_id)
            .ok_or_else(|| StorageErr::ColumnNotFound(rec.col_id))?;
        col.alive = false;
        Ok(())
    }

    pub fn apply_row_insert(&mut self, rec: RowInsert) -> Result<()> {
        todo!()
    }

    pub fn apply_row_update(&mut self, rec: RowUpdate) -> Result<()> {
        todo!()
    }

    pub fn apply_row_delete(&mut self, rec: RowDelete) -> Result<()> {
        todo!()
    }
}
