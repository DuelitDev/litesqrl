use crate::executor::{ColumnId, RowId, TableId};
use crate::var_char::VarChar;
use std::cmp::PartialEq;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::SeekFrom;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};

#[repr(u8)]
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
pub enum DataType {
    Int = 11,
    Float = 12,
    Bool = 13,
    String = 14,
}

#[derive(PartialEq, Clone, Debug)]
pub enum DataValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    VChar(VarChar),
}

impl DataType {
    pub fn as_str(self) -> &'static str {
        match self {
            DataType::Int => "Int",
            DataType::Float => "Float",
            DataType::Bool => "Bool",
            DataType::String => "String",
        }
    }
}

impl DataValue {
    pub fn verify(self, data_type: DataType) -> bool {
        match self {
            DataValue::Int(_) => DataType::Int == data_type,
            DataValue::Float(_) => DataType::Float == data_type,
            DataValue::Bool(_) => DataType::Bool == data_type,
            DataValue::VChar(_) => DataType::String == data_type,
        }
    }
}

const ROWS_PER_FILE: u64 = 256;

pub async fn create_table(name: String) -> io::Result<TableId> {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    let val = hasher.finish();
    fs::create_dir(val.to_string()).await?;
    let mut file = fs::File::create(format!("{}/schema", val)).await?;
    file.write_all("LAST_ID 0000000000000000\n".as_bytes())
        .await?;
    file.write_all(format!("NAME {}\n", name).as_bytes())
        .await?;
    file.flush().await?;
    Ok(TableId(val))
}

pub async fn create_column(
    table_id: TableId,
    col_name: String,
    col_type: DataType,
) -> tokio::io::Result<ColumnId> {
    let mut hasher = DefaultHasher::new();
    col_name.hash(&mut hasher);
    let val = hasher.finish();
    let mut file = fs::File::options()
        .append(true)
        .open(format!("{}/schema", table_id.0))
        .await?;
    file.write_all(format!("COLUMN {} {} {col_name}\n", val, col_type.as_str()).as_bytes())
        .await?;
    file.flush().await?;
    Ok(ColumnId(val))
}

pub async fn write_data(fd: &mut fs::File, data: DataValue) -> io::Result<()> {
    match data {
        DataValue::Int(i) => fd.write_all(&i.to_be_bytes()).await, // 8 bytes
        DataValue::Float(f) => fd.write_all(&f.to_be_bytes()).await, // 8 bytes
        DataValue::Bool(b) => fd.write_all(&[b as u8]).await,      // 1 byte
        DataValue::VChar(s) => {
            let var_char = VarChar::try_from(s).unwrap();
            fd.write_all(&var_char.as_bytes()).await
        }
    }
}

pub async fn create_row(table_id: TableId, values: Vec<DataValue>) -> io::Result<RowId> {
    let mut schema_file = fs::File::options()
        .write(true)
        .read(true)
        .open(format!("{}/schema", table_id.0))
        .await?;
    let error = || io::Error::new(io::ErrorKind::Other, "Schema file is corrupted");
    let mut buf = String::new();
    schema_file.read_to_string(&mut buf).await?;

    let mut lines = buf.lines();
    let last_id = buf[8..24].to_string();
    let last_id = u64::from_str_radix(&last_id, 16).unwrap();

    let tb_name = lines
        .nth(1)
        .ok_or_else(error)?
        .strip_prefix("NAME ")
        .ok_or_else(error)?
        .to_string();
    let mut cols = Vec::new();
    for line in lines {
        let mut column_data = line.split(' ');
        column_data.next().ok_or_else(error)?;
        let col_id = column_data.next().ok_or_else(error)?;
        let col_type = column_data.next().ok_or_else(error)?;
        let col_name = column_data.next().ok_or_else(error)?;
        cols.push((
            col_id.to_string(),
            col_type.to_string(),
            col_name.to_string(),
        ));
    }
    let id = last_id + 1;
    let file_num = id / ROWS_PER_FILE;
    let filepath = format!("{}/{}", table_id.0, file_num);
    // write row to data file
    let mut data_file = fs::File::options()
        .create(true)
        .append(true)
        .open(filepath)
        .await?;
    for value in values {
        write_data(&mut data_file, value).await?;
    }
    // rewrite schema with updated last_id
    let schema = format!("LAST_ID {:016x}\nNAME {}\n", id, tb_name);
    schema_file.seek(SeekFrom::Start(0)).await?;
    schema_file.write_all(schema.as_bytes()).await?;
    for (col_id, col_type, col_name) in cols {
        let col = format!("COLUMN {col_id} {col_type} {col_name}\n");
        schema_file.write_all(col.as_bytes()).await?;
    }
    Ok(RowId(id))
}

mod test {
    use super::*;
    #[tokio::test]
    async fn test_create_and_read() {
        std::fs::remove_dir_all("6025841138654200372").unwrap();
        let table_id = create_table("users".to_string()).await.unwrap();
        println!("Created table with ID: {}", table_id.0);
        create_column(table_id, "name".to_string(), DataType::String)
            .await
            .unwrap();
        create_column(table_id, "age".to_string(), DataType::Int)
            .await
            .unwrap();
        create_row(
            table_id,
            vec![
                DataValue::VChar(VarChar::try_from("Alice").unwrap()),
                DataValue::Int(30),
            ],
        )
        .await
        .unwrap();
    }
}
