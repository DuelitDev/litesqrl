use super::codec::{encode_str, read_str, read_u64};
use super::error::{Result, StorageErr};
use super::types::TableId;
use std::io::{Read, Write};

pub const RECORD_HEADER_LEN: u32 = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RecordKind {
    TableCreate = 1,
    TableDrop = 2,
    ColumnCreate = 3,
    ColumnAlter = 4,
    ColumnDrop = 5,
    RowInsert = 6,
    RowUpdate = 7,
    RowDelete = 8,
}

impl RecordKind {
    fn from_u16(v: u16) -> Result<Self> {
        match v {
            1 => Ok(Self::TableCreate),
            2 => Ok(Self::TableDrop),
            3 => Ok(Self::ColumnCreate),
            4 => Ok(Self::ColumnAlter),
            5 => Ok(Self::ColumnDrop),
            6 => Ok(Self::RowInsert),
            7 => Ok(Self::RowUpdate),
            8 => Ok(Self::RowDelete),
            _ => Err(StorageErr::Corrupted(format!("unknown rec_type: {v}"))),
        }
    }
}

#[derive(Debug)]
pub enum Record {
    TableCreate { table_id: TableId, name: Box<str> },
    TableDrop { table_id: TableId },
}

impl Record {
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::TableCreate { .. } => RecordKind::TableCreate,
            Self::TableDrop { .. } => RecordKind::TableDrop,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::TableCreate { table_id, name } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
                encode_str(&mut buf, name);
            }
            Self::TableDrop { table_id } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
            }
        }
        buf
    }
}

// ── record I/O ──

pub fn write_rec(w: &mut impl Write, rec: &Record, seq_no: u64) -> Result<()> {
    let payload = rec.encode();
    let total_len = RECORD_HEADER_LEN + payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    w.write_all(&total_len.to_le_bytes())?;
    w.write_all(&(rec.kind() as u16).to_le_bytes())?;
    w.write_all(&0u16.to_le_bytes())?;
    w.write_all(&seq_no.to_le_bytes())?;
    w.write_all(&crc.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?;
    w.write_all(&payload)?;
    Ok(())
}

pub fn read_rec(r: &mut impl Read) -> Result<Option<Record>> {
    let mut hdr = [0u8; 24];
    match r.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let total_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
    if total_len < RECORD_HEADER_LEN {
        return Err(StorageErr::Corrupted(format!(
            "total_len {total_len} < {RECORD_HEADER_LEN}"
        )));
    }

    let rec_type = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
    let kind = RecordKind::from_u16(rec_type)?;
    let seq_no = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let expected_crc = u32::from_le_bytes(hdr[16..20].try_into().unwrap());

    let payload_len = (total_len - RECORD_HEADER_LEN) as usize;
    let mut payload = vec![0u8; payload_len];
    match r.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let crc = crc32fast::hash(&payload);
    if crc != expected_crc {
        return Err(StorageErr::Corrupted(format!(
            "crc32 mismatch at {seq_no}: expected {expected_crc:#x}, got {crc:#x}"
        )));
    }

    parse_rec(kind, &mut &payload[..])
}

fn parse_rec(kind: RecordKind, r: &mut &[u8]) -> Result<Option<Record>> {
    match kind {
        RecordKind::TableCreate => {
            let table_id = TableId(read_u64(r)?);
            let name = read_str(r)?;
            Ok(Some(Record::TableCreate { table_id, name }))
        }
        RecordKind::TableDrop => {
            let table_id = TableId(read_u64(r)?);
            Ok(Some(Record::TableDrop { table_id }))
        }
        _ => Err(StorageErr::Corrupted(format!("unsupported record kind: {kind:?}"))),
    }
}
