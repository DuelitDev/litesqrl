use super::error::{Result, StorageErr};
use std::io::{Read, Seek, SeekFrom, Write};

pub const MAGIC: &[u8; 8] = b"SQRLDB02";
pub const VERSION: u16 = 2;
pub const HEADER_LEN: u16 = 64;

#[derive(Debug)]
pub struct FileHeader {
    pub flags: u32,
    pub next_table_id: u64,
    pub next_col_id: u64,
    pub next_row_id: u64,
    pub next_seq_no: u64,
}

impl FileHeader {
    pub fn new() -> Self {
        Self {
            flags: 0,
            next_table_id: 1,
            next_col_id: 1,
            next_row_id: 1,
            next_seq_no: 1,
        }
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<()> {
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_le_bytes())?;
        w.write_all(&HEADER_LEN.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.next_table_id.to_le_bytes())?;
        w.write_all(&self.next_col_id.to_le_bytes())?;
        w.write_all(&self.next_row_id.to_le_bytes())?;
        w.write_all(&self.next_seq_no.to_le_bytes())?;
        w.write_all(&[0u8; 16])?;
        Ok(())
    }

    pub fn read_from(r: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; 64];
        r.read_exact(&mut buf)?;
        if &buf[0..8] != MAGIC {
            return Err(StorageErr::Corrupted("magic mismatch".into()));
        }
        let version = u16::from_le_bytes(buf[8..10].try_into().unwrap());
        if version != VERSION {
            return Err(StorageErr::Corrupted(format!(
                "unsupported version: {version}"
            )));
        }
        let header_len = u16::from_le_bytes(buf[10..12].try_into().unwrap());
        if header_len != HEADER_LEN {
            return Err(StorageErr::Corrupted(format!(
                "unexpected header_len: {header_len}"
            )));
        }
        Ok(Self {
            flags: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            next_table_id: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            next_col_id: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            next_row_id: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            next_seq_no: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
        })
    }

    pub fn flush_to(&self, w: &mut (impl Write + Seek)) -> Result<()> {
        w.seek(SeekFrom::Start(0))?;
        self.write_to(w)?;
        Ok(())
    }
}
