use super::error::{Result, StorageErr};
use std::io::{Read, Seek, SeekFrom, Write};

pub const MAGIC: &[u8; 4] = b"SQRL";
pub const VERSION: u8 = 2;
pub const HEADER_LEN: u8 = 64;

#[derive(Debug)]
pub struct FileHeader {
    pub flags: u16,
}

impl FileHeader {
    pub fn new() -> Self {
        Self { flags: 0 }
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<()> {
        w.write_all(MAGIC)?;
        w.write_all(&[VERSION])?;
        w.write_all(&[HEADER_LEN])?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&[0u8; 56])?;
        Ok(())
    }

    pub fn read_from(r: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; 64];
        r.read_exact(&mut buf)?;
        if &buf[0..4] != MAGIC {
            return Err(StorageErr::Corrupted("magic mismatch".into()));
        }
        let version = buf[4];
        if version != VERSION {
            return Err(StorageErr::Corrupted(format!(
                "unsupported version: {version}"
            )));
        }
        let header_len = buf[5];
        if header_len != HEADER_LEN {
            return Err(StorageErr::Corrupted(format!(
                "unexpected header length: {header_len}"
            )));
        }
        let flags = u16::from_le_bytes(buf[12..14].try_into().unwrap());
        Ok(Self { flags })
    }

    pub fn flush_to(&self, w: &mut (impl Write + Seek)) -> Result<()> {
        w.seek(SeekFrom::Start(0))?;
        self.write_to(w)?;
        Ok(())
    }
}
