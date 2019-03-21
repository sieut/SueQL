use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use common::ID;
use std::io::Cursor;
use storage::PAGE_SIZE;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct BufKey {
    file_id: ID,
    offset: u64,
}

impl BufKey {
    pub const fn new(file_id: ID, offset: u64) -> BufKey {
        BufKey { file_id, offset }
    }

    pub fn to_filename(&self) -> String {
        format!("{}.dat", self.file_id)
    }

    pub fn byte_offset(&self) -> u64 {
        self.offset * (PAGE_SIZE as u64)
    }
}

impl std::convert::TryFrom<&mut Cursor<&[u8]>> for BufKey {
    type Error = std::io::Error;

    fn try_from(cursor: &mut Cursor<&[u8]>) -> Result<Self, Self::Error> {
        let file_id = cursor.read_u32::<LittleEndian>()?;
        let offset = cursor.read_u64::<LittleEndian>()?;
        Ok(BufKey { file_id, offset })
    }
}
