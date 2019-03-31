use internal_types::ID;
use storage::{Storable, PAGE_SIZE};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct BufKey {
    pub file_id: ID,
    pub offset: u64,
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

impl Storable for BufKey {
    fn size() -> usize {
        std::mem::size_of::<ID>() + std::mem::size_of::<u64>()
    }

    fn from_data(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), std::io::Error> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::Cursor;

        let mut cursor = Cursor::new(bytes);
        let key = BufKey::new(
            cursor.read_u32::<LittleEndian>()?,
            cursor.read_u64::<LittleEndian>()?,
        );
        let leftover_data = Self::leftover_data(cursor);
        Ok((key, leftover_data))
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data = vec![];
        data.append(&mut self.file_id.to_data());
        data.append(&mut self.offset.to_data());
        data
    }
}
