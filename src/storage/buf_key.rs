use internal_types::ID;
use serde::{Deserialize, Serialize};
use storage::{BufType, PAGE_SIZE};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct BufKey {
    pub file_id: ID,
    pub offset: u64,
    pub buf_type: BufType,
}

impl BufKey {
    pub const fn new(file_id: ID, offset: u64, buf_type: BufType) -> BufKey {
        BufKey {
            file_id,
            offset,
            buf_type,
        }
    }

    pub fn to_filename(&self, data_dir: String) -> String {
        match &self.buf_type {
            &BufType::Data => format!("{}/{}.dat", data_dir, self.file_id),
            &BufType::Temp => format!("{}/temp/{}.dat", data_dir, self.file_id),
            &BufType::Mem => format!("{}/mem.dat", data_dir),
        }
    }

    pub fn byte_offset(&self) -> u64 {
        self.offset * (PAGE_SIZE as u64)
    }

    pub fn inc_offset(mut self) -> BufKey {
        self.offset += 1;
        self
    }
}
