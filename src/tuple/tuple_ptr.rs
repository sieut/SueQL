use storage::{BufKey, Storable};

/// Struct that specifies location of tuple in a buffer
///     * buf_offset: starting from 0
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TuplePtr {
    pub buf_key: BufKey,
    pub buf_offset: usize,
}

impl TuplePtr {
    pub const fn new(buf_key: BufKey, buf_offset: usize) -> TuplePtr {
        TuplePtr {
            buf_key,
            buf_offset,
        }
    }

    pub fn inc_buf_offset(&mut self) {
        self.buf_offset += 1;
    }
}

impl Storable for TuplePtr {
    fn size() -> usize {
        BufKey::size() + std::mem::size_of::<u32>()
    }

    fn from_data(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), std::io::Error> {
        let (buf_key, bytes) = BufKey::from_data(bytes)?;
        let (buf_offset, bytes) = u32::from_data(bytes)?;
        let ptr = TuplePtr::new(buf_key, buf_offset as usize);
        Ok((ptr, bytes))
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data = vec![];
        data.append(&mut self.buf_key.to_data());
        data.append(&mut (self.buf_offset as u32).to_data());
        data
    }
}
