use storage::buf_key::BufKey;
use storage::buf_page::PagePtr;

/// Struct that specifies location of tuple in a buffer
///     * buf_offset: starting from 0
pub struct TuplePtr {
    buf_key: BufKey,
    buf_offset: usize
}

impl TuplePtr {
    pub fn new(buf_key: BufKey, buf_offset: usize) -> TuplePtr {
        TuplePtr { buf_key, buf_offset }
    }

    pub fn buf_key(&self) -> BufKey {
        self.buf_key.clone()
    }

    pub fn buf_offset(&self) -> PagePtr {
        self.buf_offset
    }

    pub fn inc_buf_offset(&mut self) {
        self.buf_offset += 1;
    }
}
