use storage::buf_key::BufKey;
use storage::buf_page::PagePtr;

pub struct TuplePtr {
    buf_key: BufKey,
    buf_offset: PagePtr
}

impl TuplePtr {
    pub fn new(buf_key: BufKey, buf_offset: PagePtr) -> TuplePtr {
        TuplePtr { buf_key, buf_offset }
    }

    pub fn buf_key(&self) -> BufKey {
        self.buf_key.clone()
    }

    pub fn buf_offset(&self) -> PagePtr {
        self.buf_offset
    }
}
