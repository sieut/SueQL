use common::{ID};
use storage::buf_key::BufKey;
use storage::buf_page::PagePtr;

pub struct TuplePtr {
    rel_id: ID,
    buf_key: BufKey,
    buf_offset: PagePtr
}

impl TuplePtr {
    pub fn new(rel_id: ID, buf_key: BufKey, buf_offset: PagePtr) -> TuplePtr {
        TuplePtr { rel_id, buf_key, buf_offset }
    }

    pub fn buf_key(&self) -> BufKey {
        self.buf_key.clone()
    }

    pub fn buf_offset(&self) -> PagePtr {
        self.buf_offset
    }
}
