use common::{ID};
use storage::buf_key::BufKey;

pub struct TuplePtr {
    rel_id: ID,
    buf_key:  BufKey,
    buf_offset: u64
}

impl TuplePtr {
    pub fn new(rel_id: ID, buf_key: BufKey, buf_offset: u64) -> TuplePtr {
        TuplePtr { rel_id, buf_key, buf_offset }
    }
}
