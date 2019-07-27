use serde::{Deserialize, Serialize};
use storage::BufKey;

/// Struct that specifies location of tuple in a buffer
///     * buf_offset: starting from 0
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}
