use internal_types::LSN;
use log::OpType;
use serde::{Deserialize, Serialize};
use storage::BufKey;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogHeader {
    pub lsn: LSN,
    pub buf_key: BufKey,
    pub op: OpType,
}

impl LogHeader {
    pub fn new(lsn: LSN, buf_key: BufKey, op: OpType) -> LogHeader {
        LogHeader { lsn, buf_key, op }
    }
}
