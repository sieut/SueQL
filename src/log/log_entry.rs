use db_state::DbState;
use error::Result;
use internal_types::TupleData;
use log::{LogHeader, OpType};
use serde::{Deserialize, Serialize};
use storage::{BufKey, BufType};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub header: LogHeader,
    pub data: TupleData,
}

impl LogEntry {
    pub fn new(
        buf_key: BufKey,
        op: OpType,
        data: TupleData,
        db_state: &mut DbState,
    ) -> Result<LogEntry> {
        let lsn = db_state.meta.get_new_lsn();
        let header = LogHeader::new(lsn, buf_key, op);
        Ok(LogEntry { header, data })
    }

    pub fn new_pending_cp() -> LogEntry {
        let header = LogHeader::new(
            0,
            BufKey::new(0, 0, BufType::Data),
            OpType::PendingCheckpoint,
        );
        LogEntry {
            header,
            data: vec![],
        }
    }

    pub fn new_cp() -> LogEntry {
        let header = LogHeader::new(
            0,
            BufKey::new(0, 0, BufType::Data),
            OpType::Checkpoint,
        );
        LogEntry {
            header,
            data: vec![],
        }
    }
}
