use db_state::DbState;
use internal_types::TupleData;
use log::{LogHeader, OpType};
use storage::{BufKey, Storable};

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub header: LogHeader,
    pub data: TupleData,
}

impl LogEntry {
    pub fn load(bytes: TupleData) -> Result<LogEntry, std::io::Error> {
        let (header, data) = LogHeader::from_data(bytes)?;
        Ok(LogEntry { header, data })
    }

    pub fn new(
        buf_key: BufKey,
        op: OpType,
        data: TupleData,
        db_state: &mut DbState,
    ) -> Result<LogEntry, std::io::Error> {
        let lsn = db_state.meta.get_new_lsn()?;
        let header = LogHeader::new(lsn, buf_key, op);
        Ok(LogEntry { header, data })
    }

    pub fn new_pending_cp() -> LogEntry {
        let header = LogHeader::new(
            0,
            BufKey::new(0, 0, false),
            OpType::PendingCheckpoint,
        );
        LogEntry {
            header,
            data: vec![],
        }
    }

    pub fn new_cp() -> LogEntry {
        let header =
            LogHeader::new(0, BufKey::new(0, 0, false), OpType::Checkpoint);
        LogEntry {
            header,
            data: vec![],
        }
    }

    pub fn size(&self) -> usize {
        LogHeader::size() + self.data.len()
    }

    pub fn to_data(&self) -> TupleData {
        let mut data = vec![];
        data.append(&mut self.header.to_data());
        data.append(&mut self.data.clone());
        data
    }
}
