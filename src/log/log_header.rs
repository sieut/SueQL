use log::OpType;
use internal_types::LSN;
use storage::{BufKey, Storable};

pub struct LogHeader {
    lsn: LSN,
    buf_key: BufKey,
    op: OpType,
}

impl LogHeader {
    pub fn new(lsn: LSN, buf_key: BufKey, op: OpType) -> LogHeader {
        LogHeader { lsn, buf_key, op }
    }
}

impl Storable for LogHeader {
    fn size() -> usize {
        LSN::size() + BufKey::size() + OpType::size()
    }

    fn from_data(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), std::io::Error> {
        let (lsn, bytes) = LSN::from_data(bytes)?;
        let (buf_key, bytes) = BufKey::from_data(bytes)?;
        let (op, bytes) = OpType::from_data(bytes)?;
        Ok((LogHeader { lsn, buf_key, op }, bytes))
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data = vec![];
        data.append(&mut self.lsn.to_data());
        data.append(&mut self.buf_key.to_data());
        data.append(&mut self.op.to_data());
        data
    }
}
