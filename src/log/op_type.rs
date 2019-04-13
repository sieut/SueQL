extern crate num;
use enum_primitive::FromPrimitive;
use storage::Storable;

enum_from_primitive! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum OpType {
        InsertTuple,
        // UpdateTuple,
        Checkpoint,
        PendingCheckpoint,
    }
}

impl Storable for OpType {
    fn size() -> usize {
        std::mem::size_of::<u8>()
    }

    fn from_data(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), std::io::Error> {
        use std::io::{Error, ErrorKind};
        match OpType::from_u8(bytes[0]) {
            Some(op) => Ok((op, bytes[1..].to_vec())),
            None => {
                Err(Error::new(ErrorKind::InvalidData, "OpType does not exist"))
            }
        }
    }

    fn to_data(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}
