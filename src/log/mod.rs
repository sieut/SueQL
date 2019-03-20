pub mod op_type;
use data_type::DataType;
use storage::buf_key::BufKey;
use tuple::TupleData;
use tuple::tuple_desc::TupleDesc;

pub type LSN = u32;

pub struct LogEntry {
    lsn: LSN,
    buf_key: BufKey,
    op: op_type::OpType,
    data: TupleData
}

impl LogEntry {
    fn tuple_desc() -> TupleDesc {
        TupleDesc::new(
            vec![DataType::U32,
                 DataType::U32, DataType::U64,
                 // OpType may use up to 4 bytes if it takes arguments
                 DataType::U32,
                 DataType::VarChar],
            vec!["lsn", "bufkey_fileid", "bufkey_offset", "op", "data"])
    }
}
