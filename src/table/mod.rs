extern crate byteorder;

pub use self::column::Column;
pub use self::tuple::TupleDesc;
use types;
use utils;
use self::byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::io::Cursor;

mod column;
mod tuple;

/// Table's name is max 31 bytes long, aligning with Column's size, for now
pub struct Table {
    pub row_count: u64,
    pub name: String,
    tuple_desc: TupleDesc,
    disk_ptr: String,
}

impl Table {
    pub fn new(buffer: &bufpage::BufPage, disk_ptr: &String) -> Option<Table> {
        match Table::from_bytes(buffer.data()) {
            Some(temp_table) => Some(Table { disk_ptr: disk_ptr.clone(), ..temp_table }),
            None => None
        }
    }
}

/// Storage format for Table:
///     - row_count: 64 bytes
///     - name: 32 bytes (max 31 bytes + NULL ending bytes)
///     - columns: rest of page
/// Total: 4096 bytes (a page)
impl Storable for Table {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::get_size().unwrap() { return None; }

        let mut rc_rdr = Cursor::new(&bytes[0..64]);
        let row_count = rc_rdr.read_u64::<LittleEndian>().unwrap();

        let name = utils::string_from_bytes(&bytes[64..96]).unwrap();

        let mut columns = vec![];
        let mut iter = bytes.chunks(32);
        iter.next(); iter.next(); iter.next();            // Skip the first 3 chunks (row_count, table_name)
        while let Some(chunk) = iter.next() {
            let col = Column::from_bytes(&chunk);
            if col.is_some() { columns.push(col.unwrap()); }
            else { break; }
        }

        Some(Table {
            row_count: row_count,
            name: name,
            tuple_desc: TupleDesc::new(&columns),
            disk_ptr: String::from("")
        })
    }

    // TODO implement
    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret:Vec<u8> = vec![];

        ret.write_u64::<LittleEndian>(self.row_count).unwrap();
        ret.append(&mut utils::string_to_bytes(&self.name, 32).unwrap());

        for col in self.tuple_desc.columns.iter() {
            ret.append(&mut col.to_bytes().unwrap());
        }

        let cur_len = ret.len();
        ret.append(&mut vec![0; Self::get_size().unwrap() - cur_len]);

        Some(ret)
    }

    fn get_size() -> Option<usize> { Some(PAGE_SIZE) }
}
