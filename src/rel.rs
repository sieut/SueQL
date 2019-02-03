extern crate byteorder;
use self::byteorder::ByteOrder;
use self::byteorder::{LittleEndian, ReadBytesExt};

use std::io::Cursor;
use common::ID;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;
use storage::buf_page::BufPage;
use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;

struct Rel {
    rel_id: ID,
    tuple_desc: TupleDesc,
}

impl Rel {
    pub fn load(rel_id: ID, buf_mgr: &mut BufMgr) -> Result<Rel, std::io::Error> {
        let buf_key = BufKey::new(rel_id, 0);
        let mut tuple_ptr = TuplePtr::new(buf_key, 0);

        let mut buf_page = buf_mgr.get_buf(&buf_key)?;
        let read_lock = buf_page.buf.read().unwrap();

        let iter = buf_page.iter();
        // The data should have at least num_attr, and an attr type
        assert!(iter.count() >= 2);

        let cursor = Cursor::new(&read_lock[iter.next().unwrap()]);
        let num_attr = cursor.read_u32::<LittleEndian>()?;

        let mut attr_ids = vec![];
        loop {
            match iter.next() {
                Some(range) => {
                    let cursor = Cursor::new(&read_lock[range]);
                    attr_ids.push(cursor.read_u32::<LittleEndian>()?);
                },
                None => { break; }
            };
        }
    }

    fn read_u32(buf_page: &BufPage, tuple_ptr: &TuplePtr) -> Result<u32, std::io::Error> {
        let range = buf_page.get_tuple_data_range(tuple_ptr)?;
        let read_lock = buf_page.buf.read().unwrap();
        let cursor = Cursor::new(&read_lock[range]);
        Ok(cursor.read_u32::<LittleEndian>()?)
    }
}
