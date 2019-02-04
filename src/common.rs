use std::io::Cursor;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use storage::buf_mgr::BufMgr;
use storage::buf_key::BufKey;
use tuple::tuple_ptr::TuplePtr;
use utils;

pub type ID = u64;

static META_REL_ID: ID = 0;
static CUR_ID_OFFSET: usize = 0;

pub fn get_new_id(buf_mgr: &mut BufMgr) -> Result<ID, std::io::Error> {
    let buf_page = buf_mgr.get_buf(&BufKey::new(META_REL_ID, 0))?;
    let mut lock = buf_page.write().unwrap();

    let ptr = TuplePtr::new(BufKey::new(META_REL_ID, 0), CUR_ID_OFFSET);

    let cur_id = {
        let data = lock.get_tuple_data(&ptr)?;
        utils::assert_data_len(&data, 8)?;
        let mut cursor = Cursor::new(&data);
        cursor.read_u64::<LittleEndian>()?
    };

    let new_id = cur_id + 1;
    let mut data: Vec<u8> = vec![];
    LittleEndian::write_u64(&mut data, new_id);
    lock.write_tuple_data(&data[0..8], Some(&ptr))?;

    Ok(new_id)
}
