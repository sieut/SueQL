use std::io::Cursor;
use std::sync::RwLockWriteGuard;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use db_state;
use log::LSN;
use storage::buf_mgr::BufMgr;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;
use tuple::tuple_ptr::TuplePtr;
use utils;

pub type ID = u32;

pub static META_REL_ID: ID = 0;
pub static META_BUF_KEY: BufKey = BufKey::new(META_REL_ID, 0);
pub static TABLE_REL_ID: ID = 1;
pub static TABLE_BUF_KEY: BufKey = BufKey::new(TABLE_REL_ID, 0);
static STATE_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 0);
static CUR_ID_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 1);
static CUR_LSN_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 2);

pub fn set_state(
    buf_mgr: &mut BufMgr,
    state: db_state::State,
    guard: Option<RwLockWriteGuard<BufPage>>)
-> Result<(), std::io::Error> {
    let meta = buf_mgr.get_buf(&META_BUF_KEY)?;
    let mut guard = match guard {
        Some(guard) => guard,
        None => meta.write().unwrap(),
    };

    let data: Vec<u8> = state.into();
    guard.write_tuple_data(&data, Some(&STATE_PTR))?;
    Ok(())
}

pub fn get_new_id(buf_mgr: &mut BufMgr) -> Result<ID, std::io::Error> {
    inc_counter(buf_mgr, &CUR_ID_PTR)
}

pub fn get_new_lsn(buf_mgr: &mut BufMgr) -> Result<LSN, std::io::Error> {
    inc_counter(buf_mgr, &CUR_LSN_PTR)
}

fn inc_counter(buf_mgr: &mut BufMgr, ptr: &TuplePtr)
        -> Result<u32, std::io::Error> {
    let buf_page = buf_mgr.get_buf(&META_BUF_KEY)?;
    let mut lock = buf_page.write().unwrap();

    let cur_counter = {
        let data = lock.get_tuple_data(ptr)?;
        utils::assert_data_len(&data, 4)?;
        let mut cursor = Cursor::new(&data);
        cursor.read_u32::<LittleEndian>()?
    };

    let new_counter = cur_counter + 1;
    let mut data = vec![0u8; 4];
    LittleEndian::write_u32(&mut data, new_counter);
    lock.write_tuple_data(&data[0..4], Some(&ptr))?;
    Ok(new_counter)
}
