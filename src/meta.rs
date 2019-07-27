use bincode;
use data_type::DataType;
use db_state::State;
use error::Result;
use internal_types::{ID, LSN};
use rel::Rel;
use storage::buf_mgr::PageLock;
use storage::{BufKey, BufMgr, BufType};
use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;
use utils;

pub static META_REL_ID: ID = 0;
pub static META_BUF_KEY: BufKey = BufKey::new(META_REL_ID, 0, BufType::Data);
pub static TABLE_REL_ID: ID = 1;
pub static TABLE_BUF_KEY: BufKey = BufKey::new(TABLE_REL_ID, 0, BufType::Data);
static STATE_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 0);
static CUR_ID_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 1);
static CUR_LSN_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 2);

#[derive(Clone, Debug)]
pub struct Meta {
    // Keep hold of page from BufMgr so it's never evicted
    buf: PageLock,
}

impl Meta {
    pub fn create_and_load(buf_mgr: &mut BufMgr) -> Result<Meta> {
        use std::io::ErrorKind;

        match Meta::load(buf_mgr) {
            Ok(meta) => Ok(meta),
            Err(e) => match e.io_kind() {
                Some(ErrorKind::NotFound) => Meta::new(buf_mgr),
                _ => panic!("Cannot create_and_load meta\n Error: {:?}", e),
            },
        }
    }

    pub fn load(buf_mgr: &mut BufMgr) -> Result<Meta> {
        let buf = buf_mgr.get_buf(&META_BUF_KEY)?;
        let lock = buf.read().unwrap();

        let id_data = lock.get_tuple_data(&CUR_ID_PTR)?;
        utils::assert_data_len(&id_data, 4)?;

        let lsn_data = lock.get_tuple_data(&CUR_LSN_PTR)?;
        utils::assert_data_len(&lsn_data, 4)?;

        Ok(Meta { buf: buf.clone() })
    }

    pub fn new(buf_mgr: &mut BufMgr) -> Result<Meta> {
        let buf = buf_mgr.new_buf(&META_BUF_KEY)?;
        let mut guard = buf.write().unwrap();
        // State
        let state_data = bincode::serialize(&State::Down)?;
        guard.write_tuple_data(&state_data, None, None)?;
        // ID Counter
        guard.write_tuple_data(&Meta::default_id_counter(), None, None)?;
        // LSN Counter
        guard.write_tuple_data(&[0u8; 4], None, None)?;

        Rel::new_meta_rel(TABLE_REL_ID, table_rel_desc(), buf_mgr)?;

        Ok(Meta { buf: buf.clone() })
    }

    pub fn set_state(&self, state: State) -> Result<()> {
        let mut guard = self.buf.write().unwrap();
        guard.write_tuple_data(
            &bincode::serialize(&state)?,
            Some(&STATE_PTR),
            None,
        )?;
        Ok(())
    }

    pub fn get_new_id(&self) -> Result<ID> {
        self.inc_counter(&CUR_ID_PTR)
    }

    pub fn get_new_lsn(&self) -> Result<LSN> {
        self.inc_counter(&CUR_LSN_PTR)
    }

    fn inc_counter(&self, ptr: &TuplePtr) -> Result<u32> {
        let mut lock = self.buf.write().unwrap();

        let cur_val: u32 = bincode::deserialize(&lock.get_tuple_data(ptr)?)?;
        lock.write_tuple_data(
            &bincode::serialize(&(cur_val + 1))?,
            Some(&ptr),
            None,
        )?;

        Ok(cur_val + 1)
    }

    const fn default_id_counter() -> [u8; 4] {
        [2u8, 0u8, 0u8, 0u8]
    }
}

// TODO is there a way for this to be a const fn?
fn table_rel_desc() -> TupleDesc {
    TupleDesc::new(
        vec![DataType::VarChar, DataType::U32],
        vec![String::from("table_name"), String::from("rel_id")],
    )
}
