use bincode;
use data_type::DataType;
use db_state::State;
use error::Result;
use index::HashIndex;
use internal_types::{ID, LSN};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use storage::buf_mgr::PageLock;
use storage::{BufKey, BufMgr, BufType};
use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;
use utils;

pub static META_REL_ID: ID = 0;
pub static META_BUF_KEY: BufKey = BufKey::new(META_REL_ID, 0, BufType::Data);
pub static TABLE_REL_ID: ID = 1;
pub static TABLE_OVERFLOW_ID: ID = 2;
pub static TABLE_BUF_KEY: BufKey = BufKey::new(TABLE_REL_ID, 0, BufType::Data);
static DEFAULT_ID: ID = 4;
static DEFAULT_LSN: LSN = 1;
static STATE_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 0);
static CUR_ID_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 1);
static CUR_LSN_PTR: TuplePtr = TuplePtr::new(META_BUF_KEY, 2);

#[derive(Clone, Debug)]
pub struct Meta {
    // Keep hold of page from BufMgr so it's never evicted
    buf: PageLock,
    cur_id: Arc<AtomicU32>,
    cur_lsn: Arc<AtomicU32>,
    pub table_index: HashIndex,
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
        let cur_id = Arc::new(
            AtomicU32::new(bincode::deserialize(&id_data)?));

        let lsn_data = lock.get_tuple_data(&CUR_LSN_PTR)?;
        utils::assert_data_len(&lsn_data, 4)?;
        let cur_lsn = Arc::new(
            AtomicU32::new(bincode::deserialize(&lsn_data)?));
        let table_index = HashIndex {
            file_id: TABLE_REL_ID,
            overflow_file_id: TABLE_OVERFLOW_ID,
            key_desc: table_rel_desc(),
            rel_id: 0,
        };

        Ok(Meta { buf: buf.clone(), cur_id, cur_lsn, table_index })
    }

    pub fn new(buf_mgr: &mut BufMgr) -> Result<Meta> {
        let buf = buf_mgr.new_buf(&META_BUF_KEY)?;
        let mut guard = buf.write().unwrap();
        // State
        let state_data = bincode::serialize(&State::Down)?;
        guard.write_tuple_data(&state_data, None, None)?;
        // TODO ID and LSN counter will be corrupted when the db crashes
        // The recovery process should update ID and LSN based on the op.
        // ID Counter
        guard.write_tuple_data(
            &bincode::serialize(&DEFAULT_ID)?, None, None)?;
        // LSN Counter
        guard.write_tuple_data(
            &bincode::serialize(&DEFAULT_LSN)?, None, None)?;
        // Table name hash index
        let table_index = HashIndex::new_meta(
            TABLE_REL_ID, TABLE_OVERFLOW_ID, table_rel_desc(), buf_mgr)?;

        Ok(Meta {
            buf: buf.clone(),
            cur_id: Arc::new(AtomicU32::new(DEFAULT_ID)),
            cur_lsn: Arc::new(AtomicU32::new(DEFAULT_LSN)),
            table_index,
        })
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

    pub fn get_new_id(&self) -> ID {
        self.cur_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_new_lsn(&self) -> LSN {
        self.cur_lsn.fetch_add(1, Ordering::SeqCst)
    }

    pub fn persist_counters(&self) -> Result<()> {
        let mut guard = self.buf.write().unwrap();
        guard.write_tuple_data(
            &bincode::serialize(&self.cur_id.load(Ordering::SeqCst))?,
            Some(&CUR_ID_PTR),
            None
        )?;
        guard.write_tuple_data(
            &bincode::serialize(&self.cur_lsn.load(Ordering::SeqCst))?,
            Some(&CUR_LSN_PTR),
            None
        )?;
        Ok(())
    }
}

// TODO is there a way for this to be a const fn?
fn table_rel_desc() -> TupleDesc {
    TupleDesc::new(
        vec![DataType::VarChar],
        vec![String::from("table_name")],
    )
}
