use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use data_type::DataType;
use db_state::State;
use internal_types::{ID, LSN};
use rel::Rel;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use storage::{BufKey, BufMgr, BufType};
use storage::buf_mgr::TableItem;
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
    buf: TableItem,
    cur_id: Arc<Mutex<ID>>,
    cur_lsn: Arc<Mutex<LSN>>,
}

impl Meta {
    pub fn create_and_load(
        buf_mgr: &mut BufMgr,
    ) -> Result<Meta, std::io::Error> {
        use std::io::ErrorKind;

        match Meta::load(buf_mgr) {
            Ok(meta) => Ok(meta),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => Meta::new(buf_mgr),
                _ => panic!("Cannot create_and_load meta\n Error: {:?}", e),
            },
        }
    }

    pub fn load(buf_mgr: &mut BufMgr) -> Result<Meta, std::io::Error> {
        let buf = buf_mgr.get_buf(&META_BUF_KEY)?;
        let lock = buf.read().unwrap();

        let id_data = lock.get_tuple_data(&CUR_ID_PTR)?;
        utils::assert_data_len(&id_data, 4)?;
        let mut cursor = Cursor::new(&id_data);
        let cur_id = Arc::new(Mutex::new(cursor.read_u32::<LittleEndian>()?));

        let lsn_data = lock.get_tuple_data(&CUR_LSN_PTR)?;
        utils::assert_data_len(&lsn_data, 4)?;
        let mut cursor = Cursor::new(&lsn_data);
        let cur_lsn = Arc::new(Mutex::new(cursor.read_u32::<LittleEndian>()?));

        Ok(Meta {
            buf: buf.clone(),
            cur_id,
            cur_lsn,
        })
    }

    pub fn new(buf_mgr: &mut BufMgr) -> Result<Meta, std::io::Error> {
        let buf = buf_mgr.new_buf(&META_BUF_KEY)?;
        let mut guard = buf.write().unwrap();
        // State
        let state_data: Vec<u8> = State::Down.into();
        guard.write_tuple_data(&state_data, None, None)?;
        // ID Counter
        guard.write_tuple_data(&Meta::default_id_counter(), None, None)?;
        // LSN Counter
        guard.write_tuple_data(&[0u8; 4], None, None)?;

        Rel::new_meta_rel(TABLE_REL_ID, table_rel_desc(), buf_mgr)?;

        Ok(Meta {
            buf: buf.clone(),
            cur_id: Arc::new(Mutex::new(2)),
            cur_lsn: Arc::new(Mutex::new(0)),
        })
    }

    pub fn set_state(&self, state: State) -> Result<(), std::io::Error> {
        let mut guard = self.buf.write().unwrap();
        let data: Vec<u8> = state.into();
        guard.write_tuple_data(&data, Some(&STATE_PTR), None)?;
        Ok(())
    }

    pub fn get_new_id(&self) -> Result<ID, std::io::Error> {
        let mut cur_id = self.cur_id.lock().unwrap();
        *cur_id += 1;
        self.inc_counter(*cur_id, &CUR_ID_PTR)
    }

    pub fn get_new_lsn(&self) -> Result<LSN, std::io::Error> {
        let mut cur_lsn = self.cur_lsn.lock().unwrap();
        *cur_lsn += 1;
        self.inc_counter(*cur_lsn, &CUR_LSN_PTR)
    }

    fn inc_counter(
        &self,
        val: u32,
        ptr: &TuplePtr,
    ) -> Result<u32, std::io::Error> {
        let mut lock = self.buf.write().unwrap();
        let mut data = vec![0u8; 4];
        LittleEndian::write_u32(&mut data, val);
        lock.write_tuple_data(&data[0..4], Some(&ptr), None)?;
        Ok(val)
    }

    const fn default_id_counter() -> [u8; 4] {
        [0u8, 0u8, 0u8, 2u8]
    }
}

// TODO is there a way for this to be a const fn?
fn table_rel_desc() -> TupleDesc {
    TupleDesc::new(
        vec![DataType::VarChar, DataType::U32],
        vec![String::from("table_name"), String::from("rel_id")],
    )
}
