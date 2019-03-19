use std::fs;
use common::{META_REL_ID, TABLE_REL_ID};
use data_type::DataType;
use rel::Rel;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;
use tuple::tuple_desc::TupleDesc;

#[derive(Clone)]
pub struct DbState {
    pub buf_mgr: BufMgr,
    settings: DbSettings,
}

#[derive(Clone)]
pub struct DbSettings {
    pub buf_mgr_size: Option<usize>,
}

impl DbState {
    pub fn start_db(settings: DbSettings) -> Result<DbState, std::io::Error> {
        let mut buf_mgr = BufMgr::new(settings.buf_mgr_size);
        init_db(&mut buf_mgr)?;

        Ok(DbState {
            buf_mgr: buf_mgr,
            settings: settings,
        })
    }
}

impl DbSettings {
    pub fn default() -> DbSettings {
        DbSettings {
            buf_mgr_size: None
        }
    }
}

fn init_db(buf_mgr: &mut BufMgr) -> Result<(), std::io::Error> {
    // Check if meta rel exists and load to buf_mgr
    let meta_key = BufKey::new(META_REL_ID, 0);
    let table_key = BufKey::new(TABLE_REL_ID, 0);
    if fs::metadata(&meta_key.to_filename()).is_ok() {
        buf_mgr.get_buf(&meta_key)?;
        buf_mgr.get_buf(&table_key)?;
    }
    else {
        let meta = buf_mgr.new_buf(&meta_key)?;
        meta.write().unwrap().write_tuple_data(&[0u8; 4], None)?;

        Rel::new(table_rel_desc(), buf_mgr, None)?;
    }

    Ok(())
}

fn table_rel_desc() -> TupleDesc {
    TupleDesc::new(
        vec![DataType::VarChar, DataType::U32],
        vec![String::from("table_name"), String::from("rel_id")])
}
