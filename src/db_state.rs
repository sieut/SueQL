extern crate num;
use self::num::FromPrimitive;

use std::fs;
use common;
use data_type::DataType;
use rel::Rel;
use storage::buf_mgr::BufMgr;
use tuple::tuple_desc::TupleDesc;

#[derive(Clone, Debug)]
pub struct DbState {
    pub buf_mgr: BufMgr,
    settings: DbSettings,
}

impl DbState {
    pub fn start_db(settings: DbSettings) -> Result<DbState, std::io::Error> {
        let mut db_state = DbState {
            buf_mgr: BufMgr::new(settings.buf_mgr_size),
            settings: settings,
        };
        DbState::init_db(&mut db_state.buf_mgr)?;
        common::set_state(&mut db_state.buf_mgr, State::Up, None)?;
        Ok(db_state)
    }

    pub fn shutdown(&mut self) -> Result<(), std::io::Error> {
        // Persist one last time
        // NOTE: might be slow and extra here if BufMgr is already persisting
        self.buf_mgr.persist()?;
        // Set state on disk to down
        common::set_state(&mut self.buf_mgr, State::Down, None)?;
        Ok(())
    }

    fn init_db(buf_mgr: &mut BufMgr) -> Result<(), std::io::Error> {
        // Check if meta rel exists and load to buf_mgr
        if fs::metadata(&common::META_BUF_KEY.to_filename()).is_ok() {
            buf_mgr.get_buf(&common::META_BUF_KEY)?;
            buf_mgr.get_buf(&common::TABLE_BUF_KEY)?;
        }
        else {
            {
                let meta = buf_mgr.new_buf(&common::META_BUF_KEY)?;
                let mut guard = meta.write().unwrap();
                // State
                let state_data: Vec<u8> = State::Down.into();
                guard.write_tuple_data(&state_data, None)?;
                // ID Counter
                guard.write_tuple_data(&[0u8; 4], None)?;
                // LSN Counter
                guard.write_tuple_data(&[0u8; 4], None)?;
            }

            Rel::new(table_rel_desc(), buf_mgr, None)?;
        }

        Ok(())
    }

}

#[derive(Clone, Debug)]
pub struct DbSettings {
    pub buf_mgr_size: Option<usize>,
}

impl DbSettings {
    pub fn default() -> DbSettings {
        DbSettings {
            buf_mgr_size: None
        }
    }
}

enum_from_primitive!{
    #[derive(Debug, Copy, Clone)]
    pub enum State {
        Up,
        Down
    }
}

impl From<State> for Vec<u8> {
    fn from(state: State) -> Self {
        vec![state as u8]
    }
}

impl From<&[u8]> for State {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() > 1);
        State::from_u8(bytes[0]).unwrap()
    }
}

fn table_rel_desc() -> TupleDesc {
    TupleDesc::new(
        vec![DataType::VarChar, DataType::U32],
        vec![String::from("table_name"), String::from("rel_id")])
}
