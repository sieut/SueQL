extern crate num;
use self::num::FromPrimitive;

use log::LogMgr;
use meta::Meta;
use storage::BufMgr;

#[derive(Clone, Debug)]
pub struct DbState {
    pub buf_mgr: BufMgr,
    pub log_mgr: LogMgr,
    pub meta: Meta,
    pub settings: DbSettings,
}

impl DbState {
    pub fn start_db(settings: DbSettings) -> Result<DbState, std::io::Error> {
        let data_dir = settings.clone().data_dir.unwrap_or("data".to_string());
        DbState::create_data_dir(data_dir)?;

        dbg_log!("Starting SueQL database");
        let mut buf_mgr = BufMgr::new(settings.clone());
        let log_mgr = LogMgr::create_and_load(&mut buf_mgr)?;
        let meta = Meta::create_and_load(&mut buf_mgr)?;
        meta.set_state(State::Up)?;

        buf_mgr.start_persist(&log_mgr)?;

        Ok(DbState {
            buf_mgr,
            log_mgr,
            meta,
            settings,
        })
    }

    pub fn shutdown(&mut self) -> Result<(), std::io::Error> {
        // Set state on disk to down
        self.meta.set_state(State::Down)?;
        // Persist one last time
        // NOTE: might be slow and extra here if BufMgr is already persisting
        self.buf_mgr.persist()?;
        Ok(())
    }

    fn create_data_dir<S: Into<String>>(
        data_dir: S,
    ) -> Result<(), std::io::Error> {
        use std::fs::create_dir;
        use std::io::ErrorKind;

        let data_dir = data_dir.into();
        match create_dir(data_dir) {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => Ok(()),
                _ => Err(e),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct DbSettings {
    pub buf_mgr_size: Option<usize>,
    pub data_dir: Option<String>,
}

impl DbSettings {
    pub fn default() -> DbSettings {
        DbSettings {
            buf_mgr_size: None,
            data_dir: None,
        }
    }
}

enum_from_primitive! {
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
