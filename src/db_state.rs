use error::{Error, Result};
use log::LogMgr;
use meta::Meta;
use serde::{Deserialize, Serialize};
use storage::BufMgr;

#[derive(Clone, Debug)]
pub struct DbState {
    pub buf_mgr: BufMgr,
    pub log_mgr: LogMgr,
    pub meta: Meta,
    pub settings: DbSettings,
}

impl DbState {
    pub fn start_db(settings: DbSettings) -> Result<DbState> {
        let data_dir = settings.get_data_dir();
        DbState::create_data_dir(data_dir)?;

        dbg_log!("Starting SueQL database");
        let mut buf_mgr = BufMgr::new(settings.clone());
        let log_mgr = LogMgr::create_and_load(&mut buf_mgr)?;
        let meta = Meta::create_and_load(&mut buf_mgr)?;
        meta.set_state(State::Up)?;

        buf_mgr.start_persist(&meta, &log_mgr)?;

        Ok(DbState {
            buf_mgr,
            log_mgr,
            meta,
            settings,
        })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        // Set state on disk to down
        self.meta.set_state(State::Down)?;
        // Persist one last time
        // NOTE: might be slow and extra here if BufMgr is already persisting
        self.buf_mgr.persist()?;
        Ok(())
    }

    fn create_data_dir<S: Into<String>>(data_dir: S) -> Result<()> {
        use std::fs::create_dir;
        use std::io::ErrorKind;

        let data_dir = data_dir.into();
        let temp_dir = format!("{}/temp", data_dir);
        match create_dir(data_dir) {
            Ok(_) => {}
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => {}
                _ => {
                    return Err(Error::from(e));
                }
            },
        };
        match create_dir(temp_dir) {
            Ok(_) => {}
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => {}
                _ => {
                    return Err(Error::from(e));
                }
            },
        };

        Ok(())
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

    pub fn data_dir<S>(mut self, dir: S) -> DbSettings
    where
        S: Into<String>,
    {
        self.data_dir = Some(dir.into());
        self
    }

    pub fn get_data_dir(&self) -> String {
        self.data_dir.clone().unwrap_or("data".to_string())
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum State {
    Up,
    Down,
}
