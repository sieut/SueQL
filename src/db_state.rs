use std::fs;
use common::META_REL_ID;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;

pub struct DbState {
    pub buf_mgr: BufMgr,
    settings: DbSettings,
}

pub struct DbSettings {
    pub buf_mgr_size: Option<usize>,
}

impl DbState {
    pub fn start_db(settings: DbSettings) -> Result<DbState, std::io::Error> {
        let mut buf_mgr = BufMgr::new(settings.buf_mgr_size);

        // Check if meta rel exists and load to buf_mgr
        let meta_key = BufKey::new(META_REL_ID, 0);
        if fs::metadata(&meta_key.to_filename()).is_ok() {
            buf_mgr.get_buf(&meta_key)?;
        }
        else {
            buf_mgr.new_buf(&meta_key)?;
        }

        Ok(DbState {
            buf_mgr: buf_mgr,
            settings: settings,
        })
    }
}
