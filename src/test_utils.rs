use db_state::{DbState, DbSettings};
use storage::BufMgr;
use log::LogMgr;
use meta::Meta;

pub fn setup_no_persist(data_dir: &str) -> DbState {
    // Similar to start_db but persist loop is not started
    use std::fs::create_dir;
    use std::io::ErrorKind;

    match create_dir(data_dir) {
        Ok(_) => {}
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => {}
            _ => panic!("Error when setting up test: {:?}", e),
        },
    };

    let settings = DbSettings {
        buf_mgr_size: None,
        data_dir: Some(data_dir.to_string()),
    };

    let mut buf_mgr = BufMgr::new(settings.clone());
    let log_mgr = LogMgr::create_and_load(&mut buf_mgr).unwrap();
    let meta = Meta::create_and_load(&mut buf_mgr).unwrap();

    DbState {
        buf_mgr,
        log_mgr,
        meta,
        settings,
    }
}

pub fn teardown(mut db_state: DbState) {
    use std::fs::remove_dir_all;
    db_state.shutdown().unwrap();
    remove_dir_all(db_state.settings.get_data_dir()).unwrap();
}
