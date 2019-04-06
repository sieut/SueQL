use db_state::{DbSettings, DbState};
use log::{LogEntry, OpType, LOG_REL_ID};
use storage::BufKey;

#[test]
fn test_write_log_entries() {
    let data_dir = "test_write_log_entries";
    let mut db_state = setup(data_dir);

    let entry = LogEntry::new(
        BufKey::new(3, 1),
        OpType::InsertTuple,
        vec![0u8; 4],
        &mut db_state,
    )
    .unwrap();

    let entry_ptr = db_state
        .log_mgr
        .write_entries(vec![entry.clone()], &mut db_state.buf_mgr)
        .unwrap();

    let log_page = db_state
        .buf_mgr
        .get_buf(&BufKey::new(LOG_REL_ID, 1))
        .unwrap();
    let guard = log_page.read().unwrap();
    let written_entry =
        LogEntry::load(guard.get_tuple_data(&entry_ptr[0]).unwrap().to_vec())
            .unwrap();

    teardown(db_state, data_dir);

    assert_eq!(entry_ptr.len(), 1);

    assert_eq!(written_entry.header, entry.header);
    assert_eq!(written_entry.data, entry.data);
}

#[test]
fn test_log_checkpoints() {
    let data_dir = "test_log_checkpoints";
    let mut db_state = setup(data_dir);

    let entry = LogEntry::new(
        BufKey::new(3, 1),
        OpType::InsertTuple,
        vec![0u8; 4],
        &mut db_state,
    )
    .unwrap();

    // Create first ever checkpoint
    let cp_1 = db_state
        .log_mgr
        .create_checkpoint(&mut db_state.buf_mgr)
        .unwrap();
    db_state
        .log_mgr
        .confirm_checkpoint(cp_1, &mut db_state.buf_mgr)
        .unwrap();

    // A checkpoint won't be created because there's no new entries
    let cp_2 = db_state
        .log_mgr
        .create_checkpoint(&mut db_state.buf_mgr)
        .unwrap();

    // A new checkpoint because there is a new entry
    db_state
        .log_mgr
        .write_entries(vec![entry], &mut db_state.buf_mgr)
        .unwrap();
    let cp_3 = db_state
        .log_mgr
        .create_checkpoint(&mut db_state.buf_mgr)
        .unwrap();
    db_state
        .log_mgr
        .confirm_checkpoint(cp_3, &mut db_state.buf_mgr)
        .unwrap();

    teardown(db_state, data_dir);

    assert_eq!(cp_1.buf_key, BufKey::new(LOG_REL_ID, 1));
    assert_eq!(cp_1.buf_offset, 0);

    assert_eq!(cp_2.buf_key, BufKey::new(LOG_REL_ID, 0));
    assert_eq!(cp_2.buf_offset, 0);

    assert_eq!(cp_3.buf_key, BufKey::new(LOG_REL_ID, 1));
    assert_eq!(cp_3.buf_offset, 2);
}

fn setup(data_dir: &str) -> DbState {
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

    DbState::start_db(settings).unwrap()
}

fn teardown(mut db_state: DbState, data_dir: &str) {
    use std::fs::remove_dir_all;
    db_state.shutdown().unwrap();
    remove_dir_all(data_dir).unwrap();
}
