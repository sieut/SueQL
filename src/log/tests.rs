use data_type::DataType;
use db_state::{DbSettings, DbState};
use log::{LogEntry, LogMgr, OpType, LOG_REL_ID};
use meta::Meta;
use rel::Rel;
use storage::{BufKey, BufMgr};
use tuple::TupleDesc;

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

#[test]
fn test_recovery() {
    use nom_sql::Literal;

    let data_dir = "test_recovery";
    let mut db_state = setup(data_dir);
    // Create a Rel
    let desc = TupleDesc::new(
        vec![DataType::Char, DataType::U32],
        vec!["char", "u32"],
    );
    let rel = Rel::new("rel", desc.clone(), &mut db_state).unwrap();
    let rel_id = rel.rel_id;
    // Persist the Rel creation
    db_state.buf_mgr.persist().unwrap();
    // Insert 2 tuples, there will be 2 uncheckpointed entries after this
    let tuples = rel.data_from_literal(vec![
        vec![Literal::String("a".to_string()), Literal::Integer(1)],
        vec![Literal::String("b".to_string()), Literal::Integer(2)],
    ]);
    rel.write_new_tuple(&tuples[0], &mut db_state).unwrap();
    rel.write_new_tuple(&tuples[1], &mut db_state).unwrap();

    // Restart db, basically
    let mut db_state = setup(data_dir);
    // Load the Rel and check for the tuples
    let rel = Rel::load(rel_id, &mut db_state).unwrap();
    let mut written_tuples = vec![];
    rel.scan(
        &mut db_state,
        |_| true,
        |data| {
            written_tuples.push(data.to_vec());
        },
    )
    .unwrap();

    teardown(db_state, data_dir);

    assert_eq!(written_tuples.len(), 2);
    let data1 = rel.data_to_strings(&written_tuples[0], None).unwrap();
    assert_eq!(data1[0], "a");
    assert_eq!(data1[1], "1");
    let data2 = rel.data_to_strings(&written_tuples[1], None).unwrap();
    assert_eq!(data2[0], "b");
    assert_eq!(data2[1], "2");
}

#[test]
#[ignore]
// TODO This test does not pass because new rel is not an OpType
fn test_recover_new_rel() {
    let data_dir = "test_recover_new_rel";
    let mut db_state = setup(data_dir);
    // Persist the DB creation
    db_state.buf_mgr.persist().unwrap();
    // Create a Rel
    let desc = TupleDesc::new(
        vec![DataType::Char, DataType::U32],
        vec!["char", "u32"],
    );
    let rel = Rel::new("rel", desc.clone(), &mut db_state).unwrap();
    let rel_id = rel.rel_id;
    // Restart db, basically
    let mut db_state = setup(data_dir);
    // Load the Rel
    let rel = Rel::load(rel_id, &mut db_state).unwrap();
    teardown(db_state, data_dir);

    assert_eq!(rel.tuple_desc(), desc);
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

fn teardown(mut db_state: DbState, data_dir: &str) {
    use std::fs::remove_dir_all;
    db_state.shutdown().unwrap();
    remove_dir_all(data_dir).unwrap();
}
