use data_type::DataType;
use db_state::{DbSettings, DbState};
use log::LogMgr;
use meta::Meta;
use rel::Rel;
use storage::{BufMgr, BufType};
use tuple::TupleDesc;
use utils;

#[test]
fn test_new_rel() {
    let data_dir = "test_new_rel";
    let mut db_state = setup(data_dir);

    let desc = TupleDesc::new(
        vec![DataType::Char, DataType::U32],
        vec!["char", "u32"],
    );
    let rel = Rel::new("test_new_rel", desc.clone(), &mut db_state).unwrap();
    let returned_id = rel.rel_id;
    let rel = Rel::load(rel.rel_id, BufType::Data, &mut db_state).unwrap();

    let id =
        utils::get_table_id("test_new_rel".to_string(), &mut db_state).unwrap();

    teardown(db_state, data_dir);

    assert_eq!(rel.tuple_desc(), desc);
    assert_eq!(returned_id, id);
}

#[test]
fn test_write_tuple() {
    use nom_sql::Literal;

    let data_dir = "test_write_tuple";
    let mut db_state = setup(data_dir);

    let desc = TupleDesc::new(
        vec![DataType::Char, DataType::U32],
        vec!["char", "u32"],
    );
    let rel = Rel::new("test_write_tuple", desc, &mut db_state).unwrap();
    let tuples = rel.data_from_literal(vec![vec![
        Literal::String("c".to_string()),
        Literal::Integer(1),
    ]]);
    let ptr = rel.write_new_tuple(&tuples[0], &mut db_state).unwrap();

    let written_tuple;
    let lsn;
    {
        let page = db_state.buf_mgr.get_buf(&ptr.buf_key).unwrap();
        let guard = page.read().unwrap();
        written_tuple = guard.get_tuple_data(&ptr).unwrap().to_vec();
        lsn = guard.lsn;
    }

    teardown(db_state, data_dir);

    assert_eq!(tuples[0], written_tuple);
    assert!(lsn != 0);
}

fn setup(data_dir: &str) -> DbState {
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

fn teardown(mut db_state: DbState, data_dir: &str) {
    use std::fs::remove_dir_all;
    db_state.shutdown().unwrap();
    remove_dir_all(data_dir).unwrap();
}
