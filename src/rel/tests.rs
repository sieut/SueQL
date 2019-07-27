use data_type::DataType;
use db_state::{DbSettings, DbState};
use error::Result;
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
    let tuples = rel
        .literal_to_data(vec![vec![
            Literal::String("c".to_string()),
            Literal::Integer(1),
        ]])
        .unwrap();
    let ptrs = rel.write_tuples(tuples.clone(), &mut db_state).unwrap();
    assert_eq!(ptrs.len(), 1);
    let ptr = ptrs.get(0).unwrap();

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

#[test]
fn test_rel_lock_macro() -> Result<()> {
    let data_dir = "test_rel_lock_macro";
    let mut db_state = setup(data_dir);

    let desc = TupleDesc::new(
        vec![DataType::Char, DataType::U32],
        vec!["char", "u32"],
    );
    let rel1 =
        Rel::new("test_rel_lock_macro_1", desc.clone(), &mut db_state).unwrap();
    let rel2 =
        Rel::new("test_rel_lock_macro_2", desc.clone(), &mut db_state).unwrap();

    {
        rel_read_lock!(rel1, db_state.buf_mgr);
        rel_write_lock!(rel2, db_state.buf_mgr);
        let rel1_meta = db_state.buf_mgr.get_buf(&rel1.meta_buf_key())?;
        assert!(rel1_meta.try_write().is_err());
    }

    teardown(db_state, data_dir);
    Ok(())
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
