use data_type::DataType;
use error::Result;
use super::Rel;
use storage::BufType;
use tuple::TupleDesc;
use test_utils::{setup_no_persist, teardown};
use utils;

#[test]
fn test_new_rel() {
    let mut db_state = setup_no_persist("test_new_rel");

    let desc = TupleDesc::new(
        vec![DataType::Char, DataType::U32],
        vec!["char", "u32"],
    );
    let rel = Rel::new("test_new_rel", desc.clone(), &mut db_state).unwrap();
    let returned_id = rel.rel_id;
    let rel = Rel::load(rel.rel_id, BufType::Data, &mut db_state).unwrap();

    let id =
        utils::get_table_id("test_new_rel".to_string(), &mut db_state).unwrap();

    teardown(db_state);

    assert_eq!(rel.tuple_desc(), desc);
    assert_eq!(returned_id, id);
}

#[test]
fn test_write_tuple() {
    use nom_sql::Literal;
    let mut db_state = setup_no_persist("test_write_tuple");

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

    teardown(db_state);

    assert_eq!(tuples[0], written_tuple);
    assert!(lsn != 0);
}

#[test]
fn test_rel_lock_macro() -> Result<()> {
    let mut db_state = setup_no_persist("test_rel_lock_macro");

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

    teardown(db_state);
    Ok(())
}
