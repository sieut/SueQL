use data_type::DataType;
use db_state::{DbSettings, DbState};
use index::HashIndex;
use storage::{BufKey, BufType};
use tuple::{TupleDesc, TuplePtr};

#[test]
fn test_insert_and_get_hash() {
    let settings = DbSettings::default().data_dir("test_insert_and_get_hash");
    let mut db_state = DbState::start_db(settings).unwrap();

    let key_desc = TupleDesc::new(vec![DataType::U32], vec![""]);
    let index = HashIndex::new(0, key_desc, &mut db_state).unwrap();

    let test_buf_key = BufKey::new(0, 0, BufType::Data);
    let test_ptr = TuplePtr::new(test_buf_key.clone(), 4);
    let test_data = vec![1, 2, 3, 4];
    index
        .insert(&test_data, test_ptr.clone(), &mut db_state)
        .unwrap();
    let return_ptr = index.get(&test_data, &mut db_state).unwrap();

    teardown(db_state);

    assert_eq!(test_ptr, return_ptr);
}

#[test]
fn test_split_hash() {
    use bincode;
    use sha2::{Digest, Sha256};

    let settings = DbSettings::default().data_dir("test_split_hash");
    let mut db_state = DbState::start_db(settings).unwrap();

    let key_desc = TupleDesc::new(vec![DataType::U32], vec![""]);
    let index = HashIndex::new(0, key_desc, &mut db_state).unwrap();

    let test_buf_key = BufKey::new(0, 0, BufType::Data);
    let bucket_one: Vec<u32> = (0..190)
        .filter_map(|i| {
            if Sha256::digest(&bincode::serialize(&(i as u32)).unwrap())[31] % 2
                == 0
            {
                Some(i as u32)
            } else {
                None
            }
        })
        .collect();
    // Make sure the index will split
    assert!(bucket_one.len() > 80);

    for i in bucket_one.iter() {
        index
            .insert(
                &bincode::serialize(&(*i as u32)).unwrap(),
                TuplePtr::new(test_buf_key.clone(), *i as usize),
                &mut db_state,
            )
            .unwrap();
    }

    let meta_key = BufKey::new(index.file_id, 0, BufType::Data);
    let meta_page = db_state.buf_mgr.get_buf(&meta_key).unwrap();
    let guard = meta_page.read().unwrap();
    let next: BufKey = bincode::deserialize(
        guard
            .get_tuple_data(&TuplePtr::new(meta_key.clone(), 2))
            .unwrap(),
    )
    .unwrap();
    let level: u32 = bincode::deserialize(
        guard
            .get_tuple_data(&TuplePtr::new(meta_key.clone(), 3))
            .unwrap(),
    )
    .unwrap();

    let new_page_ok = db_state
        .buf_mgr
        .get_buf(&BufKey::new(index.file_id, 3, BufType::Data))
        .is_ok();

    teardown(db_state);

    assert_eq!(next, BufKey::new(index.file_id, 2, BufType::Data));
    assert_eq!(level, 1);
    assert!(new_page_ok);
}

fn teardown(mut db_state: DbState) {
    use std::fs::remove_dir_all;
    db_state.shutdown().unwrap();
    remove_dir_all(db_state.settings.data_dir.unwrap()).unwrap();
}
