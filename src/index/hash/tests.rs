use data_type::DataType;
use index::Index;
use super::{HashIndex, HashBucket, HashItem, ITEMS_PER_BUCKET};
use storage::{BufKey, BufType};
use tuple::{TupleDesc, TuplePtr};
use test_utils::{setup, teardown};

#[test]
fn test_insert_and_get_hash() {
    let mut db_state = setup("test_insert_and_get_hash");

    let key_desc = TupleDesc::new(vec![DataType::U32], vec![""]);
    let index = HashIndex::new(0, key_desc, &mut db_state).unwrap();

    let test_buf_key = BufKey::new(0, 0, BufType::Data);
    let test_ptr = TuplePtr::new(test_buf_key.clone(), 4);
    let test_data = vec![1, 2, 3, 4];
    index
        .insert(vec![(&test_data, test_ptr.clone())], &mut db_state)
        .unwrap();
    let return_ptrs = index.get(&test_data, &mut db_state).unwrap();

    teardown(db_state);

    assert_eq!(return_ptrs.len(), 1);
    assert_eq!(return_ptrs[0], test_ptr);
}

#[test]
fn test_split_hash() {
    use bincode;

    let mut db_state = setup("test_split_hash");

    let key_desc = TupleDesc::new(vec![DataType::U32], vec![""]);
    let index = HashIndex::new(0, key_desc, &mut db_state).unwrap();

    let test_buf_key = BufKey::new(0, 0, BufType::Data);
    let bucket_one: Vec<u32> = (0..ITEMS_PER_BUCKET * 2)
        .filter_map(|i| {
            if index.hash(&bincode::serialize(&(i as u32)).unwrap()) % 2 == 0 {
                Some(i as u32)
            } else {
                None
            }
        })
        .collect();
    let bucket_one_data = bucket_one
        .iter()
        .map(|i| bincode::serialize(i).unwrap())
        .collect::<Vec<_>>();
    let bucket_three: Vec<u32> = bucket_one
        .iter()
        .filter_map(|i| {
            if index.hash(&bincode::serialize(i).unwrap()) % 4 == 2 {
                Some(i.clone())
            } else {
                None
            }
        })
        .collect();
    // Make sure the index will split
    assert!(bucket_one.len() > ITEMS_PER_BUCKET);
    let items = bucket_one_data
        .iter()
        .map(|data| (data, TuplePtr::new(test_buf_key.clone(), 0)))
        .collect::<Vec<_>>();
    index.insert(items, &mut db_state).unwrap();

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

    let new_bucket = HashBucket {
        buf_key: BufKey::new(index.file_id, 3, BufType::Data),
        overflow_file_id: index.overflow_file_id,
    };
    let bucket_three_items = bucket_three
        .iter()
        .map(|i| {
            new_bucket.get_items(
                index.hash(&bincode::serialize(i).unwrap()),
                &mut db_state).unwrap()
        })
        .collect::<Vec<_>>();

    teardown(db_state);

    assert_eq!(next, BufKey::new(index.file_id, 2, BufType::Data));
    assert_eq!(level, 1);
    bucket_three
        .iter()
        .zip(bucket_three_items.iter())
        .for_each(|(i, items)| {
            assert_eq!(items.len(), 1);
            let item = items.get(0).unwrap();
            assert_eq!(
                index.hash(&bincode::serialize(i).unwrap()),
                item.hash);
        });
}

#[test]
fn test_insert_and_get_bucket() {
    let mut db_state = setup("test_insert_to_bucket");

    let bucket_file_id = db_state.meta.get_new_id().unwrap();
    let overflow_file_id = db_state.meta.get_new_id().unwrap();
    let _ = db_state.buf_mgr.new_buf(
        &BufKey::new(overflow_file_id, 0, BufType::Data),
    ).unwrap();
    let bucket = HashBucket::new(
        BufKey::new(bucket_file_id, 0, BufType::Data),
        overflow_file_id,
        &mut db_state,
    ).unwrap();
    let items = (0..ITEMS_PER_BUCKET * 3)
        .map(|_| HashItem {
            hash: 0,
            ptr: TuplePtr::new(bucket.buf_key.clone(), 0),
        })
        .collect::<Vec<_>>();
    bucket.write_items(items.clone(), &mut db_state).unwrap();
    let returned_items = bucket.get_items(0, &mut db_state).unwrap();

    let mut buf_key = bucket.buf_key.clone();
    let mut overflow_count = 0;
    while bucket.is_valid_overflow(&buf_key) {
        let page = db_state.buf_mgr.get_buf(&buf_key).unwrap();
        let guard = page.read().unwrap();
        buf_key = bucket.get_items_from_page(0, &guard).unwrap().0;
        overflow_count += 1;
    };

    teardown(db_state);

    items
        .into_iter()
        .zip(returned_items)
        .for_each(|(item1, item2)| assert_eq!(item1, item2));
    assert_eq!(overflow_count, 3);
}
