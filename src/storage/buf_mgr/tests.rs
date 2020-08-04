use db_state::DbSettings;
use std::io::Write;
use storage::buf_page::HEADER_SIZE;
use storage::{BufKey, BufPage, BufType, PAGE_SIZE};
use super::BufMgr;

#[test]
fn test_bufmgr_get() {
    let data_dir = "test_bufmgr_get";
    let mut buf_mgr = setup_bufmgr(data_dir, None);
    let buf_page = buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    teardown_bufmgr(data_dir);

    let lock = buf_page.read().unwrap();
    assert_eq!(lock.buf().len(), PAGE_SIZE);
}

#[test]
fn test_bufmgr_store() {
    let data_dir = "test_bufmgr_store";
    let mut buf_mgr = setup_bufmgr(data_dir, None);
    {
        let buf_page =
            buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
        // Change values in buf_page
        let mut lock = buf_page.write().unwrap();
        lock.write_tuple_data(&vec![1, 1, 1, 1], None, None)
            .unwrap();
    }
    // Write buf page
    buf_mgr
        .store_buf(&BufKey::new(0, 0, BufType::Data), None)
        .unwrap();

    let mut buf_mgr = BufMgr::new(DbSettings {
        buf_mgr_size: None,
        data_dir: Some(data_dir.to_string()),
    });
    let buf_page = buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    teardown_bufmgr(data_dir);

    let lock = buf_page.read().unwrap();
    assert_eq!(lock.upper_ptr, PAGE_SIZE - 4);
    assert_eq!(lock.lower_ptr, HEADER_SIZE + 4);
    assert_eq!(lock.iter().next().unwrap().to_vec(), vec![1, 1, 1, 1]);
}

#[test]
fn test_bufmgr_new_buf() {
    let data_dir = "test_bufmgr_new_buf";

    let mut buf_mgr = setup_bufmgr(data_dir, None);
    // Next eligible buf_key in file 0 is (0, 1)
    assert!(buf_mgr.new_buf(&BufKey::new(0, 2, BufType::Data)).is_err());
    // Buf already exists (db's meta file)
    assert!(buf_mgr.new_buf(&BufKey::new(0, 0, BufType::Data)).is_err());

    let _buf_page = buf_mgr.new_buf(&BufKey::new(0, 1, BufType::Data)).unwrap();
    let _temp_page =
        buf_mgr.new_buf(&BufKey::new(0, 0, BufType::Temp)).unwrap();
    let _mem_page = buf_mgr.new_buf(&BufKey::new(0, 0, BufType::Mem)).unwrap();
    teardown_bufmgr(data_dir);
}

#[test]
fn test_bufmgr_evict() {
    let data_dir = "test_bufmgr_evict";

    // BufMgr of max 3 pages
    let mut buf_mgr = setup_bufmgr(data_dir, Some(3));
    buf_mgr.new_buf(&BufKey::new(0, 1, BufType::Data)).unwrap();
    buf_mgr.new_buf(&BufKey::new(0, 2, BufType::Data)).unwrap();
    buf_mgr.new_buf(&BufKey::new(0, 3, BufType::Data)).unwrap();

    // Queue: page-1  page-2  page-3
    // Ref:     1       1       1
    buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 1, BufType::Data)));

    // Queue: page-2  page-3  page-0
    // Ref:     0       0       1
    buf_mgr.get_buf(&BufKey::new(0, 1, BufType::Data)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 2, BufType::Data)));

    // Queue: page-3  page-0  page-1
    // Ref:     0       1       1
    buf_mgr.get_buf(&BufKey::new(0, 3, BufType::Data)).unwrap();

    // Queue: page-3  page-0  page-1
    // Ref:     1       1       1
    buf_mgr.get_buf(&BufKey::new(0, 2, BufType::Data)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 3, BufType::Data)));

    // Queue: page-0  page-1  page-2
    // Ref:     0       0       1
    buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    buf_mgr.get_buf(&BufKey::new(0, 3, BufType::Data)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 1, BufType::Data)));

    // Queue: page-2  page-0  page-3
    // Ref:     1       0       1
    let _buf_two = buf_mgr.get_buf(&BufKey::new(0, 2, BufType::Data)).unwrap();
    buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    buf_mgr.get_buf(&BufKey::new(0, 1, BufType::Data)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 0, BufType::Data)));
    teardown_bufmgr(data_dir);
}

#[test]
fn test_bufmgr_ref() {
    let data_dir = "test_bufmgr_ref";

    let mut buf_mgr = setup_bufmgr(data_dir, None);
    let mut clone = buf_mgr.clone();
    // Have 2 clones to make sure refs don't go up with BufMgr clones
    let _clone2 = buf_mgr.clone();

    let buf = buf_mgr.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    let _buf_clone = clone.get_buf(&BufKey::new(0, 0, BufType::Data)).unwrap();
    assert_eq!(buf.ref_count(), 3);
    teardown_bufmgr(data_dir);
}

#[test]
fn test_allocate_mem_bufs() {
    let data_dir = "allocate_mem_bufs";

    let mut buf_mgr = setup_bufmgr(data_dir, None);
    let pages = buf_mgr.allocate_mem_bufs(Some(200)).unwrap();
    assert_eq!(pages.len(), 200);
    teardown_bufmgr(data_dir);
}

fn setup_bufmgr(data_dir: &str, buf_mgr_size: Option<usize>) -> BufMgr {
    use std::fs::{create_dir, File};
    use std::io::ErrorKind;

    let temp_dir: String = format!("{}/temp", data_dir);
    match create_dir(data_dir) {
        Ok(_) => {}
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => {}
            _ => panic!("Error when setting up test: {:?}", e),
        },
    };
    match create_dir(temp_dir) {
        Ok(_) => {}
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => {}
            _ => panic!("Error when setting up test: {:?}", e),
        },
    };

    let mut file = File::create(format!("{}/0.dat", data_dir)).unwrap();
    file.write_all(&BufPage::default_buf()).unwrap();

    let settings = DbSettings {
        buf_mgr_size,
        data_dir: Some(data_dir.to_string()),
    };

    BufMgr::new(settings)
}

fn teardown_bufmgr(data_dir: &str) {
    use std::fs::remove_dir_all;
    remove_dir_all(data_dir).unwrap();
}
