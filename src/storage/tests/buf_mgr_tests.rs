use db_state::DbSettings;
use std::io::Write;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;
use storage::buf_page::{BufPage, HEADER_SIZE};
use storage::PAGE_SIZE;

#[test]
fn test_bufmgr_get() {
    let data_dir = "test_bufmgr_get";
    let mut buf_mgr = setup_bufmgr(data_dir, None);
    let buf_page = buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
    teardown_bufmgr(data_dir);

    let lock = buf_page.read().unwrap();
    assert_eq!(lock.buf().len(), PAGE_SIZE);
}

#[test]
fn test_bufmgr_store() {
    let data_dir = "test_bufmgr_store";
    let mut buf_mgr = setup_bufmgr(data_dir, None);
    {
        let buf_page = buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
        // Change values in buf_page
        let mut lock = buf_page.write().unwrap();
        lock.write_tuple_data(&vec![1, 1, 1, 1], None).unwrap();
    }
    // Write buf page
    buf_mgr.store_buf(&BufKey::new(0, 0), None).unwrap();

    let mut buf_mgr = BufMgr::new(
        DbSettings {
            buf_mgr_size: None,
            data_dir: Some(data_dir.to_string())
        });
    let buf_page = buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
    teardown_bufmgr(data_dir);

    let lock = buf_page.read().unwrap();
    assert_eq!(lock.upper_ptr(), PAGE_SIZE - 4);
    assert_eq!(lock.lower_ptr(), HEADER_SIZE + 4);
    assert_eq!(lock.iter().next().unwrap().to_vec(), vec![1, 1, 1, 1]);
}

#[test]
fn test_bufmgr_new_buf() {
    let data_dir = "test_bufmgr_new_buf";

    let mut buf_mgr = setup_bufmgr(data_dir, None);
    assert!(buf_mgr.new_buf(&BufKey::new(0, 2)).is_err());
    assert!(buf_mgr.new_buf(&BufKey::new(0, 0)).is_err());

    let _buf_page = buf_mgr.new_buf(&BufKey::new(0, 1)).unwrap();
    teardown_bufmgr(data_dir);
}

#[test]
fn test_bufmgr_evict() {
    let data_dir = "test_bufmgr_evict";

    // BufMgr of max 3 pages
    let mut buf_mgr = setup_bufmgr(data_dir, Some(3));
    buf_mgr.new_buf(&BufKey::new(0, 1)).unwrap();
    buf_mgr.new_buf(&BufKey::new(0, 2)).unwrap();
    buf_mgr.new_buf(&BufKey::new(0, 3)).unwrap();

    // Queue: page-1  page-2  page-3
    // Ref:     1       1       1
    buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 1)));

    // Queue: page-2  page-3  page-0
    // Ref:     0       0       1
    buf_mgr.get_buf(&BufKey::new(0, 1)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 2)));

    // Queue: page-3  page-0  page-1
    // Ref:     0       1       1
    buf_mgr.get_buf(&BufKey::new(0, 3)).unwrap();

    // Queue: page-3  page-0  page-1
    // Ref:     1       1       1
    buf_mgr.get_buf(&BufKey::new(0, 2)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 3)));

    // Queue: page-0  page-1  page-2
    // Ref:     0       0       1
    buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
    buf_mgr.get_buf(&BufKey::new(0, 3)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 1)));

    // Queue: page-2  page-0  page-3
    // Ref:     1       0       1
    let _buf_two = buf_mgr.get_buf(&BufKey::new(0, 2)).unwrap();
    buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
    buf_mgr.get_buf(&BufKey::new(0, 1)).unwrap();
    assert!(!buf_mgr.has_buf(&BufKey::new(0, 0)));
    teardown_bufmgr(data_dir);
}

#[test]
fn test_bufmgr_ref() {
    let data_dir = "test_bufmgr_ref";

    let mut buf_mgr = setup_bufmgr(data_dir, None);
    let mut clone = buf_mgr.clone();

    let buf = buf_mgr.get_buf(&BufKey::new(0, 0)).unwrap();
    let _buf_clone = clone.get_buf(&BufKey::new(0, 0)).unwrap();
    assert_eq!(buf.ref_count(), 3);
    teardown_bufmgr(data_dir);
}

fn setup_bufmgr(data_dir: &str, buf_mgr_size: Option<usize>) -> BufMgr {
    use std::fs::{create_dir, File};
    use std::io::ErrorKind;

    match create_dir(data_dir) {
        Ok(_) => {},
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => {},
            _ => panic!("Error when setting up test: {:?}", e)
        }
    };

    let mut file = File::create(format!("{}/0.dat", data_dir)).unwrap();
    file.write_all(&BufPage::default_buf()).unwrap();

    let settings = DbSettings {
        buf_mgr_size,
        data_dir: Some(data_dir.to_string())
    };

    BufMgr::new(settings)
}

fn teardown_bufmgr(data_dir: &str) {
    use std::fs::remove_dir_all;
    remove_dir_all(data_dir).unwrap();
}
