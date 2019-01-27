use std::fs::{File, remove_file};
use std::io::{Write};
use storage::PAGE_SIZE;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;

#[test]
fn test_bufmgr_get() {
    let data_file = "1.dat";

    setup_bufmgr(data_file);
    let mut buf_mgr = BufMgr::new();
    let buf_page = buf_mgr.get_buf(&BufKey::new(1, 0)).unwrap();
    teardown_bufmgr(data_file);

    assert_eq!(buf_page.buf.len() as u64, PAGE_SIZE);
    for byte in buf_page.buf.iter() { assert_eq!(*byte, 0); }
}

#[test]
fn test_bufmgr_store() {
    let data_file = "2.dat";

    setup_bufmgr(data_file);
    let mut buf_mgr = BufMgr::new();
    {
        let buf_page = buf_mgr.get_buf(&BufKey::new(2, 0)).unwrap();
        // Change values in buf_page
        buf_page.buf[0] = 1;
        buf_page.buf[1] = 1;
    }
    // Write buf page
    buf_mgr.store_buf(&BufKey::new(2, 0)).unwrap();

    let mut buf_mgr = BufMgr::new();
    let buf_page = buf_mgr.get_buf(&BufKey::new(2, 0)).unwrap();
    teardown_bufmgr(data_file);

    assert_eq!(buf_page.buf[0], 1);
    assert_eq!(buf_page.buf[1], 1);
}

fn setup_bufmgr(data_file: &str) {
    let mut file = File::create(data_file).unwrap();
    file.write_all(&[0; PAGE_SIZE as usize]).unwrap();
}

fn teardown_bufmgr(data_file: &str) {
    remove_file(data_file).unwrap();
}
