extern crate byteorder;
use self::byteorder::ByteOrder;
use self::byteorder::{LittleEndian, ReadBytesExt};

use std::io::Cursor;
use storage::PAGE_SIZE;
use storage::buf_page::BufPage;
use storage::buf_key::BufKey;
use tuple::tuple_ptr::TuplePtr;

#[test]
fn test_write_new_tuple() {
    let mut buffer: [u8; PAGE_SIZE as usize] = [0; PAGE_SIZE as usize];
    LittleEndian::write_u32(&mut buffer[0..4], PAGE_SIZE);
    LittleEndian::write_u32(&mut buffer[4..8], 8);

    let mut buf_page = BufPage::load_from(
        &buffer, &BufKey::new(0, 0)).unwrap();

    let test_data = [5; 16];
    let buf_offset = buf_page.write_tuple_data(&test_data, None).unwrap();
    assert_eq!(buf_offset, 8);

    let read_lock = buf_page.buf.read().unwrap();
    let mut reader = Cursor::new(&read_lock[0..12]);
    // upper_ptr
    assert_eq!(reader.read_u32::<LittleEndian>().unwrap(), PAGE_SIZE - 16);
    // lower_ptr
    assert_eq!(reader.read_u32::<LittleEndian>().unwrap(), 8 + 4);
    // tuple_ptr
    assert_eq!(reader.read_u32::<LittleEndian>().unwrap(), PAGE_SIZE - 16);
    // tuple_data
    for byte in read_lock[(PAGE_SIZE - 16) as usize
            ..PAGE_SIZE as usize].iter() {
        assert_eq!(*byte, 5);
    }
}

#[test]
fn test_get_tuple_range() {
    let mut buffer: [u8; PAGE_SIZE as usize] = [0; PAGE_SIZE as usize];
    // upper_ptr
    LittleEndian::write_u32(&mut buffer[0..4], PAGE_SIZE - 16);
    // lower_ptr
    LittleEndian::write_u32(&mut buffer[4..8], 12);
    // tuple_ptr 1
    LittleEndian::write_u32(&mut buffer[8..12], PAGE_SIZE - 8);
    // tuple_ptr 2
    LittleEndian::write_u32(&mut buffer[12..16], PAGE_SIZE - 16);

    let buf_page = BufPage::load_from(
        &buffer, &BufKey::new(0, 0)).unwrap();

    let invalid_tuple_ptr = TuplePtr::new(BufKey::new(0, 1), 8);
    assert!(buf_page.get_tuple_data_range(&invalid_tuple_ptr).is_err());

    let tuple_ptr_1 = TuplePtr::new(BufKey::new(0, 0), 8);
    let tuple_range_1 = buf_page.get_tuple_data_range(&tuple_ptr_1).unwrap();
    assert_eq!(tuple_range_1.start as u32, PAGE_SIZE - 8);
    assert_eq!(tuple_range_1.end as u32, PAGE_SIZE);

    let tuple_ptr_2 = TuplePtr::new(BufKey::new(0, 0), 12);
    let tuple_range_2 = buf_page.get_tuple_data_range(&tuple_ptr_2).unwrap();
    assert_eq!(tuple_range_2.start as u32, PAGE_SIZE - 16);
    assert_eq!(tuple_range_2.end as u32, PAGE_SIZE - 8);
}
