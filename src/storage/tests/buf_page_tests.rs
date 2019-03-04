use std::io::Cursor;
use byteorder::ByteOrder;
use byteorder::{LittleEndian, ReadBytesExt};
use storage::PAGE_SIZE;
use storage::buf_page::BufPage;
use storage::buf_key::BufKey;
use tuple::tuple_ptr::TuplePtr;

#[test]
fn test_write_new_tuple() {
    let mut buffer: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
    LittleEndian::write_u32(&mut buffer[0..4], PAGE_SIZE as u32);
    LittleEndian::write_u32(&mut buffer[4..8], 8);

    let mut buf_page = BufPage::load_from(
        &buffer, &BufKey::new(0, 0)).unwrap();

    let test_data = [5; 16];
    let buf_offset = buf_page.write_tuple_data(&test_data, None).unwrap();
    assert_eq!(buf_offset, 0);

    let mut reader = Cursor::new(&buf_page.buf()[0..12]);
    // upper_ptr
    assert_eq!(reader.read_u32::<LittleEndian>().unwrap() as usize,
               PAGE_SIZE - 16);
    // lower_ptr
    assert_eq!(reader.read_u32::<LittleEndian>().unwrap() as usize,
               8 + 4);
    // tuple_ptr
    assert_eq!(reader.read_u32::<LittleEndian>().unwrap() as usize,
               PAGE_SIZE - 16);
    // tuple_data
    for byte in buf_page.buf()[(PAGE_SIZE - 16) as usize
            ..PAGE_SIZE as usize].iter() {
        assert_eq!(*byte, 5);
    }
}

#[test]
fn test_get_tuple_data() {
    let mut buf_page = new_page();
    buf_page.write_tuple_data(&[2u8; 8], None);
    buf_page.write_tuple_data(&[1u8; 16], None);

    let invalid_tuple_ptr = TuplePtr::new(BufKey::new(0, 1), 4);
    assert!(buf_page.get_tuple_data(&invalid_tuple_ptr).is_err());

    let tuple_ptr_1 = TuplePtr::new(BufKey::new(0, 0), 0);
    let tuple = buf_page.get_tuple_data(&tuple_ptr_1).unwrap();
    assert_eq!(tuple, &[2u8; 8]);

    let tuple_ptr_2 = TuplePtr::new(BufKey::new(0, 0), 1);
    let tuple = buf_page.get_tuple_data(&tuple_ptr_2).unwrap();
    assert_eq!(tuple, &[1u8; 16]);
}

#[test]
fn test_buf_page_iter() {
    let mut buf_page = new_page();
    buf_page.write_tuple_data(&[0u8; 8], None).unwrap();
    buf_page.write_tuple_data(&[0u8; 16], None).unwrap();

    let mut iter = buf_page.iter();
    assert_eq!(iter.next().unwrap().len(), 8);
    assert_eq!(iter.next().unwrap().len(), 16);
}

fn new_page() -> BufPage {
    let mut buffer = [0u8; PAGE_SIZE];
    buffer.copy_from_slice(&BufPage::default_buf());
    BufPage::load_from(
        &buffer, &BufKey::new(0, 0)).unwrap()
}
