use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;
use storage::PAGE_SIZE;
use tuple::tuple_ptr::TuplePtr;

#[test]
fn test_write_new_tuple() {
    use storage::buf_page::HEADER_SIZE;

    let mut buf_page = new_page();
    let test_data = [5; 16];
    let tuple_ptr = buf_page.write_tuple_data(&test_data, None, None).unwrap();
    assert_eq!(tuple_ptr.buf_offset, 0);

    let mut reader = Cursor::new(&buf_page.buf()[4..10]);
    // upper_ptr
    assert_eq!(
        reader.read_u16::<LittleEndian>().unwrap() as usize,
        PAGE_SIZE - 16
    );
    // lower_ptr
    assert_eq!(
        reader.read_u16::<LittleEndian>().unwrap() as usize,
        HEADER_SIZE + 4
    );
    // tuple_ptr
    assert_eq!(
        reader.read_u16::<LittleEndian>().unwrap() as usize,
        PAGE_SIZE - 16
    );
    // tuple_data
    for byte in
        buf_page.buf()[(PAGE_SIZE - 16) as usize..PAGE_SIZE as usize].iter()
    {
        assert_eq!(*byte, 5);
    }
}

#[test]
fn test_get_tuple_data() {
    let mut buf_page = new_page();
    buf_page.write_tuple_data(&[2u8; 8], None, None).unwrap();
    buf_page.write_tuple_data(&[1u8; 16], None, None).unwrap();

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
    buf_page.write_tuple_data(&[0u8; 8], None, None).unwrap();
    buf_page.write_tuple_data(&[0u8; 16], None, None).unwrap();

    let mut iter = buf_page.iter();
    assert_eq!(iter.next().unwrap().len(), 8);
    assert_eq!(iter.next().unwrap().len(), 16);
}

fn new_page() -> BufPage {
    let mut buffer = [0u8; PAGE_SIZE];
    buffer.copy_from_slice(&BufPage::default_buf());
    BufPage::load_from(&buffer, &BufKey::new(0, 0)).unwrap()
}
