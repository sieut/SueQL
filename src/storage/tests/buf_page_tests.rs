use bincode;
use storage::buf_page::HEADER_SIZE;
use storage::{BufKey, BufPage, BufType, PAGE_SIZE};
use tuple::tuple_ptr::TuplePtr;

#[test]
fn test_write_new_tuple() {
    use storage::buf_page::{HEADER_SIZE, LOWER_PTR_RANGE, UPPER_PTR_RANGE};

    let mut buf_page = new_page();
    let test_data = [5; 16];
    let tuple_ptr = buf_page.write_tuple_data(&test_data, None, None).unwrap();
    assert_eq!(tuple_ptr.buf_offset, 0);

    // upper_ptr
    assert_eq!(
        bincode::deserialize::<u16>(&buf_page.buf()[UPPER_PTR_RANGE]).unwrap()
            as usize,
        PAGE_SIZE - 16
    );
    // lower_ptr
    assert_eq!(
        bincode::deserialize::<u16>(&buf_page.buf()[LOWER_PTR_RANGE]).unwrap()
            as usize,
        HEADER_SIZE + 4
    );
    // tuple_ptr
    assert_eq!(
        bincode::deserialize::<u16>(
            &buf_page.buf()[HEADER_SIZE..HEADER_SIZE + 2]
        )
        .unwrap() as usize,
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

    let invalid_tuple_ptr = TuplePtr::new(BufKey::new(0, 1, BufType::Data), 4);
    assert!(buf_page.get_tuple_data(&invalid_tuple_ptr).is_err());

    let tuple_ptr_1 = TuplePtr::new(BufKey::new(0, 0, BufType::Data), 0);
    let tuple = buf_page.get_tuple_data(&tuple_ptr_1).unwrap();
    assert_eq!(tuple, &[2u8; 8]);

    let tuple_ptr_2 = TuplePtr::new(BufKey::new(0, 0, BufType::Data), 1);
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
    assert!(iter.next().is_none());
}

#[test]
fn test_remove_tuple() {
    // Case 1: Remove middle tuple
    {
        let mut buf_page = new_page();
        // Write some tuples
        buf_page.write_tuple_data(&[0u8; 1], None, None).unwrap();
        let to_remove =
            buf_page.write_tuple_data(&[1u8; 1], None, None).unwrap();
        buf_page.write_tuple_data(&[2u8; 1], None, None).unwrap();
        // Remove
        buf_page.remove_tuple(&to_remove, None).unwrap();
        assert_eq!(buf_page.upper_ptr, PAGE_SIZE - 2);
        assert_eq!(buf_page.lower_ptr, HEADER_SIZE + (4 * 3));
        assert_eq!(buf_page.iter().count(), 2);

        let mut iter = buf_page.iter();
        assert_eq!(iter.next().unwrap(), [0u8]);
        assert_eq!(iter.next().unwrap(), [2u8]);
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    // Case 2: Remove last tuple
    {
        let mut buf_page = new_page();
        // Write some tuples
        buf_page.write_tuple_data(&[0u8; 1], None, None).unwrap();
        buf_page.write_tuple_data(&[1u8; 1], None, None).unwrap();
        let to_remove =
            buf_page.write_tuple_data(&[2u8; 1], None, None).unwrap();
        assert_eq!(buf_page.lower_ptr, HEADER_SIZE + (4 * 3));
        // Remove
        buf_page.remove_tuple(&to_remove, None).unwrap();
        assert_eq!(buf_page.upper_ptr, PAGE_SIZE - 2);
        assert_eq!(buf_page.lower_ptr, HEADER_SIZE + (4 * 2));

        let mut iter = buf_page.iter();
        assert_eq!(iter.next().unwrap(), [0u8]);
        assert_eq!(iter.next().unwrap(), [1u8]);
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }
}

#[test]
fn test_remove_and_write() {
    let mut buf_page = new_page();
    // Write some tuples
    buf_page.write_tuple_data(&[0u8; 1], None, None).unwrap();
    let to_remove = buf_page.write_tuple_data(&[1u8; 1], None, None).unwrap();
    buf_page.write_tuple_data(&[2u8; 1], None, None).unwrap();

    buf_page.remove_tuple(&to_remove, None).unwrap();
    buf_page.write_tuple_data(&[3u8; 1], None, None).unwrap();

    let mut iter = buf_page.iter();
    assert_eq!(iter.next().unwrap(), [0u8]);
    assert_eq!(iter.next().unwrap(), [3u8]);
    assert_eq!(iter.next().unwrap(), [2u8]);
    assert!(iter.next().is_none());
}

fn new_page() -> BufPage {
    BufPage::load_from(
        &BufPage::default_buf(),
        &BufKey::new(0, 0, BufType::Data),
    )
    .unwrap()
}
