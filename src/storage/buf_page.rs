extern crate byteorder;
use self::byteorder::{LittleEndian, ReadBytesExt};

use std::io::Cursor;
use std::sync::RwLock;
use storage::{PAGE_SIZE};
use storage::buf_key::BufKey;
use tuple::tuple_ptr::TuplePtr;

static HEADER_SIZE: usize = 8;

// Page layout will be similar to Postgres' (http://www.interdb.jp/pg/pgsql01.html#_1.3.)
pub struct BufPage {
    pub buf: RwLock<Vec<u8>>,
    // Values in page's header
    upper_ptr: PagePtr,
    lower_ptr: PagePtr,
    // BufKey for assertions
    buf_key: BufKey,
}

pub type PagePtr = u32;

impl BufPage {
    pub fn load_from(buffer: &[u8; PAGE_SIZE as usize], buf_key: &BufKey) -> Result<BufPage, std::io::Error> {
        let mut reader = Cursor::new(&buffer[0..HEADER_SIZE]);
        let upper_ptr = reader.read_u32::<LittleEndian>()?;
        let lower_ptr = reader.read_u32::<LittleEndian>()?;

        Ok(BufPage {
            buf: RwLock::new(buffer.to_vec()),
            upper_ptr,
            lower_ptr,
            buf_key: buf_key.clone(),
        })
    }

    pub fn get_tuple_data_range(&self, tuple_ptr: &TuplePtr) -> Result<std::ops::Range<usize>, std::io::Error> {
        if self.buf_key != tuple_ptr.buf_key() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid buf_key"));
        }
        if self.is_valid_buf_offset(tuple_ptr.buf_offset()) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid buf_offset"));
        }

    pub fn get_tuple_data_range(&self, tuple_ptr: &TuplePtr)
            -> Result<std::ops::Range<usize>, std::io::Error> {
        match self.is_valid_tuple_ptr(tuple_ptr) {
            Some(e) => return Err(e),
            None => {},
        }
        let read_lock = self.buf.read().unwrap();
        let mut reader = Cursor::new(
            &read_lock[(tuple_ptr.buf_offset() - 8) as usize
            ..tuple_ptr.buf_offset() as usize]);

        let end =
            if tuple_ptr.buf_offset() as usize == HEADER_SIZE {
                // Skip the current u32 value at the cursor
                reader.read_u32::<LittleEndian>().unwrap();
                PAGE_SIZE
            }
            else {
                reader.read_u32::<LittleEndian>().unwrap()
            };
        let start = reader.read_u32::<LittleEndian>().unwrap();

        Ok(start as usize..end as usize)
    }

    fn is_valid_tuple_ptr(&self, tuple_ptr: &TuplePtr) -> Option<std::io::Error> {
        if self.buf_key != tuple_ptr.buf_key() {
            Some(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid buf_key"))
        }
        else if tuple_ptr.buf_offset() <= self.lower_ptr - 8 {
            Some(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid buf_offset"))
        }
        else {
            None
        }
    }
    }
}
