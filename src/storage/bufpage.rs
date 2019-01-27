extern crate byteorder;

use std::io::Cursor;
use storage::{PAGE_SIZE};
use self::byteorder::{LittleEndian, ReadBytesExt};

static HEADER_SIZE: usize = 8;

// Page layout will be similar to Postgres' (http://www.interdb.jp/pg/pgsql01.html#_1.3.)
pub struct BufPage {
    // TODO update tests.rs and remove pub
    pub buf: Vec<u8>,
    // Values in page's header
    upper_ptr: PagePtr,
    lower_ptr: PagePtr
}

type PagePtr = u32;

impl BufPage {
    pub fn load_from(buffer: &[u8; PAGE_SIZE as usize]) -> Result<BufPage, std::io::Error> {
        let mut reader = Cursor::new(&buffer[0..HEADER_SIZE]);
        let upper_ptr = reader.read_u32::<LittleEndian>()?;
        let lower_ptr = reader.read_u32::<LittleEndian>()?;

        Ok(BufPage {
            buf: buffer.to_vec(),
            upper_ptr,
            lower_ptr
        })
    }
}
