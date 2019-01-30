extern crate byteorder;
use self::byteorder::ByteOrder;
use self::byteorder::{LittleEndian, ReadBytesExt};

use std::io::Cursor;
use std::sync::RwLock;
use storage::{PAGE_SIZE};
use storage::buf_key::BufKey;
use tuple::tuple_ptr::TuplePtr;

static HEADER_SIZE: usize = 8;
static UPPER_PTR_OFFSET: PagePtr = 0;
static LOWER_PTR_OFFSET: PagePtr = 8;

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
    pub fn load_from(buffer: &[u8; PAGE_SIZE as usize], buf_key: &BufKey)
            -> Result<BufPage, std::io::Error> {
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

    pub fn write_tuple_data(&mut self,
                            tuple_data: &[u8],
                            tuple_ptr: Option<&TuplePtr>)
            -> Result<PagePtr, std::io::Error> {
        let buf_offset = match tuple_ptr {
            Some(ptr) => {
                self.is_valid_tuple_ptr(ptr)?;
                // TODO handle this case
                // TODO this case will also happen if a column is of variable length
                if self.tuple_data_len(ptr)? != tuple_data.len() {
                    panic!("Different sized tuple");
                }

                let read_lock = self.buf.read().unwrap();
                let mut reader = Cursor::new(
                    &read_lock[ptr.buf_offset() as usize
                        ..(ptr.buf_offset() + 4) as usize]);
                reader.read_u32::<LittleEndian>()?
            },
            None => {
                // TODO Handle this case
                if (self.available_data_space() as usize) < tuple_data.len() {
                    panic!("Not enough space in page");
                }

                let mut write_lock = self.buf.write().unwrap();
                let new_ptr = self.upper_ptr - tuple_data.len() as u32;
                LittleEndian::write_u32(
                    &mut write_lock[self.lower_ptr as usize
                        ..(self.lower_ptr + 4) as usize],
                    new_ptr);

                self.lower_ptr += 8;
                LittleEndian::write_u32(
                    &mut write_lock[LOWER_PTR_OFFSET as usize
                        ..(LOWER_PTR_OFFSET + 4) as usize],
                    self.lower_ptr);

                self.upper_ptr -= tuple_data.len() as u32;
                LittleEndian::write_u32(
                    &mut write_lock[UPPER_PTR_OFFSET as usize
                        ..(UPPER_PTR_OFFSET + 4) as usize],
                    self.upper_ptr);

                new_ptr
            }
        };

        let mut write_lock = self.buf.write().unwrap();
        write_lock[buf_offset as usize..buf_offset as usize + tuple_data.len()]
            .clone_from_slice(tuple_data);

        Ok(buf_offset)
    }

    pub fn get_tuple_data_range(&self, tuple_ptr: &TuplePtr)
            -> Result<std::ops::Range<usize>, std::io::Error> {
        self.is_valid_tuple_ptr(tuple_ptr)?;
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

    fn is_valid_tuple_ptr(&self, tuple_ptr: &TuplePtr)
            -> Result<(), std::io::Error> {
        if self.buf_key != tuple_ptr.buf_key() {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid buf_key"))
        }
        else if tuple_ptr.buf_offset() <= self.lower_ptr - 8 {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid buf_offset"))
        }
        else {
            Ok(())
        }
    }

    fn tuple_data_len(&self, tuple_ptr: &TuplePtr)
            -> Result<usize, std::io::Error> {
        let current_tuple_range = self.get_tuple_data_range(tuple_ptr)?;
        Ok(current_tuple_range.end - current_tuple_range.start)
    }

    fn available_data_space(&self) -> u32 {
        // -8 because we also have to make space for a new ptr
        self.upper_ptr - self.lower_ptr - 8
    }
}
