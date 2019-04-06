use byteorder::ByteOrder;
use byteorder::{LittleEndian, ReadBytesExt};
use internal_types::LSN;
use std::io::Cursor;
use std::iter::Iterator;
use storage::buf_key::BufKey;
use storage::PAGE_SIZE;
use tuple::tuple_ptr::TuplePtr;

pub const HEADER_SIZE: usize = 8;
const LSN_RANGE: std::ops::Range<usize> = (0..4);
const UPPER_PTR_RANGE: std::ops::Range<usize> = (4..6);
const LOWER_PTR_RANGE: std::ops::Range<usize> = (6..8);

// Page layout will be similar to Postgres'
// http://www.interdb.jp/pg/pgsql01.html#_1.3.
pub struct BufPage {
    buf: Vec<u8>,
    // Values in page's header
    lsn: LSN,
    upper_ptr: PagePtr,
    lower_ptr: PagePtr,
    // BufKey for assertions
    buf_key: BufKey,
}

pub type PagePtr = usize;

impl BufPage {
    pub fn default_buf() -> Vec<u8> {
        let mut vec = vec![0 as u8; PAGE_SIZE];
        LittleEndian::write_u32(&mut vec[LSN_RANGE], 0);
        LittleEndian::write_u16(&mut vec[UPPER_PTR_RANGE], PAGE_SIZE as u16);
        LittleEndian::write_u16(&mut vec[LOWER_PTR_RANGE], HEADER_SIZE as u16);
        vec
    }

    pub fn load_from(
        buffer: &[u8; PAGE_SIZE as usize],
        buf_key: &BufKey,
    ) -> Result<BufPage, std::io::Error> {
        let mut reader = Cursor::new(&buffer[0..HEADER_SIZE]);
        let lsn = reader.read_u32::<LittleEndian>()?;
        let upper_ptr = reader.read_u16::<LittleEndian>()? as PagePtr;
        let lower_ptr = reader.read_u16::<LittleEndian>()? as PagePtr;

        Ok(BufPage {
            buf: buffer.to_vec(),
            lsn,
            upper_ptr,
            lower_ptr,
            buf_key: buf_key.clone(),
        })
    }

    pub fn write_tuple_data(
        &mut self,
        tuple_data: &[u8],
        tuple_ptr: Option<&TuplePtr>,
    ) -> Result<TuplePtr, std::io::Error> {
        let ret_offset;

        let page_ptr: PagePtr = match tuple_ptr {
            Some(ptr) => {
                self.is_valid_tuple_ptr(ptr)?;
                // TODO handle this case
                // TODO this case will also happen if a column is of variable length
                if self.tuple_data_len(ptr)? != tuple_data.len() {
                    panic!("Different sized tuple");
                }

                ret_offset = ptr.buf_offset;

                let mut reader = Cursor::new(
                    &self.buf[BufPage::offset_to_ptr(ptr.buf_offset)
                        ..(BufPage::offset_to_ptr(ptr.buf_offset + 1))],
                );
                reader.read_u16::<LittleEndian>()? as usize
            }
            None => {
                if self.available_data_space() < tuple_data.len() {
                    use std::io::{Error, ErrorKind};
                    return Err(Error::new(
                        ErrorKind::Other, "Not enough space for tuple"));
                }

                ret_offset = BufPage::ptr_to_offset(self.lower_ptr);

                let new_start = self.upper_ptr - tuple_data.len();
                let new_end = self.upper_ptr;
                LittleEndian::write_u16(
                    &mut self.buf[self.lower_ptr..self.lower_ptr + 2],
                    new_start as u16,
                );
                LittleEndian::write_u16(
                    &mut self.buf[self.lower_ptr + 2..self.lower_ptr + 4],
                    new_end as u16,
                );

                self.lower_ptr += 4;
                LittleEndian::write_u16(&mut self.buf[LOWER_PTR_RANGE], self.lower_ptr as u16);

                self.upper_ptr -= tuple_data.len();
                LittleEndian::write_u16(&mut self.buf[UPPER_PTR_RANGE], self.upper_ptr as u16);

                new_start
            }
        };

        self.buf[page_ptr..page_ptr + tuple_data.len()].clone_from_slice(tuple_data);

        Ok(TuplePtr::new(self.buf_key.clone(), ret_offset))
    }

    pub fn get_tuple_data(&self, tuple_ptr: &TuplePtr) -> Result<&[u8], std::io::Error> {
        self.is_valid_tuple_ptr(tuple_ptr)?;
        let mut reader = Cursor::new(
            &self.buf[BufPage::offset_to_ptr(tuple_ptr.buf_offset)
                ..BufPage::offset_to_ptr(tuple_ptr.buf_offset + 1)],
        );
        let start = reader.read_u16::<LittleEndian>()? as usize;
        let end = reader.read_u16::<LittleEndian>()? as usize;

        Ok(&self.buf[start..end])
    }

    pub fn iter(&self) -> Iter {
        Iter {
            buf_page: self,
            tuple_ptr: TuplePtr::new(self.buf_key.clone(), 0),
        }
    }

    pub fn upper_ptr(&self) -> PagePtr {
        self.upper_ptr
    }

    pub fn lower_ptr(&self) -> PagePtr {
        self.lower_ptr
    }

    pub fn buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn buf_key(&self) -> BufKey {
        self.buf_key.clone()
    }

    fn offset_to_ptr(buf_offset: usize) -> PagePtr {
        HEADER_SIZE + buf_offset * 4
    }

    fn ptr_to_offset(ptr: PagePtr) -> usize {
        (ptr - HEADER_SIZE) / 4
    }

    pub fn tuple_count(&self) -> usize {
        (self.lower_ptr - HEADER_SIZE) / 4
    }

    fn is_valid_tuple_ptr(&self, tuple_ptr: &TuplePtr) -> Result<(), std::io::Error> {
        if self.buf_key != tuple_ptr.buf_key {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid buf_key",
            ))
        } else if tuple_ptr.buf_offset >= self.tuple_count() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid buf_offset {}", tuple_ptr.buf_offset),
            ))
        } else {
            Ok(())
        }
    }

    fn tuple_data_len(&self, tuple_ptr: &TuplePtr) -> Result<usize, std::io::Error> {
        let tuple_data = self.get_tuple_data(tuple_ptr)?;
        Ok(tuple_data.len())
    }

    pub fn available_data_space(&self) -> usize {
        // - 4 because we also have to make space for a new ptr
        self.upper_ptr - self.lower_ptr - 4
    }
}

pub struct Iter<'a> {
    buf_page: &'a BufPage,
    tuple_ptr: TuplePtr,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self.buf_page.get_tuple_data(&self.tuple_ptr) {
            Ok(data) => {
                self.tuple_ptr.inc_buf_offset();
                Some(data)
            }
            Err(_) => None,
        }
    }

    fn count(self) -> usize {
        self.buf_page.tuple_count()
    }
}

impl std::fmt::Debug for BufPage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "BufPage {{
               lsn: {},
               upper_ptr: {},
               lower_ptr: {},
               buf_key: {:?}
               }}",
            self.lsn, self.upper_ptr, self.lower_ptr, self.buf_key
        )
    }
}
