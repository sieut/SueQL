extern crate num;

use byteorder::ByteOrder;
use byteorder::{LittleEndian, ReadBytesExt};
use enum_primitive::FromPrimitive;
use internal_types::LSN;
use std::io::Cursor;
use std::iter::Iterator;
use storage::buf_key::BufKey;
use storage::PAGE_SIZE;
use tuple::tuple_ptr::TuplePtr;

pub const HEADER_SIZE: usize = 12;
const LSN_RANGE: std::ops::Range<usize> = (0..4);
const UPPER_PTR_RANGE: std::ops::Range<usize> = (4..6);
const LOWER_PTR_RANGE: std::ops::Range<usize> = (6..8);
const SPACE_FLAG_RANGE: std::ops::Range<usize> = (8..10);
const GAP_COUNT_RANGE: std::ops::Range<usize> = (10..12);

// Page layout will be similar to Postgres'
// http://www.interdb.jp/pg/pgsql01.html#_1.3.
pub struct BufPage {
    buf: Vec<u8>,
    // Values in page's header
    pub lsn: LSN,
    pub upper_ptr: PagePtr,
    pub lower_ptr: PagePtr,
    space_flag: SpaceFlag,
    gap_count: u16,
    // BufKey for assertions
    pub buf_key: BufKey,
}

pub type PagePtr = usize;

impl BufPage {
    pub fn default_buf() -> Vec<u8> {
        let mut vec = vec![0 as u8; PAGE_SIZE];
        LittleEndian::write_u32(&mut vec[LSN_RANGE], 0);
        LittleEndian::write_u16(&mut vec[UPPER_PTR_RANGE], PAGE_SIZE as u16);
        LittleEndian::write_u16(&mut vec[LOWER_PTR_RANGE], HEADER_SIZE as u16);
        LittleEndian::write_u16(
            &mut vec[SPACE_FLAG_RANGE],
            SpaceFlag::Standard as u16,
        );
        LittleEndian::write_u16(&mut vec[GAP_COUNT_RANGE], 0u16);
        vec
    }

    pub fn load_from(
        buffer: &[u8],
        buf_key: &BufKey,
    ) -> Result<BufPage, std::io::Error> {
        use std::io::{Error, ErrorKind};

        assert_eq!(buffer.len(), PAGE_SIZE);
        let mut reader = Cursor::new(&buffer[0..HEADER_SIZE]);
        let lsn = reader.read_u32::<LittleEndian>()?;
        let upper_ptr = reader.read_u16::<LittleEndian>()? as PagePtr;
        let lower_ptr = reader.read_u16::<LittleEndian>()? as PagePtr;
        let space_flag =
            match SpaceFlag::from_u16(reader.read_u16::<LittleEndian>()?) {
                Some(flag) => flag,
                None => {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Undefined flag",
                    ))
                }
            };
        let gap_count = reader.read_u16::<LittleEndian>()?;

        Ok(BufPage {
            buf: buffer.to_vec(),
            lsn,
            upper_ptr,
            lower_ptr,
            space_flag,
            buf_key: buf_key.clone(),
            gap_count,
        })
    }

    pub fn clear(&mut self) {
        self.buf = BufPage::default_buf();
        self.lsn = 0;
        self.upper_ptr = PAGE_SIZE;
        self.lower_ptr = HEADER_SIZE;
        self.space_flag = SpaceFlag::Standard;
        self.gap_count = 0;
    }

    pub fn clone_from(&mut self, other: &BufPage) {
        assert_eq!(other.buf.len(), PAGE_SIZE);

        self.buf = other.buf.clone();
        self.lsn = other.lsn;
        self.upper_ptr = other.upper_ptr;
        self.lower_ptr = other.lower_ptr;
        self.space_flag = other.space_flag;
        self.gap_count = other.gap_count;
    }

    pub fn write_tuple_data(
        &mut self,
        tuple_data: &[u8],
        tuple_ptr: Option<&TuplePtr>,
        lsn: Option<LSN>,
    ) -> Result<TuplePtr, std::io::Error> {
        let (ret, page_ptr) = match tuple_ptr {
            Some(ptr) => {
                self.is_valid_tuple_ptr(ptr)?;
                // TODO handle this case
                // TODO this case will also happen if a column is of variable length
                if self.get_tuple_len(ptr)? != tuple_data.len() {
                    panic!("Different sized tuple");
                }
                let (page_ptr, _) = self.get_tuple_range(ptr)?;
                (ptr.clone(), page_ptr)
            }
            None => {
                if self.available_data_space() < tuple_data.len() {
                    use std::io::{Error, ErrorKind};
                    return Err(Error::new(
                        ErrorKind::Other,
                        "Not enough space for tuple",
                    ));
                }
                let new_ptr = match self.space_flag {
                    SpaceFlag::Standard => TuplePtr::new(
                        self.buf_key.clone(),
                        BufPage::ptr_to_offset(self.lower_ptr),
                    ),
                    SpaceFlag::Gaps => {
                        self.set_gap_count(self.gap_count - 1);
                        self.get_gap()
                    }
                };
                let new_start = self.upper_ptr - tuple_data.len();
                let new_end = self.upper_ptr;
                self.write_start_end(&new_ptr, (new_start, new_end));
                self.set_lower_ptr(self.lower_ptr + 4);
                self.set_upper_ptr(self.upper_ptr - tuple_data.len());
                (new_ptr, new_start)
            }
        };

        // Write tuple
        self.buf[page_ptr..page_ptr + tuple_data.len()]
            .clone_from_slice(tuple_data);
        self.update_lsn(lsn);

        Ok(ret)
    }

    pub fn get_tuple_data(
        &self,
        tuple_ptr: &TuplePtr,
    ) -> Result<&[u8], std::io::Error> {
        self.is_valid_tuple_ptr(tuple_ptr)?;
        let (start, end) = self.get_tuple_range(tuple_ptr)?;
        Ok(&self.buf[start..end])
    }

    /// Remove tuple from the page. The gap in data section is filled by
    /// shifting other tuples' over, but the gap in pointer section is not.
    /// This is to prevent having to update indices when a tuple is removed.
    pub fn remove_tuple(
        &mut self,
        tuple_ptr: &TuplePtr,
        lsn: Option<LSN>,
    ) -> Result<(), std::io::Error> {
        self.is_valid_tuple_ptr(tuple_ptr)?;
        let last_ptr = self.get_last_tuple_ptr();
        let (start, end) = self.get_tuple_range(tuple_ptr)?;
        let tup_len = end - start;

        if start != self.upper_ptr {
            // Shift data section
            let data = self.buf[self.upper_ptr..start].to_vec();
            self.buf[self.upper_ptr + tup_len..end].clone_from_slice(&data);
            // Update ptrs of shifted tuples
            let affected_ptrs: Vec<_> = self
                .get_all_ptrs()
                .iter()
                .cloned()
                .filter(|ptr| {
                    let (ptr_start, ptr_end) =
                        self.get_tuple_range(ptr).unwrap();
                    ptr_start >= self.upper_ptr && ptr_end <= start
                })
                .collect();
            for ptr in affected_ptrs.iter() {
                let (ptr_start, ptr_end) = self.get_tuple_range(&ptr).unwrap();
                self.write_start_end(
                    &ptr,
                    (ptr_start + tup_len, ptr_end + tup_len),
                );
            }
        }

        self.set_upper_ptr(self.upper_ptr + tup_len);

        if last_ptr == *tuple_ptr {
            self.set_lower_ptr(self.lower_ptr - 4);
        } else {
            // Make ptr invalid
            self.buf[BufPage::offset_to_ptr(tuple_ptr.buf_offset)
                ..BufPage::offset_to_ptr(tuple_ptr.buf_offset + 1)]
                .clone_from_slice(&[0u8; 4]);
            // Update space_flag and gap_count
            self.set_gap_count(self.gap_count + 1);
        }

        self.update_lsn(lsn);
        Ok(())
    }

    pub fn next_ptr(
        &self,
        mut tuple_ptr: TuplePtr,
    ) -> Result<TuplePtr, std::io::Error> {
        self.is_valid_tuple_ptr(&tuple_ptr)?;
        loop {
            tuple_ptr.buf_offset += 1;
            if tuple_ptr.buf_offset > self.get_last_tuple_ptr().buf_offset {
                break Ok(tuple_ptr);
            }

            let (start, end) = self.get_tuple_range(&tuple_ptr)?;
            if start != 0 && end != 0 {
                break Ok(tuple_ptr);
            }
        }
    }

    pub fn iter(&self) -> Iter {
        Iter {
            buf_page: self,
            tuple_ptr: TuplePtr::new(self.buf_key.clone(), 0),
        }
    }

    pub fn buf(&self) -> &Vec<u8> {
        &self.buf
    }

    fn offset_to_ptr(buf_offset: usize) -> PagePtr {
        HEADER_SIZE + buf_offset * 4
    }

    fn ptr_to_offset(ptr: PagePtr) -> usize {
        (ptr - HEADER_SIZE) / 4
    }

    pub fn tuple_count(&self) -> usize {
        (self.lower_ptr - HEADER_SIZE) / 4 - self.gap_count as usize
    }

    fn is_valid_tuple_ptr(
        &self,
        tuple_ptr: &TuplePtr,
    ) -> Result<(), std::io::Error> {
        if self.buf_key != tuple_ptr.buf_key {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid buf_key",
            ))
        } else if tuple_ptr.buf_offset > self.get_last_tuple_ptr().buf_offset {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid buf_offset {}", tuple_ptr.buf_offset),
            ))
        } else {
            Ok(())
        }
    }

    fn get_tuple_len(
        &self,
        tuple_ptr: &TuplePtr,
    ) -> Result<usize, std::io::Error> {
        let (start, end) = self.get_tuple_range(tuple_ptr)?;
        Ok(end - start)
    }

    fn update_lsn(&mut self, lsn: Option<LSN>) {
        match lsn {
            Some(lsn) => {
                self.lsn = lsn;
                LittleEndian::write_u32(&mut self.buf[LSN_RANGE], lsn as u32)
            }
            None => {}
        }
    }

    fn get_tuple_range(
        &self,
        tuple_ptr: &TuplePtr,
    ) -> Result<(PagePtr, PagePtr), std::io::Error> {
        let mut reader = Cursor::new(
            &self.buf[BufPage::offset_to_ptr(tuple_ptr.buf_offset)
                ..BufPage::offset_to_ptr(tuple_ptr.buf_offset + 1)],
        );
        let start = reader.read_u16::<LittleEndian>()? as usize;
        let end = reader.read_u16::<LittleEndian>()? as usize;
        Ok((start, end))
    }

    fn get_last_tuple_ptr(&self) -> TuplePtr {
        TuplePtr {
            buf_key: self.buf_key.clone(),
            buf_offset: (self.lower_ptr - HEADER_SIZE) / 4 - 1,
        }
    }

    fn write_start_end(
        &mut self,
        tuple_ptr: &TuplePtr,
        (start, end): (PagePtr, PagePtr),
    ) {
        let page_ptr = BufPage::offset_to_ptr(tuple_ptr.buf_offset);
        LittleEndian::write_u16(
            &mut self.buf[page_ptr..page_ptr + 2],
            start as u16,
        );
        LittleEndian::write_u16(
            &mut self.buf[page_ptr + 2..page_ptr + 4],
            end as u16,
        );
    }

    fn set_upper_ptr(&mut self, ptr: PagePtr) {
        self.upper_ptr = ptr;
        LittleEndian::write_u16(
            &mut self.buf[UPPER_PTR_RANGE],
            self.upper_ptr as u16,
        );
    }

    fn set_lower_ptr(&mut self, ptr: PagePtr) {
        self.lower_ptr = ptr;
        LittleEndian::write_u16(
            &mut self.buf[LOWER_PTR_RANGE],
            self.lower_ptr as u16,
        );
    }

    fn set_gap_count(&mut self, count: u16) {
        self.gap_count = count;
        LittleEndian::write_u16(&mut self.buf[GAP_COUNT_RANGE], self.gap_count);

        if self.gap_count == 0 {
            self.space_flag = SpaceFlag::Standard;
        } else {
            self.space_flag = SpaceFlag::Gaps;
        }
        LittleEndian::write_u16(
            &mut self.buf[SPACE_FLAG_RANGE],
            self.space_flag as u16,
        );
    }

    fn get_all_ptrs(&self) -> Vec<TuplePtr> {
        (0..self.get_last_tuple_ptr().buf_offset + 1)
            .map(|offset| TuplePtr::new(self.buf_key.clone(), offset))
            .filter(|ptr| {
                let (start, end) = self.get_tuple_range(ptr).unwrap();
                start != 0 && end != 0
            })
            .collect()
    }

    fn get_gap(&self) -> TuplePtr {
        (0..self.get_last_tuple_ptr().buf_offset + 1)
            .map(|offset| TuplePtr::new(self.buf_key.clone(), offset))
            .find(|ptr| {
                let (start, end) = self.get_tuple_range(ptr).unwrap();
                start == 0 && end == 0
            })
            .unwrap()
    }

    pub fn available_data_space(&self) -> usize {
        match self.space_flag {
            SpaceFlag::Standard => self.upper_ptr - self.lower_ptr - 4,
            SpaceFlag::Gaps => self.upper_ptr - self.lower_ptr,
        }
    }
}

pub struct Iter<'a> {
    buf_page: &'a BufPage,
    tuple_ptr: TuplePtr,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let next_ptr = self.buf_page.next_ptr(self.tuple_ptr.clone());

        match self.buf_page.get_tuple_data(&self.tuple_ptr) {
            Ok(data) => {
                self.tuple_ptr = next_ptr.unwrap();
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

enum_from_primitive! {
    /// Flag to help control deleting of tuples
    ///  - Standard: Page is in standard format
    ///  - Gaps: Tuple(s) deleted from page, leaving gaps that can be filled in
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum SpaceFlag {
        Standard,
        Gaps,
    }
}
