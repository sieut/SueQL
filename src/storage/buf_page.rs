use bincode;
use error::{Error, Result};
use internal_types::LSN;
use std::iter::Iterator;
use storage::buf_key::BufKey;
use storage::PAGE_SIZE;
use tuple::tuple_ptr::TuplePtr;

pub const HEADER_SIZE: usize = 12;
pub const LSN_RANGE: std::ops::Range<usize> = (0..4);
pub const UPPER_PTR_RANGE: std::ops::Range<usize> = (4..6);
pub const LOWER_PTR_RANGE: std::ops::Range<usize> = (6..8);
pub const GAP_COUNT_RANGE: std::ops::Range<usize> = (8..12);

// Page layout will be similar to Postgres'
// http://www.interdb.jp/pg/pgsql01.html#_1.3.
pub struct BufPage {
    buf: Vec<u8>,
    // Values in page's header
    pub lsn: LSN,
    pub upper_ptr: PagePtr,
    pub lower_ptr: PagePtr,
    gap_count: u32,
    // BufKey for assertions
    pub buf_key: BufKey,
}

pub type PagePtr = usize;

impl BufPage {
    pub fn default_buf() -> Vec<u8> {
        let mut vec = vec![0 as u8; PAGE_SIZE];
        vec[LSN_RANGE].clone_from_slice(&bincode::serialize(&0u32).unwrap());
        vec[UPPER_PTR_RANGE].clone_from_slice(
            &bincode::serialize(&(PAGE_SIZE as u16)).unwrap(),
        );
        vec[LOWER_PTR_RANGE].clone_from_slice(
            &bincode::serialize(&(HEADER_SIZE as u16)).unwrap(),
        );
        vec[GAP_COUNT_RANGE]
            .clone_from_slice(&bincode::serialize(&0u32).unwrap());
        vec
    }

    pub fn load_from(buffer: &[u8], buf_key: &BufKey) -> Result<BufPage> {
        assert_eq!(buffer.len(), PAGE_SIZE);
        let lsn: u32 = bincode::deserialize(&buffer[LSN_RANGE])?;
        let upper_ptr: PagePtr =
            bincode::deserialize::<u16>(&buffer[UPPER_PTR_RANGE])? as PagePtr;
        let lower_ptr: PagePtr =
            bincode::deserialize::<u16>(&buffer[LOWER_PTR_RANGE])? as PagePtr;
        let gap_count: u32 = bincode::deserialize(&buffer[GAP_COUNT_RANGE])?;

        Ok(BufPage {
            buf: buffer.to_vec(),
            lsn,
            upper_ptr: upper_ptr as PagePtr,
            lower_ptr: lower_ptr as PagePtr,
            buf_key: buf_key.clone(),
            gap_count,
        })
    }

    pub fn clear(&mut self) {
        self.buf = BufPage::default_buf();
        self.lsn = 0;
        self.upper_ptr = PAGE_SIZE;
        self.lower_ptr = HEADER_SIZE;
        self.gap_count = 0;
    }

    pub fn clone_from(&mut self, other: &BufPage) {
        assert_eq!(other.buf.len(), PAGE_SIZE);

        self.buf = other.buf.clone();
        self.lsn = other.lsn;
        self.upper_ptr = other.upper_ptr;
        self.lower_ptr = other.lower_ptr;
        self.gap_count = other.gap_count;
    }

    pub fn write_tuple_data(
        &mut self,
        tuple_data: &[u8],
        tuple_ptr: Option<&TuplePtr>,
        lsn: Option<LSN>,
    ) -> Result<TuplePtr> {
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
                    return Err(Error::Internal(String::from(
                        "Not enough space for tuple",
                    )));
                }
                let new_ptr = match self.gap_count {
                    0 => TuplePtr::new(
                        self.buf_key.clone(),
                        BufPage::ptr_to_offset(self.lower_ptr),
                    ),
                    _ => {
                        self.set_gap_count(self.gap_count - 1)?;
                        self.get_gap()
                    }
                };
                let new_start = self.upper_ptr - tuple_data.len();
                let new_end = self.upper_ptr;
                self.write_start_end(&new_ptr, (new_start, new_end))?;
                self.set_lower_ptr(self.lower_ptr + 4)?;
                self.set_upper_ptr(self.upper_ptr - tuple_data.len())?;
                (new_ptr, new_start)
            }
        };

        // Write tuple
        self.buf[page_ptr..page_ptr + tuple_data.len()]
            .clone_from_slice(tuple_data);
        self.update_lsn(lsn)?;

        Ok(ret)
    }

    pub fn get_tuple_data(&self, tuple_ptr: &TuplePtr) -> Result<&[u8]> {
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
    ) -> Result<()> {
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
                )?;
            }
        }

        self.set_upper_ptr(self.upper_ptr + tup_len)?;

        if last_ptr == *tuple_ptr {
            self.set_lower_ptr(self.lower_ptr - 4)?;
        } else {
            // Make ptr invalid
            self.buf[BufPage::offset_to_ptr(tuple_ptr.buf_offset)
                ..BufPage::offset_to_ptr(tuple_ptr.buf_offset + 1)]
                .clone_from_slice(&[0u8; 4]);
            // Update gap_count
            self.set_gap_count(self.gap_count + 1)?;
        }

        self.update_lsn(lsn)?;
        Ok(())
    }

    pub fn next_ptr(&self, mut tuple_ptr: TuplePtr) -> Result<TuplePtr> {
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

    fn is_valid_tuple_ptr(&self, tuple_ptr: &TuplePtr) -> Result<()> {
        if self.buf_key != tuple_ptr.buf_key {
            Err(Error::Internal(String::from("Invalid buf_key")))
        } else if tuple_ptr.buf_offset > self.get_last_tuple_ptr().buf_offset {
            Err(Error::Internal(String::from(format!(
                "Invalid buf_offset {}",
                tuple_ptr.buf_offset
            ))))
        } else {
            Ok(())
        }
    }

    fn get_tuple_len(&self, tuple_ptr: &TuplePtr) -> Result<usize> {
        let (start, end) = self.get_tuple_range(tuple_ptr)?;
        Ok(end - start)
    }

    fn update_lsn(&mut self, lsn: Option<LSN>) -> Result<()> {
        match lsn {
            Some(lsn) => {
                self.lsn = lsn;
                self.buf[LSN_RANGE]
                    .clone_from_slice(&bincode::serialize(&lsn)?);
            }
            None => {}
        };
        Ok(())
    }

    fn get_tuple_range(
        &self,
        tuple_ptr: &TuplePtr,
    ) -> Result<(PagePtr, PagePtr)> {
        let ptr: PagePtr = BufPage::offset_to_ptr(tuple_ptr.buf_offset);
        let start =
            bincode::deserialize::<u16>(&self.buf[ptr..ptr + 2])? as PagePtr;
        let end = bincode::deserialize::<u16>(&self.buf[ptr + 2..ptr + 4])?
            as PagePtr;
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
    ) -> Result<()> {
        let ptr = BufPage::offset_to_ptr(tuple_ptr.buf_offset);
        self.buf[ptr..ptr + 2]
            .clone_from_slice(&bincode::serialize(&(start as u16))?);
        self.buf[ptr + 2..ptr + 4]
            .clone_from_slice(&bincode::serialize(&(end as u16))?);
        Ok(())
    }

    fn set_upper_ptr(&mut self, ptr: PagePtr) -> Result<()> {
        self.upper_ptr = ptr;
        self.buf[UPPER_PTR_RANGE]
            .clone_from_slice(&bincode::serialize(&(ptr as u16))?);
        Ok(())
    }

    fn set_lower_ptr(&mut self, ptr: PagePtr) -> Result<()> {
        self.lower_ptr = ptr;
        self.buf[LOWER_PTR_RANGE]
            .clone_from_slice(&bincode::serialize(&(ptr as u16))?);
        Ok(())
    }

    fn set_gap_count(&mut self, count: u32) -> Result<()> {
        self.gap_count = count;
        self.buf[GAP_COUNT_RANGE]
            .clone_from_slice(&bincode::serialize(&count)?);
        Ok(())
    }

    pub fn get_all_ptrs(&self) -> Vec<TuplePtr> {
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
        match self.gap_count {
            0 => self.upper_ptr - self.lower_ptr - 4,
            _ => self.upper_ptr - self.lower_ptr,
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
