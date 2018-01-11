use std::ops::{Drop,Index,IndexMut,Range};
use std::boxed::Box;

use storage::PAGE_SIZE;

// let max page count be 512 for now
const MAX_PAGE_COUNT:usize = 512;

// keep track of current page count
static mut page_count: usize = 0;

pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
    zeroed: bool,
    in_memory: bool,
    dirty: bool
}

impl Page {
    fn new() -> Page {
        Page {
            data: Box::new([0; PAGE_SIZE]),
            zeroed: true,
            in_memory: false,
            dirty: false
        }
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        // TODO check this unsafe
        unsafe { page_count -= 1; }
    }
}

impl Index<usize> for Page {
    type Output = u8;

    fn index<'a>(&'a self, index: usize) -> &'a Self::Output {
        &self.data[index]
    }
}

impl IndexMut<usize> for Page {
    fn index_mut<'a>(&'a mut self, index: usize) -> &'a mut Self::Output {
        &mut self.data[index]
    }
}

impl Index<Range<usize>> for Page {
    type Output = [u8];

    fn index<'a>(&'a self, index: Range<usize>) -> &'a Self::Output {
        &self.data[index]
    }
}
