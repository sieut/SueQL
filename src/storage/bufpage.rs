use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::cmp::Eq;
use std::sync::Mutex;
use storage::PAGE_SIZE;

lazy_static! {
    static ref ALLOCATED_PAGES: Mutex<HashSet<BufPageId>> = Mutex::new(HashSet::new());
}

pub struct BufPage {
    id: BufPageId,
    data: [u8; PAGE_SIZE]
}

#[derive(PartialEq)]
pub struct BufPageId {
    file_name: String,
    offset: usize
}

impl BufPage {
    fn new(file_name: String, offset: usize) -> BufPage {
        // TODO check if bufpage is already allocated
        ALLOCATED_PAGES.lock().unwrap().insert(BufPageId { file_name: file_name.clone(), offset: offset });

        BufPage {
            id: BufPageId { file_name: file_name.clone(), offset: offset },
            data: [0; PAGE_SIZE]
        }
    }
}

impl Hash for BufPageId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_name.hash(state);
        self.offset.hash(state);
    }
}

impl Eq for BufPageId { }
