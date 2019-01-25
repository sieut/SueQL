use storage::{PAGE_SIZE};

pub struct BufPage {
    pub buf: Vec<u8>,
}

impl BufPage {
    pub fn new(buffer: &[u8; PAGE_SIZE as usize]) -> BufPage {
        BufPage {
            buf: buffer.to_vec()
        }
    }
}
