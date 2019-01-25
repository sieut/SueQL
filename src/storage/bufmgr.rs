use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Seek, Read};
use storage;
use storage::bufkey::BufKey;
use storage::bufpage::BufPage;

pub struct BufMgr {
    buf_table: HashMap<BufKey, BufPage>,
}

impl BufMgr {
    pub fn new() -> BufMgr {
        BufMgr {
            buf_table: HashMap::new(),
        }
    }

    pub fn get_buf(&mut self, key: &BufKey) -> Option<&mut BufPage> {
        if !self.buf_table.contains_key(key) {
            let read_result = self.read_buf(key);
            // TODO Handle error
            if read_result.is_err() {
                return None;
            }
        }
        Some(self.buf_table.get_mut(key).unwrap())
    }

    pub fn store_buf(&self, key: &BufKey) -> Option<()> {
        match self.buf_table.get(key) {
            Some(buf) => {
            },
            None => None
        }
    }

    fn read_buf(&mut self, key: &BufKey) -> Result<(), io::Error> {
        let mut file = self.open_file(key)?;
        let mut buf = [0; storage::PAGE_SIZE as usize];
        file.read_exact(&mut buf)?;

        self.buf_table.insert(key.clone(), BufPage::new(&buf));
        Ok(())
    }

    fn open_file(&self, key: &BufKey) -> Result<File, io::Error> {
        let mut file = File::open(key.to_filename())?;
        file.seek(io::SeekFrom::Start(key.byte_offset()))?;
        Ok(file)
    }
}
