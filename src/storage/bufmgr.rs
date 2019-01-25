use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Read, Write};
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

    pub fn store_buf(&self, key: &BufKey) -> Result<(), io::Error> {
        match self.buf_table.get(key) {
            Some(buf_page) => {
                let mut file = OpenOptions::new().write(true).open(key.to_filename())?;
                file.seek(io::SeekFrom::Start(key.byte_offset()))?;
                file.write_all(buf_page.buf.as_slice())?;
                Ok(())
            },
            None => Err(io::Error::new(io::ErrorKind::NotFound, "Buffer not found"))
        }
    }

    fn read_buf(&mut self, key: &BufKey) -> Result<(), io::Error> {
        let mut file = File::open(key.to_filename())?;
        file.seek(io::SeekFrom::Start(key.byte_offset()))?;

        let mut buf = [0; storage::PAGE_SIZE as usize];
        file.read_exact(&mut buf)?;

        self.buf_table.insert(key.clone(), BufPage::new(&buf));
        Ok(())
    }
}
