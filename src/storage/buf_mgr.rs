use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Read, Write};
use storage;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;

pub struct BufMgr {
    buf_table: HashMap<BufKey, BufPage>,
}

impl BufMgr {
    pub fn new() -> BufMgr {
        BufMgr {
            buf_table: HashMap::new(),
        }
    }

    pub fn get_buf(&mut self, key: &BufKey) -> Result<&mut BufPage, std::io::Error> {
        if !self.buf_table.contains_key(key) {
            let read_result = self.read_buf(key);
            if read_result.is_err() {
                return Err(read_result.unwrap_err());
            }
        }
        Ok(self.buf_table.get_mut(key).unwrap())
    }

    pub fn store_buf(&self, key: &BufKey) -> Result<(), io::Error> {
        match self.buf_table.get(key) {
            Some(buf_page) => {
                let read_lock = buf_page.buf.read().unwrap();
                let mut file = OpenOptions::new().write(true).open(key.to_filename())?;
                file.seek(io::SeekFrom::Start(key.byte_offset()))?;
                file.write_all(&*read_lock.as_slice())?;
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

        self.buf_table.insert(key.clone(), BufPage::load_from(&buf, key)?);
        Ok(())
    }
}
