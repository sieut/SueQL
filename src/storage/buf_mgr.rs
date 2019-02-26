use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::{Seek, Read, Write};
use std::sync::{Arc, RwLock};
use storage;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;
use utils;

pub struct BufMgr {
    buf_table: HashMap<BufKey, Arc<RwLock<BufPage>>>,
}

impl BufMgr {
    pub fn new() -> BufMgr {
        BufMgr {
            buf_table: HashMap::new(),
        }
    }

    pub fn has_buf(&self, key: &BufKey) -> bool {
        self.buf_table.contains_key(key)
    }

    pub fn get_buf(&mut self, key: &BufKey)
            -> Result<Arc<RwLock<BufPage>>, io::Error> {
        if !self.has_buf(key) {
            self.read_buf(key)?;
        }
        Ok(Arc::clone(self.buf_table.get(key).unwrap()))
    }

    pub fn get_bufs(&mut self, keys: Vec<&BufKey>)
            -> Result<Vec<Arc<RwLock<BufPage>>>, io::Error> {
        let mut unread = vec![];
        for key in keys.iter() {
            if !self.has_buf(key) {
                unread.push(*key)
            }
        }

        self.read_bufs(unread)?;
        Ok(keys.iter().map(
                |&key| Arc::clone(self.buf_table.get(key).unwrap())).collect())
    }

    pub fn store_buf(&self, key: &BufKey) -> Result<(), io::Error> {
        match self.buf_table.get(key) {
            Some(buf_page) => {
                let read_lock = buf_page.read().unwrap();
                let mut file = fs::OpenOptions::new().write(true)
                    .open(key.to_filename())?;
                file.seek(io::SeekFrom::Start(key.byte_offset()))?;
                file.write_all(&read_lock.buf.as_slice())?;
                Ok(())
            },
            None => Err(io::Error::new(io::ErrorKind::NotFound, "Buffer not found"))
        }
    }

    pub fn new_buf(&mut self, key: &BufKey)
            -> Result<Arc<RwLock<BufPage>>, io::Error> {
        // Create new file
        if key.byte_offset() == 0 {
            // Check if the file already exists
            if fs::metadata(&key.to_filename()).is_ok() {
                Err(io::Error::new(io::ErrorKind::AlreadyExists,
                                   "File already exists"))
            }
            else {
                utils::create_file(&key.to_filename())?;
                self.get_buf(key)
            }
        }
        // Add new page to file
        else {
            let metadata = fs::metadata(&key.to_filename())?;
            if !metadata.is_file() {
                Err(io::Error::new(io::ErrorKind::InvalidInput,
                                   "Path is not a file"))
            }
            else if metadata.len() != key.byte_offset() {
                Err(io::Error::new(io::ErrorKind::InvalidInput,
                                   "Invalid key offset"))
            }
            else {
                let mut file = fs::OpenOptions::new().write(true)
                    .open(key.to_filename())?;
                file.seek(io::SeekFrom::Start(key.byte_offset()))?;
                file.write_all(&BufPage::default_buf())?;
                self.get_buf(key)
            }
        }
    }

    fn read_buf(&mut self, key: &BufKey) -> Result<(), io::Error> {
        let mut file = fs::File::open(key.to_filename())?;
        file.seek(io::SeekFrom::Start(key.byte_offset()))?;

        let mut buf = [0 as u8; storage::PAGE_SIZE];
        file.read_exact(&mut buf)?;

        self.buf_table.insert(
            key.clone(),
            Arc::new(RwLock::new(BufPage::load_from(&buf, key)?)));
        Ok(())
    }
}
