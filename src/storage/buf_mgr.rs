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
    buf_ref_table: HashMap<BufKey, bool>,
    buf_q: Vec<BufKey>,
    buf_q_hand: usize,
    max_size: usize,
}

impl BufMgr {
    pub fn new(max_size: Option<usize>) -> BufMgr {
        BufMgr {
            buf_table: HashMap::new(),
            buf_ref_table: HashMap::new(),
            buf_q: vec![],
            buf_q_hand: 0,
            // Default size a bit less than 4GB
            max_size: match max_size { Some(size) => size, None => 80000 },
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
        *self.buf_ref_table.get_mut(key).unwrap() = true;
        Ok(Arc::clone(self.buf_table.get(key).unwrap()))
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

        if self.buf_q.len() >= self.max_size {
            // TODO locking
            self.evict()?;
        }

        self.buf_table.insert(
            key.clone(),
            Arc::new(RwLock::new(BufPage::load_from(&buf, key)?)));
        self.buf_ref_table.insert(key.clone(), false);
        self.buf_q.push(key.clone());
        Ok(())
    }

    fn evict(&mut self) -> Result<(), io::Error> {
        {
            let to_evict = loop {
                let key = &self.buf_q[self.buf_q_hand];
                // Keep the page if it was referenced within the cycle
                // or if it is still being used
                if *self.buf_ref_table.get(key).unwrap()
                        || self.ref_count(key) > 0 {
                    *self.buf_ref_table.get_mut(key).unwrap() = false;
                    self.buf_q_hand = (self.buf_q_hand + 1) % self.buf_q.len();
                }
                else {
                    break key;
                }
            };

            self.buf_table.remove(to_evict).unwrap();
            self.buf_ref_table.remove(to_evict).unwrap();
        }

        self.buf_q.remove(self.buf_q_hand);
        self.buf_q_hand = self.buf_q_hand % self.buf_q.len();
        Ok(())
    }

    // Return number of threads that are using a page
    fn ref_count(&self, key: &BufKey) -> usize {
        // -1 because the BufMgr has 1 ref to the page
        Arc::strong_count(self.buf_table.get(key).unwrap()) - 1
    }
}
