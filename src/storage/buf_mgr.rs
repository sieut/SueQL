use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::{Seek, Read, Write};
use std::sync::{Arc, Mutex, RwLock};
use evmap;
use storage;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;
use utils;

#[macro_use]
macro_rules! insert {
    ($evmap_lock:expr, $key:expr, $val:expr) => {
        $evmap_lock.insert($key, $val);
        $evmap_lock.refresh();
    }
}

#[macro_use]
macro_rules! remove {
    ($evmap_lock:expr, $key:expr) => {
        $evmap_lock.empty($key);
        $evmap_lock.refresh();
    }
}

struct MapItem<T> {
    item: Arc<RwLock<T>>,
}

impl<T> MapItem<T> {
    fn new(item: T) -> Self {
        Self { item: Arc::new(RwLock::new(item)) }
    }
}

impl<T> evmap::ShallowCopy for MapItem<T> {
    unsafe fn shallow_copy(&mut self) -> Self {
        self.clone()
    }
}

impl<T> Clone for MapItem<T> {
    fn clone(&self) -> Self {
        Self { item: Arc::clone(&self.item) }
    }
}

impl<T> PartialEq for MapItem<T> {
    fn eq(&self, other: &MapItem<T>) -> bool {
        Arc::ptr_eq(&self.item, &other.item)
    }
}

impl<T> Eq for MapItem<T> {}

struct BufInfo {
    ref_bit: bool,
}

#[derive(Clone)]
pub struct BufMgr {
    buf_table_r: evmap::ReadHandle<BufKey, MapItem<BufPage>>,
    buf_table_w: Arc<Mutex<evmap::WriteHandle<BufKey, MapItem<BufPage>>>>,
    info_table_r: evmap::ReadHandle<BufKey, MapItem<BufInfo>>,
    info_table_w: Arc<Mutex<evmap::WriteHandle<BufKey, MapItem<BufInfo>>>>,
    evict_queue: Arc<Mutex<VecDeque<BufKey>>>,
    max_size: Arc<usize>,
}

impl BufMgr {
    pub fn new(max_size: Option<usize>) -> BufMgr {
        let buf_table = evmap::new::<BufKey, MapItem<BufPage>>();
        let info_table = evmap::new::<BufKey, MapItem<BufInfo>>();

        BufMgr {
            buf_table_r:    buf_table.0,
            buf_table_w:    Arc::new(Mutex::new(buf_table.1)),
            info_table_r:   info_table.0,
            info_table_w:   Arc::new(Mutex::new(info_table.1)),
            evict_queue:    Arc::new(Mutex::new(VecDeque::new())),
            // Default size a bit less than 4GB
            max_size:       match max_size {
                                Some(size) => Arc::new(size),
                                None => Arc::new(80000)
                            },
        }
    }

    pub fn has_buf(&self, key: &BufKey) -> bool {
        self.buf_table_r.contains_key(key)
    }

    pub fn get_buf(&mut self, key: &BufKey)
            -> Result<Arc<RwLock<BufPage>>, io::Error> {
        // TODO not the best way to get_buf, but the old way was not correct
        loop {
            match self.get_buf_arc(key) {
                Some(buf) => {
                    let info = self.get_info_arc(key).unwrap();
                    let mut write = info.write().unwrap();
                    write.ref_bit = true;
                    break Ok(buf);
                },
                None => {
                    self.read_buf(key)?;
                }
            };
        }
    }

    pub fn store_buf(&self, key: &BufKey) -> Result<(), io::Error> {
        match self.get_buf_arc(key) {
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

        let mut buf_w = self.buf_table_w.lock().unwrap();
        let mut info_w = self.info_table_w.lock().unwrap();
        let mut evict_q = self.evict_queue.lock().unwrap();

        // Could have been loaded after acquiring the locks
        if self.has_buf(key) {
            return Ok(());
        }

        // Evict
        if self.buf_table_r.len() >= *(self.max_size) {
            loop {
                let key = evict_q.pop_front().unwrap();
                // Holding a lock of buf's info will make sure
                // another thread doesn't get this buf while it is
                // being evicted
                let info = self.get_info_arc(&key).unwrap();
                match info.try_write() {
                    Ok(mut guard) => {
                        // Evict the page IF:
                        //      its ref_bit is false
                        //      its ref_count is 0
                        if !guard.ref_bit && self.ref_count(&key) == 0 {
                            remove!(buf_w, key.clone());
                            remove!(info_w, key.clone());
                            break;
                        }
                        else {
                            guard.ref_bit = false;
                        }
                    },
                    Err(_) => {}
                };

                evict_q.push_back(key);
            };
        }

        insert!(buf_w, key.clone(),
                MapItem::new(BufPage::load_from(&buf, key)?));
        insert!(info_w, key.clone(), MapItem::new(BufInfo { ref_bit: false }));
        evict_q.push_back(key.clone());

        Ok(())
    }

    fn get_buf_arc(&self, key: &BufKey) -> Option<Arc<RwLock<BufPage>>> {
        self.buf_table_r.get_and(key, |bufs| bufs[0].clone().item)
    }

    fn get_info_arc(&self, key: &BufKey) -> Option<Arc<RwLock<BufInfo>>> {
        self.info_table_r.get_and(key, |infos| infos[0].clone().item)
    }

    // Return number of threads that are using a page
    fn ref_count(&self, key: &BufKey) -> usize {
        // -3 because evmap has 2 refs
        // and calling get_buf_arc will create a ref
        Arc::strong_count(&self.get_buf_arc(key).unwrap()) - 3
    }
}
