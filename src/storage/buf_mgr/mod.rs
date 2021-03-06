use db_state::DbSettings;
use error::{Error, Result};
use evmap;
use internal_types::ID;
use log::LogMgr;
use meta::Meta;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::{Read, Seek, Write};
use std::sync::{Arc, Mutex, RwLock};
use storage;
use storage::BufType;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;
use utils;

#[cfg(test)]
mod tests;

#[macro_use]
macro_rules! insert {
    ($evmap_lock:expr, $key:expr, $val:expr) => {
        // We don't want multiple values for a key
        assert!(!$evmap_lock.contains_key(&$key));
        $evmap_lock.insert($key, $val);
        $evmap_lock.refresh();
    };
}

#[macro_use]
macro_rules! remove {
    ($evmap_lock:expr, $key:expr) => {
        $evmap_lock.empty($key);
        $evmap_lock.refresh();
    };
}

pub type WriteGuard<'a> = std::sync::RwLockWriteGuard<'a, BufPage>;
pub type ReadGuard<'a> = std::sync::RwLockReadGuard<'a, BufPage>;

#[derive(Clone, Debug)]
pub struct PageLock {
    page: Arc<RwLock<BufPage>>,
    info: Arc<RwLock<BufInfo>>,
}

impl PageLock {
    fn new(page: BufPage) -> PageLock {
        PageLock {
            page: Arc::new(RwLock::new(page)),
            info: Arc::new(RwLock::new(BufInfo {
                ref_bit: false,
                dirty: false,
            })),
        }
    }

    pub fn read(&self) -> std::sync::LockResult<ReadGuard> {
        self.page.read()
    }

    pub fn try_read(&self) -> std::sync::TryLockResult<ReadGuard> {
        self.page.try_read()
    }

    pub fn write(&self) -> std::sync::LockResult<WriteGuard> {
        self.set_dirty();
        self.page.write()
    }

    pub fn try_write(&self) -> std::sync::TryLockResult<WriteGuard> {
        self.set_dirty();
        self.page.try_write()
    }

    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.page)
    }

    fn set_dirty(&self) {
        let mut info_lock = self.info.write().unwrap();
        info_lock.dirty = true;
    }
}

impl evmap::ShallowCopy for PageLock {
    unsafe fn shallow_copy(&mut self) -> Self {
        self.clone()
    }
}

impl PartialEq for PageLock {
    fn eq(&self, other: &PageLock) -> bool {
        Arc::ptr_eq(&self.page, &other.page)
            && Arc::ptr_eq(&self.info, &other.info)
    }
}

impl Eq for PageLock {}

#[derive(Debug)]
pub struct BufInfo {
    ref_bit: bool,
    dirty: bool,
}

#[derive(Clone)]
pub struct BufMgr {
    buf_table_r: evmap::ReadHandle<BufKey, PageLock>,
    buf_table_w: Arc<Mutex<evmap::WriteHandle<BufKey, PageLock>>>,
    evict_queue: Arc<Mutex<VecDeque<BufKey>>>,
    max_size: Arc<usize>,
    data_dir: Arc<String>,
    temp_counter: Arc<Mutex<ID>>,
    mem_counter: Arc<Mutex<ID>>,
}

impl BufMgr {
    pub fn new(settings: DbSettings) -> BufMgr {
        let buf_table = evmap::new::<BufKey, PageLock>();

        BufMgr {
            buf_table_r: buf_table.0,
            buf_table_w: Arc::new(Mutex::new(buf_table.1)),
            evict_queue: Arc::new(Mutex::new(VecDeque::new())),
            // Default size a bit less than 4GB
            max_size: Arc::new(settings.buf_mgr_size.unwrap_or(80000)),
            data_dir: Arc::new(settings.data_dir.unwrap_or("data".to_string())),
            temp_counter: Arc::new(Mutex::new(0)),
            mem_counter: Arc::new(Mutex::new(0)),
        }
    }

    pub fn start_persist(&self, meta: &Meta, log_mgr: &LogMgr) -> Result<()> {
        let buf_mgr_clone = self.clone();
        let meta_clone = meta.clone();
        let log_mgr_clone = log_mgr.clone();
        std::thread::spawn(move || {
            buf_mgr_clone.persist_loop(meta_clone, log_mgr_clone);
        });
        Ok(())
    }

    pub fn has_buf(&self, key: &BufKey) -> bool {
        self.buf_table_r.contains_key(key)
    }

    pub fn get_buf(&mut self, key: &BufKey) -> Result<PageLock> {
        match &key.buf_type {
            // TODO might want to make Mem buffers retrievable
            // They are not right now because if the buffer is evicted,
            // we will have to return an error, there is also no need to
            // share these between threads
            &BufType::Mem => Err(Error::Internal(String::from(
                "In-memory buffers are not retrievable",
            ))),
            _ => {
                let buf = match self.get_item(key) {
                    Some(buf) => buf,
                    None => self.add_buf(self.read_buf(key)?, key)?,
                };

                let info = self.get_info_arc(key).unwrap();
                let mut write = info.write().unwrap();
                write.ref_bit = true;
                Ok(buf)
            }
        }
    }

    pub fn store_buf(
        &self,
        key: &BufKey,
        info_lock: Option<std::sync::RwLockWriteGuard<BufInfo>>,
    ) -> Result<()> {
        match self.get_item(key) {
            Some(item) => {
                let page_lock = item.read().unwrap();
                let mut info_lock = match info_lock {
                    Some(lock) => lock,
                    None => item.info.write().unwrap(),
                };

                if !info_lock.dirty {
                    return Ok(());
                }

                // Do not write Mem bufs
                match &page_lock.buf_key.buf_type {
                    &BufType::Mem => {
                        return Ok(());
                    }
                    _ => {}
                };

                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .open(key.to_filename(self.data_dir()))?;
                file.seek(io::SeekFrom::Start(key.byte_offset()))?;
                file.write_all(page_lock.buf().as_slice())?;

                info_lock.dirty = false;
                Ok(())
            }
            None => Err(Error::Internal(String::from("Buffer not found"))),
        }
    }

    pub fn new_buf(&mut self, key: &BufKey) -> Result<PageLock> {
        match &key.buf_type {
            // Add a non-persistent buf to BufMgr if type is Mem
            &BufType::Mem => self.add_buf(BufPage::default_buf(), key),
            // Otherwise, create buf on disk
            _ => {
                // Create new file
                if key.byte_offset() == 0 {
                    // Check if the file already exists
                    let fname = key.to_filename(self.data_dir());
                    if utils::file_exists(&fname) {
                        Err(Error::from(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            "File already exists",
                        )))
                    } else {
                        utils::create_file(&fname)?;
                        self.get_buf(key)
                    }
                }
                // Add new page to file
                else {
                    // If the offset is at the end of file, create new buf
                    if utils::file_len(&key.to_filename(self.data_dir()))?
                        == key.byte_offset()
                    {
                        let mut file = fs::OpenOptions::new()
                            .write(true)
                            .open(key.to_filename(self.data_dir()))?;
                        file.seek(io::SeekFrom::Start(key.byte_offset()))?;
                        file.write_all(&BufPage::default_buf())?;
                    }

                    self.get_buf(key)
                }
            }
        }
    }

    pub fn new_mem_buf(&mut self) -> Result<PageLock> {
        let id = self.new_mem_id();
        self.new_buf(&BufKey::new(id, 0, BufType::Mem))
    }

    // TODO refactor, combine with new_mem_id
    pub fn new_temp_id(&mut self) -> ID {
        let mut temp_cnt = self.temp_counter.lock().unwrap();
        *temp_cnt += 1;
        *temp_cnt
    }

    pub fn new_mem_id(&mut self) -> ID {
        let mut mem_cnt = self.mem_counter.lock().unwrap();
        *mem_cnt += 1;
        *mem_cnt
    }

    pub fn allocate_mem_bufs(
        &mut self,
        num: Option<usize>,
    ) -> Result<Vec<PageLock>> {
        let num = match num {
            Some(num) => if num > self.num_available_bufs() {
                return Err(Error::Internal(
                    "Not enough available buffers for allocate_mem_bufs"
                    .to_string()));
            } else {
                num
            }
            None => self.num_available_bufs() / 2,
        };
        let id_range = {
            let mut mem_cnt = self.mem_counter.lock().unwrap();
            let id_range = *mem_cnt+1..*mem_cnt+1+(num as u32);
            *mem_cnt += num as u32;
            id_range
        };
        id_range
            .map(|id| self.new_buf(&BufKey::new(id, 0, BufType::Mem)))
            .collect()
    }

    pub fn sequential_scan<F>(
        &mut self,
        start: BufKey,
        end: BufKey,
        mut func: F,
    ) -> Result<()>
    where F: FnMut(PageLock, &mut BufMgr) -> Result<()>
    {
        use std::thread;
        use std::sync::mpsc;
        assert_eq!(start.file_id, end.file_id);
        assert!(start.offset <= end.offset);
        if let BufType::Mem = start.buf_type {
            todo!("Sequential scan is not supported for mem bufs");
        }

        let (sender, receiver) = mpsc::channel::<PageLock>();
        let mut buf_mgr = self.clone();
        thread::spawn(move || buf_mgr.sequential_get_buf(start, end, sender));
        for buf in receiver {
            func(buf, self)?;
        }
        Ok(())
    }

    fn sequential_get_buf(
        &mut self,
        start: BufKey,
        end: BufKey,
        sender: std::sync::mpsc::Sender<PageLock>,
    ) -> Result<()> {
        let mut buf = [0u8; storage::PAGE_SIZE];
        let mut file = fs::File::open(start.to_filename(self.data_dir()))?;
        file.seek(io::SeekFrom::Start(start.byte_offset()))?;

        let mut cur = start;
        while cur.offset <= end.offset {
            let page = match self.get_item(&cur) {
                Some(page) => {
                    file.seek(
                        io::SeekFrom::Current(storage::PAGE_SIZE as i64))?;
                    page
                }
                None => {
                    file.read_exact(&mut buf)?;
                    self.add_buf(buf.to_vec(), &cur)?
                }
            };
            let info = self.get_info_arc(&cur).unwrap();
            let mut info_guard = info.write().unwrap();
            info_guard.ref_bit = true;
            sender.send(page).unwrap();
            cur = cur.inc_offset();
        }
        Ok(())
    }

    fn num_available_bufs(&self) -> usize {
        *self.max_size - self.buf_table_r.len()
    }

    fn read_buf(&self, key: &BufKey) -> Result<Vec<u8>> {
        let mut file = fs::File::open(key.to_filename(self.data_dir()))?;
        file.seek(io::SeekFrom::Start(key.byte_offset()))?;

        let mut buf = [0u8; storage::PAGE_SIZE];
        file.read_exact(&mut buf)?;
        Ok(buf.to_vec())
    }

    fn add_buf(&mut self, buf: Vec<u8>, key: &BufKey) -> Result<PageLock> {
        let mut buf_w = self.buf_table_w.lock().unwrap();
        let mut evict_q = self.evict_queue.lock().unwrap();

        // Could have been loaded after acquiring the locks
        match self.get_item(key) {
            Some(buf) => {
                return Ok(buf);
            }
            None => {}
        };

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
                            self.store_buf(&key, Some(guard))?;
                            remove!(buf_w, key.clone());
                            break;
                        } else {
                            guard.ref_bit = false;
                        }
                    }
                    Err(_) => {}
                };

                evict_q.push_back(key);
            }
        }

        insert!(
            buf_w,
            key.clone(),
            PageLock::new(BufPage::load_from(&buf, key)?)
        );
        evict_q.push_back(key.clone());

        Ok(self.get_item(key).unwrap())
    }

    pub fn key_to_filename(&self, key: BufKey) -> String {
        key.to_filename(self.data_dir())
    }

    fn get_item(&self, key: &BufKey) -> Option<PageLock> {
        self.buf_table_r.get_and(key, |items| items[0].clone())
    }

    fn get_info_arc(&self, key: &BufKey) -> Option<Arc<RwLock<BufInfo>>> {
        self.buf_table_r.get_and(key, |items| items[0].info.clone())
    }

    // Return number of threads that are using a page
    fn ref_count(&self, key: &BufKey) -> usize {
        // -3 because evmap has 2 refs
        // and calling get_item will create a ref
        self.get_item(key).unwrap().ref_count() - 3
    }

    pub fn data_dir(&self) -> String {
        self.data_dir.to_string()
    }

    fn persist_loop(mut self, meta: Meta, mut log_mgr: LogMgr) {
        use std::{thread, time};
        loop {
            thread::sleep(time::Duration::from_millis(200));

            match meta.persist_counters() {
                Ok(()) => {},
                Err(e) => panic!("Persisting counters failed\nError: {:?}", e),
            };

            let cp_ptr = match log_mgr.create_checkpoint(&mut self) {
                Ok(ptr) => ptr,
                Err(e) => panic!("Creating checkpoint failed\nError: {:?}", e),
            };

            if let Err(e) = self.persist() {
                panic!("Persist failed\nError: {:?}", e);
            }

            if let Err(e) = log_mgr.confirm_checkpoint(cp_ptr, &mut self) {
                panic!("Confirming checkpoint failed\nError: {:?}", e);
            }
        }
    }

    pub fn persist(&mut self) -> Result<()> {
        let keys = self.evict_queue.lock().unwrap().clone();
        for it in keys.iter() {
            self.store_buf(&*it, None)?;
        }

        Ok(())
    }
}

impl std::fmt::Debug for BufMgr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BufMgr {{ size: {} }}", self.buf_table_r.len())
    }
}
