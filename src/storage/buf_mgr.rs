use db_state::DbSettings;
use evmap;
use internal_types::ID;
use log::LogMgr;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::{Read, Seek, Write};
use std::sync::{Arc, Mutex, RwLock};
use storage;
use storage::buf_key::BufKey;
use storage::buf_page::BufPage;
use utils;

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
pub struct TableItem {
    page: Arc<RwLock<BufPage>>,
    info: Arc<RwLock<BufInfo>>,
}

impl TableItem {
    fn new(page: BufPage) -> TableItem {
        TableItem {
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

impl evmap::ShallowCopy for TableItem {
    unsafe fn shallow_copy(&mut self) -> Self {
        self.clone()
    }
}

impl PartialEq for TableItem {
    fn eq(&self, other: &TableItem) -> bool {
        Arc::ptr_eq(&self.page, &other.page)
            && Arc::ptr_eq(&self.info, &other.info)
    }
}

impl Eq for TableItem {}

#[derive(Debug)]
pub struct BufInfo {
    ref_bit: bool,
    dirty: bool,
}

#[derive(Clone)]
pub struct BufMgr {
    buf_table_r: evmap::ReadHandle<BufKey, TableItem>,
    buf_table_w: Arc<Mutex<evmap::WriteHandle<BufKey, TableItem>>>,
    evict_queue: Arc<Mutex<VecDeque<BufKey>>>,
    max_size: Arc<usize>,
    data_dir: Arc<String>,
    temp_counter: Arc<Mutex<ID>>,
    mem_counter: Arc<Mutex<ID>>,
}

impl BufMgr {
    pub fn new(settings: DbSettings) -> BufMgr {
        let buf_table = evmap::new::<BufKey, TableItem>();

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

    pub fn start_persist(
        &self,
        log_mgr: &LogMgr,
    ) -> Result<(), std::io::Error> {
        let buf_mgr_clone = self.clone();
        let log_mgr_clone = log_mgr.clone();
        std::thread::spawn(move || {
            buf_mgr_clone.persist_loop(log_mgr_clone);
        });
        Ok(())
    }

    pub fn has_buf(&self, key: &BufKey) -> bool {
        self.buf_table_r.contains_key(key)
    }

    pub fn get_buf(&mut self, key: &BufKey) -> Result<TableItem, io::Error> {
        use std::io::{Error, ErrorKind};
        use storage::BufType;

        match &key.buf_type {
            // TODO might want to make Mem buffers retrievable
            // They are not right now because if the buffer is evicted,
            // we will have to return an error, there is also no need to
            // share these between threads
            &BufType::Mem => Err(Error::new(
                ErrorKind::Other,
                "In-memory buffers are not retrievable",
            )),
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
    ) -> Result<(), io::Error> {
        use storage::BufType;

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
            None => {
                Err(io::Error::new(io::ErrorKind::NotFound, "Buffer not found"))
            }
        }
    }

    pub fn new_buf(&mut self, key: &BufKey) -> Result<TableItem, io::Error> {
        use std::io::{Error, ErrorKind};
        use storage::BufType;

        match &key.buf_type {
            // Add a non-persistent buf to BufMgr if type is Mem
            &BufType::Mem => self.add_buf(BufPage::default_buf(), key),
            // Otherwise, create buf on disk
            _ => {
                // Create new file
                if key.byte_offset() == 0 {
                    // Check if the file already exists
                    if fs::metadata(&key.to_filename(self.data_dir())).is_ok() {
                        Err(Error::new(
                            ErrorKind::AlreadyExists,
                            "File already exists",
                        ))
                    } else {
                        utils::create_file(&key.to_filename(self.data_dir()))?;
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

    pub fn new_mem_buf(&mut self) -> Result<TableItem, io::Error> {
        use storage::BufType;
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

    fn read_buf(&self, key: &BufKey) -> Result<Vec<u8>, io::Error> {
        let mut file = fs::File::open(key.to_filename(self.data_dir()))?;
        file.seek(io::SeekFrom::Start(key.byte_offset()))?;

        let mut buf = [0 as u8; storage::PAGE_SIZE];
        file.read_exact(&mut buf)?;
        Ok(buf.to_vec())
    }

    fn add_buf(
        &mut self,
        buf: Vec<u8>,
        key: &BufKey,
    ) -> Result<TableItem, io::Error> {
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
            TableItem::new(BufPage::load_from(&buf, key)?)
        );
        evict_q.push_back(key.clone());

        Ok(self.get_item(key).unwrap())
    }

    pub fn key_to_filename(&self, key: BufKey) -> String {
        key.to_filename(self.data_dir())
    }

    fn get_item(&self, key: &BufKey) -> Option<TableItem> {
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

    fn persist_loop(mut self, mut log_mgr: LogMgr) {
        use std::{thread, time};
        loop {
            thread::sleep(time::Duration::from_millis(200));

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

    pub fn persist(&mut self) -> Result<(), std::io::Error> {
        let keys = self.evict_queue.lock().unwrap().clone();
        for it in keys.iter() {
            match self.store_buf(&*it, None) {
                Ok(_) => {}
                Err(e) => match e.kind() {
                    io::ErrorKind::NotFound => {}
                    _ => return Err(e),
                },
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for BufMgr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BufMgr {{ size: {} }}", self.buf_table_r.len())
    }
}
