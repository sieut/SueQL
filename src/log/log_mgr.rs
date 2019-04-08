use internal_types::ID;
use log::LogEntry;
use std::sync::{Arc, RwLock};
use storage::buf_mgr::TableItem;
use storage::{BufKey, BufMgr, Storable};
use tuple::TuplePtr;

pub static LOG_REL_ID: ID = 2;
static LOG_META_KEY: BufKey = BufKey::new(LOG_REL_ID, 0);
static LAST_CP_PTR: TuplePtr = TuplePtr::new(LOG_META_KEY, 0);

#[derive(Clone, Debug)]
pub struct LogMgr {
    meta_page: TableItem,
    cur_page_key: Arc<RwLock<BufKey>>,
    last_cp: Arc<RwLock<TuplePtr>>,
}

impl LogMgr {
    pub fn create_and_load(
        buf_mgr: &mut BufMgr,
    ) -> Result<LogMgr, std::io::Error> {
        use std::io::ErrorKind;

        match LogMgr::load(buf_mgr) {
            Ok(log_mgr) => Ok(log_mgr),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => LogMgr::new(buf_mgr),
                _ => panic!("Cannot create and load LogMgr\nError: {:?}", e),
            },
        }
    }

    pub fn new(buf_mgr: &mut BufMgr) -> Result<LogMgr, std::io::Error> {
        let meta_page = buf_mgr.new_buf(&LOG_META_KEY)?;

        // Save metadata of LogMgr
        {
            let mut meta_guard = meta_page.write().unwrap();
            meta_guard.write_tuple_data(
                &LogMgr::default_checkpoint().to_data(),
                None,
                None,
            )?;
        }

        let _first_page = buf_mgr.new_buf(&BufKey::new(LOG_REL_ID, 1))?;
        let cur_page_key = Arc::new(RwLock::new(BufKey::new(LOG_REL_ID, 1)));

        Ok(LogMgr {
            meta_page,
            cur_page_key,
            last_cp: Arc::new(RwLock::new(LogMgr::default_checkpoint())),
        })
    }

    pub fn load(buf_mgr: &mut BufMgr) -> Result<LogMgr, std::io::Error> {
        use storage::PAGE_SIZE;
        use utils::file_len;

        let meta_page = buf_mgr.get_buf(&LOG_META_KEY)?;
        let last_cp_data;
        // Load LogMgr metadata
        {
            let meta_guard = meta_page.read().unwrap();
            last_cp_data = meta_guard.get_tuple_data(&LAST_CP_PTR)?.to_vec();
        }
        let (last_cp, last_cp_data) = TuplePtr::from_data(last_cp_data)?;
        assert_eq!(last_cp_data.len(), 0);

        let log_file_len =
            file_len(&LOG_META_KEY.to_filename(buf_mgr.data_dir()))?;
        let cur_page_key = Arc::new(RwLock::new(BufKey::new(
            LOG_REL_ID,
            log_file_len / PAGE_SIZE as u64 - 1,
        )));

        Ok(LogMgr {
            meta_page,
            cur_page_key,
            last_cp: Arc::new(RwLock::new(last_cp)),
        })
    }

    pub fn write_entries<E>(
        &mut self,
        entries: E,
        buf_mgr: &mut BufMgr,
    ) -> Result<Vec<TuplePtr>, std::io::Error>
    where
        E: Into<std::collections::VecDeque<LogEntry>>,
    {
        use std::collections::VecDeque;

        let mut entries: VecDeque<LogEntry> = entries.into();
        let _log_guard = self.meta_page.write().unwrap();
        let mut key_guard = self.cur_page_key.write().unwrap();
        let mut pages_to_store = vec![];
        let mut ret = vec![];

        while entries.len() > 0 {
            pages_to_store.push(key_guard.clone());

            let cur_page = buf_mgr.new_buf(&*key_guard)?;
            let mut page_guard = cur_page.write().unwrap();

            loop {
                match entries.pop_front() {
                    Some(entry) => {
                        if page_guard.available_data_space() < entry.size() {
                            key_guard.offset += 1;
                            entries.push_front(entry);
                            break;
                        } else {
                            ret.push(
                                page_guard.write_tuple_data(
                                    &entry.to_data(), None, None)?);
                        }
                    }
                    None => break,
                }
            }
        }

        for key in pages_to_store.iter() {
            buf_mgr.store_buf(&key, None)?;
        }
        Ok(ret)
    }

    pub fn create_checkpoint(
        &mut self,
        buf_mgr: &mut BufMgr,
    ) -> Result<TuplePtr, std::io::Error> {
        let need_cp = {
            let last_cp = self.last_cp.read().unwrap();
            let cur_key = self.cur_page_key.read().unwrap();

            assert_eq!(last_cp.buf_key.file_id, LOG_REL_ID);
            assert!(last_cp.buf_key.offset <= cur_key.offset);
            if last_cp.buf_key.offset < cur_key.offset {
                true
            } else {
                let cur_page = buf_mgr.get_buf(&cur_key)?;
                let cur_page_guard = cur_page.read().unwrap();
                last_cp.buf_offset < cur_page_guard.tuple_count() - 1
            }
        };

        if need_cp {
            dbg_log!("Creating new checkpoint");
            let pending_cp_entry = LogEntry::new_pending_cp();
            let keys = self.write_entries(vec![pending_cp_entry], buf_mgr)?;
            assert_eq!(keys.len(), 1);
            Ok(keys[0].clone())
        } else {
            Ok(LogMgr::default_checkpoint())
        }
    }

    pub fn confirm_checkpoint(
        &mut self,
        pending_cp: TuplePtr,
        buf_mgr: &mut BufMgr,
    ) -> Result<(), std::io::Error> {
        if pending_cp == LogMgr::default_checkpoint() {
            return Ok(());
        }

        let page = buf_mgr.get_buf(&pending_cp.buf_key)?;
        {
            let mut last_cp_guard = self.last_cp.write().unwrap();
            let mut log_guard = self.meta_page.write().unwrap();
            let mut page_guard = page.write().unwrap();

            let cp_entry = LogEntry::new_cp();
            // NOTE when update tuple in BufPage is implemented, change this
            log_guard
                .write_tuple_data(&pending_cp.to_data(), Some(&LAST_CP_PTR), None)?;
            page_guard
                .write_tuple_data(&cp_entry.to_data(), Some(&pending_cp), None)?;

            last_cp_guard.buf_key = pending_cp.buf_key;
            last_cp_guard.buf_offset = pending_cp.buf_offset;
        }

        buf_mgr.store_buf(&pending_cp.buf_key, None)?;
        Ok(())
    }

    /// Placeholder checkpoint when the log file is first created
    fn default_checkpoint() -> TuplePtr {
        LAST_CP_PTR
    }
}
