use internal_types::{ID, LSN};
use log::LogEntry;
use std::sync::{Arc, RwLock};
use storage::{BufMgr, BufKey};
use storage::buf_mgr::TableItem;

pub static LOG_REL_ID: ID = 2;
static LOG_META_KEY: BufKey = BufKey::new(LOG_REL_ID, 0);

pub struct LogMgr {
    // Needs BufMgr to call store_buf on new log entries
    buf_mgr: BufMgr,
    meta_page: TableItem,
    cur_page_key: Arc<RwLock<BufKey>>,
}

impl LogMgr {
    pub fn new(mut buf_mgr: BufMgr) -> Result<LogMgr, std::io::Error> {
        let meta_page = buf_mgr.new_buf(&LOG_META_KEY)?;
        // TODO save data in meta
        let cur_page_key = Arc::new(RwLock::new(BufKey::new(LOG_REL_ID, 1)));

        Ok(LogMgr { buf_mgr, meta_page, cur_page_key })
    }

    pub fn load(mut buf_mgr: BufMgr) -> Result<LogMgr, std::io::Error> {
        use utils::file_len;
        use storage::PAGE_SIZE;

        let meta_page = buf_mgr.get_buf(&LOG_META_KEY)?;
        // TODO load data from meta
        let log_file_len = file_len(&LOG_META_KEY.to_filename())?;
        let cur_page_key = Arc::new(RwLock::new(
            BufKey::new(LOG_REL_ID, log_file_len / PAGE_SIZE as u64 - 1)));

        Ok(LogMgr { buf_mgr, meta_page, cur_page_key })
    }

    pub fn write_entries<E>(
        &mut self,
        entries: E
    ) -> Result<(), std::io::Error>
    where E: Into<std::collections::VecDeque<LogEntry>> {
        use std::collections::VecDeque;

        let mut entries: VecDeque<LogEntry> = entries.into();
        let _log_guard = self.meta_page.write().unwrap();
        let mut key_guard = self.cur_page_key.write().unwrap();
        let mut pages_to_store = vec![];

        while entries.len() > 0 {
            pages_to_store.push(key_guard.clone());

            let cur_page = self.buf_mgr.get_buf(&*key_guard)?;
            let mut page_guard = cur_page.write().unwrap();

            loop {
                match entries.pop_front() {
                    Some(entry) => {
                        if page_guard.available_data_space() < entry.size() {
                            key_guard.offset += 1;
                            entries.push_front(entry);
                            break;
                        }
                        else {
                            page_guard.write_tuple_data(&entry.to_data(), None)?;
                        }
                    },
                    None => break,
                }
            }
        }

        for key in pages_to_store.iter() {
            self.buf_mgr.store_buf(&key, None)?;
        }
        Ok(())
    }
}
