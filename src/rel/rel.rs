use db_state::DbState;
use error::Result;
use internal_types::{TupleData, ID, LSN};
use log::{LogEntry, OpType};
use meta;
use nom_sql::Literal;
use storage::{BufKey, BufMgr, BufPage, BufType, PAGE_SIZE};
use tuple::{TupleDesc, TuplePtr};
use utils;

#[macro_use]
macro_rules! rel_read_lock {
    ($rel:ident, $buf_mgr:expr) => {
        let _meta = $buf_mgr.get_buf(&$rel.meta_buf_key())?;
        let _guard = _meta.read().unwrap();
    };
}

#[macro_use]
macro_rules! rel_write_lock {
    ($rel:ident, $buf_mgr:expr) => {
        let _meta = $buf_mgr.get_buf(&$rel.meta_buf_key())?;
        let _guard = _meta.write().unwrap();
    };
}

/// Represent a Relation on disk:
///     - First page of file is metadata of the relation
#[derive(Clone, Debug)]
pub struct Rel {
    pub rel_id: ID,
    buf_type: BufType,
    tuple_desc: TupleDesc,
}

impl Rel {
    pub fn load(
        rel_id: ID,
        buf_type: BufType,
        db_state: &mut DbState,
    ) -> Result<Rel> {
        let buf_page = db_state
            .buf_mgr
            .get_buf(&BufKey::new(rel_id, 0, buf_type))?;
        let lock = buf_page.read().unwrap();

        assert!(lock.tuple_count() == 1);

        let mut iter = lock.iter();
        let tuple_desc = TupleDesc::from_data(iter.next().unwrap().to_vec())?;

        Ok(Rel {
            rel_id,
            buf_type,
            tuple_desc,
        })
    }

    /// Create a new non-SueQL-controlled Relation (table),
    /// must be used when executing CREATE TABLE
    pub fn new<S: Into<String>>(
        name: S,
        tuple_desc: TupleDesc,
        db_state: &mut DbState,
    ) -> Result<Rel> {
        let rel_id = db_state.meta.get_new_id()?;
        let rel = Rel {
            rel_id,
            tuple_desc,
            buf_type: BufType::Data,
        };

        Rel::write_new_rel(&mut db_state.buf_mgr, &rel)?;
        // Add an entry to the table info rel
        let table_rel = Rel::load(meta::TABLE_REL_ID, BufType::Data, db_state)?;
        let new_entry = table_rel
            .tuple_desc
            .create_tuple_data(vec![name.into(), rel.rel_id.to_string()]);
        table_rel.write_tuples(vec![new_entry], db_state)?;

        Ok(rel)
    }

    pub fn new_temp_rel(
        tuple_desc: TupleDesc,
        db_state: &mut DbState,
    ) -> Result<Rel> {
        let rel_id = db_state.buf_mgr.new_temp_id();
        let rel = Rel {
            rel_id,
            tuple_desc,
            buf_type: BufType::Temp,
        };
        Rel::write_new_rel(&mut db_state.buf_mgr, &rel)?;
        Ok(rel)
    }

    /// Create a SueQL-controlled Relation, for database metadata
    pub fn new_meta_rel(
        rel_id: ID,
        tuple_desc: TupleDesc,
        buf_mgr: &mut BufMgr,
    ) -> Result<Rel> {
        let rel = Rel {
            rel_id,
            tuple_desc,
            buf_type: BufType::Data,
        };
        Rel::write_new_rel(buf_mgr, &rel)?;
        Ok(rel)
    }

    pub fn write_tuples(
        &self,
        mut tuples: Vec<TupleData>,
        db_state: &mut DbState,
    ) -> Result<Vec<TuplePtr>> {
        for tup in tuples.iter() {
            self.tuple_desc.assert_data_len(&tup)?;
        }

        rel_write_lock!(self, db_state.buf_mgr);
        let mut page_key = self.last_buf_key(&mut db_state.buf_mgr)?;
        let mut result = vec![];
        tuples.reverse();

        loop {
            let page = db_state.buf_mgr.get_buf(&page_key)?;
            let mut guard = page.write().unwrap();

            loop {
                match tuples.pop() {
                    Some(tup) => {
                        if guard.available_data_space() < tup.len() {
                            page_key = page_key.inc_offset();
                            tuples.push(tup);
                            break;
                        } else {
                            let lsn = match self.buf_type {
                                BufType::Data => Some(self.write_insert_log(
                                    guard.buf_key,
                                    tup.clone(),
                                    db_state,
                                )?),
                                _ => None,
                            };
                            result
                                .push(guard.write_tuple_data(&tup, None, lsn)?);
                        }
                    }
                    None => {
                        return Ok(result);
                    }
                }
            }
        }
    }

    /// Write a new page at the end of Rel's file.
    /// Must hold Rel's write lock before calling.
    // TODO make this an Op for logging
    pub fn append_page(
        &self,
        page: &BufPage,
        db_state: &mut DbState,
    ) -> Result<()> {
        let last_buf_key = self.last_buf_key(&mut db_state.buf_mgr)?;
        let new_page = db_state.buf_mgr.new_buf(&last_buf_key.inc_offset())?;
        let mut page_guard = new_page.write().unwrap();
        page_guard.clone_from(page);
        Ok(())
    }

    fn write_insert_log(
        &self,
        buf_key: BufKey,
        data: Vec<u8>,
        db_state: &mut DbState,
    ) -> Result<LSN> {
        let entry =
            LogEntry::new(buf_key, OpType::InsertTuple, data, db_state)?;
        let lsn = entry.header.lsn;
        db_state
            .log_mgr
            .write_entries(vec![entry], &mut db_state.buf_mgr)?;
        Ok(lsn)
    }

    pub fn scan<Filter, Then>(
        &self,
        db_state: &mut DbState,
        filter: Filter,
        mut then: Then,
    ) -> Result<()>
    where
        Filter: Fn(&[u8]) -> bool,
        Then: FnMut(&[u8], &mut DbState),
    {
        // TODO update scan after BufMgr bulk load is added
        rel_read_lock!(self, db_state.buf_mgr);

        for page_idx in 1..self.num_pages(&mut db_state.buf_mgr)? + 1 {
            let page = db_state.buf_mgr.get_buf(&BufKey::new(
                self.rel_id,
                page_idx as u64,
                self.buf_type,
            ))?;
            let guard = page.read().unwrap();
            for tup in guard.iter() {
                if filter(&*tup) {
                    then(&*tup, db_state);
                }
            }
        }

        Ok(())
    }

    pub fn data_to_strings(
        &self,
        data: &[u8],
        filter_indices: Option<Vec<usize>>,
    ) -> Option<Vec<String>> {
        self.tuple_desc.data_to_strings(data, filter_indices)
    }

    pub fn data_from_literal(&self, inputs: Vec<Vec<Literal>>) -> Vec<Vec<u8>> {
        self.tuple_desc.data_from_literal(inputs)
    }

    pub fn tuple_desc(&self) -> TupleDesc {
        self.tuple_desc.clone()
    }

    fn write_new_rel(buf_mgr: &mut BufMgr, rel: &Rel) -> Result<()> {
        // Create new data file
        let key = rel.meta_buf_key();
        let meta_page = buf_mgr.new_buf(&key)?;
        let _first_page = buf_mgr.new_buf(&key.inc_offset())?;

        let mut lock = meta_page.write().unwrap();
        lock.write_tuple_data(&rel.tuple_desc.to_data(), None, None)?;
        Ok(())
    }

    pub fn meta_buf_key(&self) -> BufKey {
        BufKey::new(self.rel_id, 0, self.buf_type)
    }

    fn last_buf_key(&self, buf_mgr: &mut BufMgr) -> Result<BufKey> {
        Ok(BufKey::new(
            self.rel_id,
            self.num_pages(buf_mgr)? as u64,
            self.buf_type,
        ))
    }

    //TODO Compare between saving num_pages in 1st page and getting file len
    fn num_pages(&self, buf_mgr: &mut BufMgr) -> Result<u64, std::io::Error> {
        let rel_filename = buf_mgr.key_to_filename(self.meta_buf_key());
        Ok(utils::file_len(&rel_filename)? / PAGE_SIZE as u64 - 1)
    }
}
