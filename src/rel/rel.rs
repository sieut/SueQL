use bincode;
use db_state::DbState;
use error::Result;
use index::{HashIndex, Index, IndexType};
use internal_types::{TupleData, ID, LSN};
use log::{LogEntry, OpType};
use meta;
use nom_sql::Literal;
use serde::{Deserialize, Serialize};
use storage::{BufKey, BufMgr, BufPage, BufType};
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
    indices: Vec<IndexInfo>,
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

        assert!(lock.tuple_count() == 2);

        let mut iter = lock.iter();
        let tuple_desc: TupleDesc = bincode::deserialize(iter.next().unwrap())?;
        let indices: Vec<IndexInfo> =
            bincode::deserialize(iter.next().unwrap())?;

        Ok(Rel {
            rel_id,
            buf_type,
            tuple_desc,
            indices,
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
            indices: vec![],
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
            indices: vec![],
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
            indices: vec![],
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

        let meta = db_state.buf_mgr.get_buf(&self.meta_buf_key())?;
        let rel_lock = meta.write().unwrap();

        // Prepare things for indices
        let index_writer_info = IndexWriterInfo::new(
            self.load_indices(&rel_lock)?, self.tuple_desc(), db_state)?;
        let mem_page = db_state.buf_mgr.new_mem_buf()?;
        let mut mem_guard = mem_page.write().unwrap();

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
                        }
                        let ptr = self.write_tuple(
                            &tup, &mut guard, db_state)?;
                        self.handle_index_item(
                            &tup, &ptr, &index_writer_info, &mut mem_guard,
                            db_state)?;
                        result.push(ptr);
                    }
                    None => {
                        index_writer_info.write_items(
                            &mut mem_guard, None, db_state)?;
                        return Ok(result);
                    }
                }
            }
        }
    }

    fn write_tuple(
        &self,
        tuple: &TupleData,
        page: &mut BufPage,
        db_state: &mut DbState,
    ) -> Result<TuplePtr> {
        let lsn = match self.buf_type {
            BufType::Data => Some(self.write_insert_log(
                page.buf_key,
                tuple.clone(),
                db_state,
            )?),
            _ => None,
        };
        let ptr = page.write_tuple_data(&tuple, None, lsn)?;
        Ok(ptr)
    }

    fn handle_index_item(
        &self,
        tuple: &TupleData,
        ptr: &TuplePtr,
        index_writer_info: &IndexWriterInfo,
        cache: &mut BufPage,
        db_state: &mut DbState,
    ) -> Result<()> {
        let data = index_writer_info.index_item_data(tuple, ptr)?;
        if cache.available_data_space() > data.len() {
            cache.write_tuple_data(&data, None, None)?;
        } else {
            index_writer_info.write_items(cache, Some(&data), db_state)?;
        }
        Ok(())
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

    pub fn new_index(
        &mut self,
        key: Vec<usize>,
        index_type: IndexType,
        db_state: &mut DbState,
    ) -> Result<IndexInfo> {
        let key_desc = self.tuple_desc.subset(&key)?;
        let file_id = match &index_type {
            &IndexType::Hash => {
                HashIndex::new(self.rel_id, key_desc, db_state)?.file_id
            }
        };

        let info = IndexInfo {
            file_id,
            key,
            index_type,
        };
        let meta_page = db_state.buf_mgr.get_buf(&self.meta_buf_key())?;
        let mut meta_lock = meta_page.write().unwrap();
        self.indices.push(info.clone());
        let indices_ptr = TuplePtr {
            buf_key: meta_lock.buf_key.clone(),
            buf_offset: 1,
        };
        meta_lock.write_tuple_data(
            &bincode::serialize::<Vec<IndexInfo>>(&self.indices)?,
            Some(&indices_ptr),
            None,
        )?;
        //TODO Scan through current data and update index
        Ok(info)
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
        Filter: Fn(&[u8]) -> Result<bool>,
        Then: FnMut(&[u8], &mut DbState) -> Result<()>,
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
                if filter(&*tup)? {
                    then(&*tup, db_state)?;
                }
            }
        }

        Ok(())
    }

    pub fn data_to_strings(
        &self,
        data: &[u8],
        filter_indices: Option<Vec<usize>>,
    ) -> Result<Vec<String>> {
        self.tuple_desc.data_to_strings(data, filter_indices)
    }

    pub fn literal_to_data(
        &self,
        inputs: Vec<Vec<Literal>>,
    ) -> Result<Vec<Vec<u8>>> {
        self.tuple_desc.literal_to_data(inputs)
    }

    pub fn tuple_desc(&self) -> TupleDesc {
        self.tuple_desc.clone()
    }

    pub fn indices(&self) -> Vec<IndexInfo> {
        self.indices.clone()
    }

    fn load_indices(&self, meta: &BufPage) -> Result<Vec<IndexInfo>> {
        assert!(meta.tuple_count() == 2);
        let mut iter = meta.iter();
        iter.next();
        Ok(bincode::deserialize(iter.next().unwrap())?)
    }

    fn write_new_rel(buf_mgr: &mut BufMgr, rel: &Rel) -> Result<()> {
        // Create new data file
        let key = rel.meta_buf_key();
        let meta_page = buf_mgr.new_buf(&key)?;
        let _first_page = buf_mgr.new_buf(&key.inc_offset())?;

        let mut lock = meta_page.write().unwrap();
        lock.write_tuple_data(
            &bincode::serialize(&rel.tuple_desc)?,
            None,
            None,
        )?;
        lock.write_tuple_data(
            &bincode::serialize::<Vec<IndexInfo>>(&vec![])?,
            None,
            None,
        )?;
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
    fn num_pages(&self, buf_mgr: &mut BufMgr) -> Result<u64> {
        let rel_filename = buf_mgr.key_to_filename(self.meta_buf_key());
        utils::num_pages(&rel_filename)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexInfo {
    pub file_id: ID,
    pub key: Vec<usize>,
    pub index_type: IndexType,
}

#[derive(Serialize, Deserialize)]
struct InsertTupleIndexItem {
    data: TupleData,
    ptr: TuplePtr,
}

struct IndexWriterInfo {
    indices: Vec<Box<dyn Index>>,
    indices_subsets: Vec<Vec<usize>>,
    indices_desc: TupleDesc,
    indices_subset: Vec<usize>,
    rel_desc: TupleDesc,
}

impl IndexWriterInfo {
    fn new(indices: Vec<IndexInfo>, rel_desc: TupleDesc, db_state: &mut DbState) -> Result<Self> {
        let indices = indices
            .iter()
            .map(|info| match info.index_type {
                IndexType::Hash => HashIndex::load(info.file_id, db_state)
                    .map(|hash_index| Box::new(hash_index) as Box<dyn Index>)
            })
            .collect::<Result<Vec<_>>>()?;
        let indices_subsets = indices
            .iter()
            .map(|index| rel_desc.attr_indices(
                    index.key_desc().attr_names().iter()))
            .collect::<Option<Vec<Vec<usize>>>>()
            .unwrap();
        let indices_desc = TupleDesc::union(
            indices.iter().map(|index| index.key_desc()).collect())?;
        let indices_subset = rel_desc.attr_indices(
            indices_desc.attr_names().iter()).unwrap();
        Ok(Self {
            indices,
            indices_subsets,
            indices_desc,
            indices_subset,
            rel_desc,
        })
    }

    // new_data is the new item that can't be cached
    // because we're out of space
    fn write_items(
        &self,
        cache: &mut BufPage,
        new_data: Option<&TupleData>,
        db_state: &mut DbState,
    ) -> Result<()> {
        let chain = match new_data {
            Some(data) => vec![data.as_slice()],
            None => vec![],
        };
        let index_and_subset_iter = self
            .indices
            .iter()
            .zip(self.indices_subsets.iter());
        for (index, subset) in index_and_subset_iter {
            let mut iter = cache
                .iter()
                // TODO avoid this clone
                .chain(chain.clone().into_iter())
                .map(|data| {
                    let index_item = bincode::
                        deserialize::<InsertTupleIndexItem>(data).unwrap();
                    let data = self.indices_desc.data_subset(
                        &index_item.data, subset).unwrap();
                    (data, index_item.ptr)
                });
            index.insert(&mut iter, db_state)?;
        }
        cache.clear();
        Ok(())
    }

    fn index_item_data(&self, tuple: &TupleData, ptr: &TuplePtr) -> Result<TupleData> {
        let index_item = InsertTupleIndexItem {
            data: self.rel_desc.data_subset(tuple, &self.indices_subset)?,
            ptr: ptr.clone(),
        };
        Ok(bincode::serialize(&index_item)?)
    }
}
