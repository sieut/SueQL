use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use db_state::DbState;
use internal_types::ID;
use meta;
use nom_sql::Literal;
use std::io::Cursor;
use storage::{BufMgr, BufKey};
use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;
use utils;

#[macro_use]
macro_rules! rel_data_pages {
    ($rel:expr, $rel_lock:expr) => {{
        let tup_ptr = TuplePtr::new($rel.meta_buf_key(), 0);
        let data = $rel_lock.get_tuple_data(&tup_ptr)?;
        utils::assert_data_len(&data, 4)?;
        let mut cursor = Cursor::new(&data);
        cursor.read_u32::<LittleEndian>()?
    }};
}

/// Represent a Relation on disk:
///     - First page of file is metadata of the relation
pub struct Rel {
    rel_id: ID,
    tuple_desc: TupleDesc,
}

impl Rel {
    pub fn load(rel_id: ID, db_state: &mut DbState) -> Result<Rel, std::io::Error> {
        let buf_page = db_state.buf_mgr.get_buf(&BufKey::new(rel_id, 0))?;
        let lock = buf_page.read().unwrap();

        // The data should have at least num_data_pages, num_attr, and an attr type
        assert!(lock.tuple_count() >= 3);

        let mut iter = lock.iter();
        let _num_data_pages = {
            let data = iter.next().unwrap();
            utils::assert_data_len(&data, 4)?;
            let mut cursor = Cursor::new(&data);
            cursor.read_u32::<LittleEndian>()?
        };

        let num_attr = {
            let data = iter.next().unwrap();
            utils::assert_data_len(&data, 4)?;
            let mut cursor = Cursor::new(&data);
            cursor.read_u32::<LittleEndian>()?
        };

        let mut attr_data = vec![];
        for _ in 0..num_attr {
            match iter.next() {
                Some(data) => {
                    attr_data.push(data.to_vec());
                }
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Missing attr types",
                    ));
                }
            };
        }

        Ok(Rel {
            rel_id,
            tuple_desc: TupleDesc::from_data(&attr_data)?,
        })
    }

    /// Create a new non-SueQL-controlled Relation (table),
    /// must be used when executing CREATE TABLE
    pub fn new(
        name: String,
        tuple_desc: TupleDesc,
        db_state: &mut DbState,
    ) -> Result<Rel, std::io::Error> {
        let rel_id = db_state.meta.get_new_id()?;
        let rel = Rel { rel_id, tuple_desc };

        dbg_log!("Creating rel {} with id {}", name, rel_id);

        Rel::write_new_rel(&mut db_state.buf_mgr, &rel)?;
        // Add an entry to the table info rel
        let table_rel = Rel::load(meta::TABLE_REL_ID, db_state)?;
        let new_entry = table_rel
            .tuple_desc
            .create_tuple_data(vec![name, rel.rel_id.to_string()]);
        table_rel.write_tuple(&new_entry, db_state)?;

        Ok(rel)
    }

    /// Create a SueQL-controlled Relation, for database metadata
    pub fn new_meta_rel(
        rel_id: ID,
        tuple_desc: TupleDesc,
        buf_mgr: &mut BufMgr,
    ) -> Result<Rel, std::io::Error> {
        let rel = Rel { rel_id, tuple_desc };
        Rel::write_new_rel(buf_mgr, &rel)?;
        Ok(rel)
    }

    pub fn write_tuple(
        &self,
        data: &[u8],
        db_state: &mut DbState
    ) -> Result<(), std::io::Error> {
        self.tuple_desc.assert_data_len(data)?;

        let rel_meta = db_state.buf_mgr.get_buf(&self.meta_buf_key())?;
        let mut rel_lock = rel_meta.write().unwrap();
        let num_data_pages = rel_data_pages!(self, rel_lock);

        let data_page = db_state.buf_mgr.get_buf(&BufKey::new(self.rel_id, num_data_pages as u64))?;
        let mut lock = data_page.write().unwrap();

        if lock.available_data_space() >= data.len() {
            lock.write_tuple_data(data, None)?;
            Ok(())
        }
        // Not enough space in page, have to create a new one
        else {
            let new_page =
                db_state.buf_mgr.new_buf(&BufKey::new(self.rel_id, (num_data_pages + 1) as u64))?;

            let mut pages_data = vec![0u8; 4];
            LittleEndian::write_u32(&mut pages_data, (num_data_pages + 1) as u32);
            rel_lock.write_tuple_data(&pages_data, Some(&TuplePtr::new(self.meta_buf_key(), 0)))?;

            let mut lock = new_page.write().unwrap();
            lock.write_tuple_data(data, None)?;
            Ok(())
        }
    }

    pub fn scan<Filter, Then>(
        &self,
        db_state: &mut DbState,
        filter: Filter,
        mut then: Then,
    ) -> Result<(), std::io::Error>
    where
        Filter: Fn(&[u8]) -> bool,
        Then: FnMut(&[u8]),
    {
        let rel_meta = db_state.buf_mgr.get_buf(&self.meta_buf_key())?;
        let rel_guard = rel_meta.read().unwrap();
        let num_data_pages = rel_data_pages!(self, rel_guard);

        for page_idx in 1..num_data_pages + 1 {
            let page = db_state.buf_mgr.get_buf(&BufKey::new(self.rel_id, page_idx as u64))?;
            let guard = page.read().unwrap();
            for tup in guard.iter() {
                if filter(&*tup) {
                    then(&*tup);
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

    fn write_new_rel(buf_mgr: &mut BufMgr, rel: &Rel) -> Result<(), std::io::Error> {
        // Create new data file
        let meta_page = buf_mgr.new_buf(&BufKey::new(rel.rel_id, 0))?;
        let _first_page = buf_mgr.new_buf(&BufKey::new(rel.rel_id, 1))?;
        let mut lock = meta_page.write().unwrap();

        // Write num data pages
        {
            let mut data = vec![0u8; 4];
            LittleEndian::write_u32(&mut data, 1);
            lock.write_tuple_data(&data, None)?;
        }
        // Write num attrs
        {
            let mut data = vec![0u8; 4];
            LittleEndian::write_u32(&mut data, rel.tuple_desc.num_attrs());
            lock.write_tuple_data(&data, None)?;
        }
        // Write tuple desc
        {
            let attrs_data = rel.tuple_desc.to_data();
            for tup in attrs_data.iter() {
                lock.write_tuple_data(&tup, None)?;
            }
        }
        Ok(())
    }

    fn meta_buf_key(&self) -> BufKey {
        BufKey::new(self.rel_id, 0)
    }

    fn to_filename(&self) -> String {
        format!("{}.dat", self.rel_id)
    }
}
