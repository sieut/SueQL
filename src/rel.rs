use std::io::Cursor;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use common;
use storage;
use storage::buf_page::BufPage;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;
use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;
use utils;

#[macro_use]
macro_rules! rel_data_pages {
    ($rel:expr, $rel_lock:expr) => {
        {
            let tup_ptr = TuplePtr::new($rel.meta_buf_key(), 0);
            let data = $rel_lock.get_tuple_data(&tup_ptr)?;
            utils::assert_data_len(&data, 4)?;
            let mut cursor = Cursor::new(&data);
            cursor.read_u32::<LittleEndian>()?
        }
    }
}

/// Represent a Relation on disk:
///     - First page of file is metadata of the relation
pub struct Rel {
    rel_id: common::ID,
    tuple_desc: TupleDesc,
}

impl Rel {
    pub fn load(rel_id: common::ID, buf_mgr: &mut BufMgr)
            -> Result<Rel, std::io::Error> {
        let buf_page = buf_mgr.get_buf(&BufKey::new(rel_id, 0))?;
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

        let mut attr_ids = vec![];
        for _ in 0..num_attr {
            match iter.next() {
                Some(data) => {
                    utils::assert_data_len(&data, 4)?;
                    let mut cursor = Cursor::new(&data);
                    attr_ids.push(cursor.read_u32::<LittleEndian>()?);
                },
                None => { return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Missing attr types")); }
            };
        }

        Ok(Rel {
            rel_id,
            tuple_desc: TupleDesc::from_attr_ids(&attr_ids).unwrap(),
        })
    }

    pub fn new(
            tuple_desc: TupleDesc,
            buf_mgr: &mut BufMgr,
            name: Option<String>) -> Result<Rel, std::io::Error> {
        let rel_id = common::get_new_id(buf_mgr)?;
        let rel = Rel{ rel_id, tuple_desc };

        // Create new data file
        let buf_page = buf_mgr.new_buf(&BufKey::new(rel_id, 0))?;
        let mut lock = buf_page.write().unwrap();

        // Write num data pages
        {
            let mut data: Vec<u8> = vec![];
            LittleEndian::write_u32(&mut data, 0);
            lock.write_tuple_data(&data, None)?;
        }
        // Write num attrs
        {
            let mut data: Vec<u8> = vec![];
            LittleEndian::write_u32(&mut data, rel.tuple_desc.num_attrs());
            lock.write_tuple_data(&data, None)?;
        }
        // Write attr types
        for attr in rel.tuple_desc.attr_types.iter() {
            let mut data: Vec<u8> = vec![];
            LittleEndian::write_u32(&mut data, *attr as u32);
            lock.write_tuple_data(&data, None)?;
        }

        // Add an entry to the table info rel
        if name.is_some() {
            let table_rel = Rel::load(common::TABLE_REL_ID, buf_mgr)?;
            let data = table_rel.tuple_desc.create_tuple_data(
                vec![name.unwrap(), rel.rel_id.to_string()]);
            table_rel.write_tuple(&data, buf_mgr)?;
        }

        Ok(rel)
    }

    pub fn write_tuple(&self, data: &[u8], buf_mgr: &mut BufMgr) -> Result<(), std::io::Error> {
        self.tuple_desc.assert_data_len(data)?;

        let rel_meta = buf_mgr.get_buf(&self.meta_buf_key())?;
        let mut rel_lock = rel_meta.write().unwrap();
        let num_data_pages = rel_data_pages!(self, rel_lock);

        let data_page = buf_mgr.get_buf(
            &BufKey::new(self.rel_id, num_data_pages as u64))?;
        let mut lock = data_page.write().unwrap();

        if lock.available_data_space() >= data.len() + 4 {
            lock.write_tuple_data(data, None)?;
            Ok(())
        }
        // Not enough space in page, have to create a new one
        else {
            let new_page = buf_mgr.new_buf(
                &BufKey::new(self.rel_id, (num_data_pages + 1) as u64))?;

            let mut pages_data: Vec<u8> = vec![];
            LittleEndian::write_u32(
                &mut pages_data, (num_data_pages + 1) as u32);
            rel_lock.write_tuple_data(
                &pages_data,
                Some(&TuplePtr::new(self.meta_buf_key(), 0)))?;

            let mut lock = new_page.write().unwrap();
            lock.write_tuple_data(data, None)?;
            Ok(())
        }
    }

    pub fn scan<Filter, Then>(
        &self,
        buf_mgr: &mut BufMgr,
        filter: Filter,
        then: Then) -> Result<(), std::io::Error>
    where Filter: Fn(&[u8]) -> bool,
          Then: Fn(&[u8]) {
        let rel_meta = buf_mgr.get_buf(&self.meta_buf_key())?;
        let rel_guard = rel_meta.read().unwrap();
        let num_data_pages = rel_data_pages!(self, rel_guard);

        for page_idx in 1..num_data_pages {
            let page = buf_mgr.get_buf(&BufKey::new(self.rel_id, page_idx as u64))?;
            let guard = page.read().unwrap();
            for tup in guard.iter() {
                if filter(&*tup) { then(&*tup); }
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
