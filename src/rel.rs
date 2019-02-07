use std::io::Cursor;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use common;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;
use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;
use utils;

/// Represent a Relation on disk:
///     - First page of file is metadata of the relation
struct Rel {
    rel_id: common::ID,
    num_data_pages: usize,
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
        let num_data_pages = {
            let data = iter.next().unwrap();
            utils::assert_data_len(&data, 4)?;
            let mut cursor = Cursor::new(&data);
            cursor.read_u32::<LittleEndian>()? as usize
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
            num_data_pages
        })
    }

    pub fn new(tuple_desc: TupleDesc, buf_mgr: &mut BufMgr)
            -> Result<Rel, std::io::Error> {
        let rel_id = common::get_new_id(buf_mgr)?;
        let rel = Rel{ rel_id, tuple_desc, num_data_pages: 0 };

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

        Ok(rel)
    }

    pub fn write_tuple(&self, data: &[u8], buf_mgr: &mut BufMgr) -> Result<(), std::io::Error> {
        self.tuple_desc.assert_data_len(data)?;

        let last_page = buf_mgr.get_buf(
            &BufKey::new(self.rel_id, self.num_data_pages as u64))?;
        let mut lock = last_page.write().unwrap();

        // Not enough space in page, have to create a new one
        if lock.available_data_space() < data.len() + 4 {
            let new_page = buf_mgr.new_buf(
                &BufKey::new(self.rel_id, (self.num_data_pages + 1) as u64))?;

            let meta_page = buf_mgr.get_buf(
                &BufKey::new(self.rel_id, 0))?;
            let mut meta_lock = meta_page.write().unwrap();

            self.num_data_pages += 1;
            let mut pages_data: Vec<u8> = vec![];
            LittleEndian::write_u32(
                &mut pages_data, self.num_data_pages as u32);
            meta_lock.write_tuple_data(
                &pages_data,
                Some(&TuplePtr::new(BufKey::new(self.rel_id, 0), 0)))?;

            lock = new_page.write().unwrap();
        }

        lock.write_tuple_data(data, None)?;
        Ok(())
    }

    fn to_filename(&self) -> String {
        format!("{}.dat", self.rel_id)
    }
}
