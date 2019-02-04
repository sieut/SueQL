use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use common::ID;
use storage::buf_key::BufKey;
use storage::buf_mgr::BufMgr;
use tuple::tuple_desc::TupleDesc;

struct Rel {
    rel_id: ID,
    tuple_desc: TupleDesc,
}

impl Rel {
    pub fn load(rel_id: ID, buf_mgr: &mut BufMgr) -> Result<Rel, std::io::Error> {
        let buf_page = buf_mgr.get_buf(&BufKey::new(rel_id, 0))?;
        let lock = buf_page.read().unwrap();

        // The data should have at least num_attr, and an attr type
        assert!(lock.tuple_count() >= 2);

        let mut iter = lock.iter();
        let num_attr = {
            let data = iter.next().unwrap();
            Rel::assert_data_len(&data, 4)?;
            let mut cursor = Cursor::new(&data);
            cursor.read_u32::<LittleEndian>()?
        };

        let mut attr_ids = vec![];
        for _ in 0..num_attr {
            match iter.next() {
                Some(data) => {
                    Rel::assert_data_len(&data, 4)?;
                    let mut cursor = Cursor::new(&data);
                    attr_ids.push(cursor.read_u32::<LittleEndian>()?);
                },
                None => { return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Missing attr types")); }
            };
        }

        Ok(Rel { rel_id, tuple_desc: TupleDesc::from_attr_ids(&attr_ids).unwrap() })
    }

    fn assert_data_len(data: &[u8], desired_len: usize) -> Result<(), std::io::Error> {
        if data.len() == desired_len {
            Ok(())
        }
        else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                                    "Data does not have desired length"))
        }
    }
}
