use bincode;
use db_state::DbState;
use error::{Error, Result};
use internal_types::{TupleData, ID};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{BufKey, BufPage, BufType};
use tuple::{TupleDesc, TuplePtr};

const INIT_N: u64 = 2;
const ITEMS_PER_BUCKET: usize = 80; // (PAGE_SIZE - HEADER_SIZE) / sizeof(HashItem) = 127

pub struct HashIndex {
    pub file_id: ID,
    pub rel_id: ID,
    pub key_desc: TupleDesc,
}

impl HashIndex {
    pub fn load(file_id: ID, db_state: &mut DbState) -> Result<HashIndex> {
        let meta_page = db_state.buf_mgr.get_buf(&BufKey::new(
            file_id,
            0,
            BufType::Data,
        ))?;
        let guard = meta_page.read().unwrap();

        // The data should have rel_id, key_desc, next and level
        assert!(guard.tuple_count() == 4);

        let mut iter = guard.iter();
        let rel_id: ID = bincode::deserialize(iter.next().unwrap())?;
        let key_desc: TupleDesc = bincode::deserialize(iter.next().unwrap())?;

        Ok(HashIndex {
            file_id,
            rel_id,
            key_desc,
        })
    }

    pub fn new(
        rel_id: ID,
        key_desc: TupleDesc,
        db_state: &mut DbState,
    ) -> Result<HashIndex> {
        let file_id = db_state.meta.get_new_id()?;
        let index = HashIndex {
            file_id,
            rel_id,
            key_desc,
        };

        let meta_page = db_state.buf_mgr.new_buf(&index.meta_key())?;
        let _first_page = db_state.buf_mgr.new_buf(&BufKey::new(
            index.file_id,
            1,
            BufType::Data,
        ))?;
        let _second_page = db_state.buf_mgr.new_buf(&BufKey::new(
            index.file_id,
            2,
            BufType::Data,
        ))?;

        let mut meta_guard = meta_page.write().unwrap();
        meta_guard.write_tuple_data(
            &bincode::serialize(&index.rel_id)?,
            None,
            None,
        )?;
        meta_guard.write_tuple_data(
            &bincode::serialize(&index.key_desc)?,
            None,
            None,
        )?;
        meta_guard.write_tuple_data(
            &bincode::serialize(&index.meta_key().inc_offset())?,
            None,
            None,
        )?;
        meta_guard.write_tuple_data(&bincode::serialize(&1u32)?, None, None)?;

        Ok(index)
    }

    pub fn get(
        &self,
        data: &TupleData,
        db_state: &mut DbState,
    ) -> Result<TuplePtr> {
        self.key_desc.assert_data_len(data)?;

        let meta = db_state.buf_mgr.get_buf(&self.meta_key())?;
        let meta_guard = meta.read().unwrap();
        let next: BufKey =
            bincode::deserialize(meta_guard.get_tuple_data(&self.next_ptr())?)?;
        let level: u32 = bincode::deserialize(
            meta_guard.get_tuple_data(&self.level_ptr())?,
        )?;

        let hash = self.hash(data)?;
        let bucket_key = self.get_bucket(hash, &next, level)?;
        let bucket = db_state.buf_mgr.get_buf(&bucket_key)?;
        let bucket_guard = bucket.read().unwrap();

        match self.get_item(hash, &*bucket_guard) {
            Some(item) => Ok(item.ptr),
            None => {
                Err(Error::Internal(String::from("Item not found in index")))
            }
        }
    }

    pub fn insert(
        &self,
        data: &TupleData,
        ptr: TuplePtr,
        db_state: &mut DbState,
    ) -> Result<()> {
        self.key_desc.assert_data_len(data)?;

        let meta = db_state.buf_mgr.get_buf(&self.meta_key())?;
        let mut meta_guard = meta.write().unwrap();
        let next: BufKey = bincode::deserialize(
            &meta_guard.get_tuple_data(&self.next_ptr())?,
        )?;
        let level: u32 = bincode::deserialize(
            &meta_guard.get_tuple_data(&self.level_ptr())?,
        )?;

        let hash = self.hash(data)?;
        let need_split = {
            let bucket_key = self.get_bucket(hash, &next, level)?;
            let bucket = db_state.buf_mgr.get_buf(&bucket_key)?;
            let mut bucket_guard = bucket.write().unwrap();
            match self.get_item(hash, &*bucket_guard) {
                Some(_) => {
                    return Err(Error::Internal(String::from(
                        "Item already exists in index",
                    )))
                }
                None => {
                    self.write_item(hash, ptr, &mut *bucket_guard)?;
                    bucket_guard.tuple_count() > ITEMS_PER_BUCKET
                }
            }
        };

        if need_split {
            self.split(&mut *meta_guard, db_state)?;
        }

        Ok(())
    }

    fn write_item(
        &self,
        hash: u128,
        ptr: TuplePtr,
        bucket: &mut BufPage,
    ) -> Result<()> {
        let item = HashItem { hash, ptr };
        bucket.write_tuple_data(&bincode::serialize(&item)?, None, None)?;
        Ok(())
    }

    fn get_item(&self, hash: u128, bucket: &BufPage) -> Option<HashItem> {
        bucket.iter().find_map(|tup| {
            let item: HashItem = bincode::deserialize(tup).unwrap();
            if item.hash == hash {
                Some(item)
            } else {
                None
            }
        })
    }

    fn split(&self, meta: &mut BufPage, db_state: &mut DbState) -> Result<()> {
        let next: BufKey =
            bincode::deserialize(&meta.get_tuple_data(&self.next_ptr())?)?;
        let level: u32 =
            bincode::deserialize(&meta.get_tuple_data(&self.level_ptr())?)?;
        let num_buckets = INIT_N.pow(level);
        let mut new_next = next.clone().inc_offset();

        // Get the pages
        let next_bucket = db_state.buf_mgr.get_buf(&next)?;
        let mut next_guard = next_bucket.write().unwrap();
        let new_bucket = db_state.buf_mgr.new_buf(&BufKey::new(
            self.file_id,
            next.offset + (num_buckets as u64),
            BufType::Data,
        ))?;
        let mut new_guard = new_bucket.write().unwrap();

        // Get the ptrs of tuples that are hashed into the new bucket
        let to_move: Vec<TuplePtr> = next_guard
            .iter()
            .zip(next_guard.get_all_ptrs().iter())
            .filter_map(|(tup, ptr)| {
                let item: HashItem = bincode::deserialize(tup).unwrap();
                let bucket =
                    self.get_bucket(item.hash, &new_next, level).unwrap();
                if bucket == next {
                    None
                } else {
                    Some(ptr.clone())
                }
            })
            .collect();

        // Move the tuples to the new bucket
        for ptr in to_move.iter() {
            let tup = next_guard.get_tuple_data(ptr)?.to_vec();
            next_guard.remove_tuple(ptr, None)?;
            new_guard.write_tuple_data(&tup, None, None)?;
        }

        // Update next and level if necessary
        if new_next.offset > num_buckets {
            new_next.offset = 1;
            meta.write_tuple_data(
                &bincode::serialize(&(level + 1))?,
                Some(&self.level_ptr()),
                None,
            )?;
        }
        meta.write_tuple_data(
            &bincode::serialize(&new_next)?,
            Some(&self.next_ptr()),
            None,
        )?;

        Ok(())
    }

    fn get_bucket(
        &self,
        hash: u128,
        next: &BufKey,
        level: u32,
    ) -> Result<BufKey> {
        let num_buckets = INIT_N.pow(level) as u128;

        let bucket = if hash % num_buckets < (next.offset - 1) as u128 {
            hash % (num_buckets * 2) + 1
        } else {
            hash % num_buckets + 1
        };

        Ok(BufKey::new(self.file_id, bucket as u64, BufType::Data))
    }

    fn hash(&self, data: &TupleData) -> Result<u128> {
        // Calculate hash and reverse to little endian
        let hash = Sha256::digest(data)
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        let hash = bincode::deserialize(&hash[0..16])?;
        Ok(hash)
    }

    fn meta_key(&self) -> BufKey {
        BufKey::new(self.file_id, 0, BufType::Data)
    }

    fn next_ptr(&self) -> TuplePtr {
        TuplePtr::new(self.meta_key(), 2)
    }

    fn level_ptr(&self) -> TuplePtr {
        TuplePtr::new(self.meta_key(), 3)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct HashItem {
    hash: u128,
    ptr: TuplePtr,
}
