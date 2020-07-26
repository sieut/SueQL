use bincode;
use db_state::DbState;
use error::{Error, Result};
use internal_types::{TupleData, ID};
use serde::{Deserialize, Serialize};
use storage::{BufKey, BufPage, BufType};
use tuple::{TupleDesc, TuplePtr};
use utils;

#[cfg(test)]
mod tests;

const INIT_N: u64 = 2;
// (PAGE_SIZE - HEADER_SIZE) / (sizeof(HashItem) + 4) - overflow_ptr = 91
const ITEMS_PER_BUCKET: usize = 90;
const METADATA_ITEMS: usize = 5;

#[derive(Clone)]
pub struct HashIndex {
    pub file_id: ID,
    pub rel_id: ID,
    pub key_desc: TupleDesc,
    pub overflow_file_id: ID,
}

impl HashIndex {
    pub fn load(file_id: ID, db_state: &mut DbState) -> Result<HashIndex> {
        let meta_page = db_state.buf_mgr.get_buf(&BufKey::new(
            file_id,
            0,
            BufType::Data,
        ))?;
        let guard = meta_page.read().unwrap();

        assert!(guard.tuple_count() == METADATA_ITEMS);
        let mut iter = guard.iter();
        let rel_id: ID = bincode::deserialize(iter.next().unwrap())?;
        let key_desc: TupleDesc = bincode::deserialize(iter.next().unwrap())?;
        let _next: BufKey = bincode::deserialize(iter.next().unwrap())?;
        let _level: u32 = bincode::deserialize(iter.next().unwrap())?;
        let overflow_file_id: ID = bincode::deserialize(iter.next().unwrap())?;

        Ok(HashIndex {
            file_id,
            rel_id,
            key_desc,
            overflow_file_id,
        })
    }

    pub fn new(
        rel_id: ID,
        key_desc: TupleDesc,
        db_state: &mut DbState,
    ) -> Result<HashIndex> {
        let file_id = db_state.meta.get_new_id()?;
        let overflow_file_id = db_state.meta.get_new_id()?;
        let index = HashIndex {
            file_id,
            rel_id,
            key_desc,
            overflow_file_id,
        };

        let meta_page = db_state.buf_mgr.new_buf(&index.meta_key())?;
        let _first_bucket = HashBucket::new(
            BufKey::new(index.file_id, 1, BufType::Data),
            index.overflow_file_id,
            db_state)?;
        let _second_bucket = HashBucket::new(
            BufKey::new(index.file_id, 2, BufType::Data),
            index.overflow_file_id,
            db_state)?;
        let _overflow = db_state.buf_mgr.new_buf(
            &BufKey::new(index.overflow_file_id, 0, BufType::Data))?;

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
        meta_guard.write_tuple_data(
            &bincode::serialize(&1u32)?, None, None)?;
        meta_guard.write_tuple_data(
            &bincode::serialize(&index.overflow_file_id)?, None, None)?;

        Ok(index)
    }

    pub fn get(
        &self,
        data: &TupleData,
        db_state: &mut DbState,
    ) -> Result<Vec<TuplePtr>> {
        self.key_desc.assert_data_len(data)?;
        let meta = db_state.buf_mgr.get_buf(&self.meta_key())?;
        let meta_guard = meta.read().unwrap();
        let next: BufKey =
            bincode::deserialize(meta_guard.get_tuple_data(&self.next_ptr())?)?;
        let level: u32 = bincode::deserialize(
            meta_guard.get_tuple_data(&self.level_ptr())?,
        )?;
        let hash = self.hash(data);
        let bucket = self.get_bucket(hash, &next, level);
        Ok(bucket
            .get_items(hash, db_state)?
            .iter()
            .map(|item| item.ptr)
            .collect())
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

        let hash = self.hash(data);
        let need_split = {
            let bucket = self.get_bucket(hash, &next, level);
            bucket.write_items(
                vec![HashItem { hash, ptr }],
                db_state,
            )?
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

    fn get_items(&self, hash: u128, bucket: &BufPage) -> Vec<HashItem> {
        bucket.iter().filter_map(|tup| {
            let item: HashItem = bincode::deserialize(tup).unwrap();
            if item.hash == hash {
                Some(item)
            } else {
                None
            }
        }).collect()
    }

    fn split(&self, meta: &mut BufPage, db_state: &mut DbState) -> Result<()> {
        let next: BufKey =
            bincode::deserialize(&meta.get_tuple_data(&self.next_ptr())?)?;
        let level: u32 =
            bincode::deserialize(&meta.get_tuple_data(&self.level_ptr())?)?;
        let num_buckets = INIT_N.pow(level);
        let mut new_next = next.clone().inc_offset();

        let next_bucket = HashBucket {
            buf_key: next,
            overflow_file_id: self.overflow_file_id,
        };
        let new_bucket = HashBucket::new(
            BufKey::new(
                self.file_id,
                next.offset + (num_buckets as u64),
                BufType::Data),
            self.overflow_file_id,
            db_state,
        )?;
        next_bucket.split(&new_bucket, (num_buckets * 2) as u128, db_state)?;

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
    ) -> HashBucket {
        let num_buckets = INIT_N.pow(level) as u128;
        let bucket = if hash % num_buckets < (next.offset - 1) as u128 {
            hash % (num_buckets * 2) + 1
        } else {
            hash % num_buckets + 1
        };
        HashBucket {
            buf_key: BufKey::new(self.file_id, bucket as u64, BufType::Data),
            overflow_file_id: self.overflow_file_id
        }
    }

    fn hash(&self, data: &TupleData) -> u128 {
        use fasthash::murmur3;
        murmur3::hash128(data)
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

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct HashItem {
    hash: u128,
    ptr: TuplePtr,
}

struct HashBucket {
    buf_key: BufKey,
    overflow_file_id: ID,
}

impl HashBucket {
    fn new(
        buf_key: BufKey,
        overflow_file_id: ID,
        db_state: &mut DbState,
    ) -> Result<Self> {
        let page = db_state.buf_mgr.new_buf(&buf_key)?;
        let mut guard = page.write().unwrap();
        assert_eq!(guard.tuple_count(), 0);
        guard.write_tuple_data(
            &bincode::serialize(&BufKey::new(0, 0, BufType::Data))?,
            None,
            None)?;
        Ok(Self {
            buf_key,
            overflow_file_id,
        })
    }

    /// Returns True if bucket is overflowed
    fn write_items(
        &self,
        items: Vec<HashItem>,
        db_state: &mut DbState
    ) -> Result<bool> {
        let bucket = db_state.buf_mgr.get_buf(&self.buf_key)?;
        let mut bucket_guard = bucket.write().unwrap();
        let mut items = self.write_items_to_page(items, &mut bucket_guard)?;
        if items.is_empty() {
            Ok(false)
        } else {
            let mut overflow_key = self.get_overflow_key(
                &mut bucket_guard, db_state)?;
            loop {
                let overflow = db_state.buf_mgr.get_buf(&overflow_key)?;
                let mut guard = overflow.write().unwrap();
                items = self.write_items_to_page(items, &mut guard)?;
                if items.len() > 0 {
                    overflow_key = self.get_overflow_key(
                        &mut guard, db_state)?;
                } else {
                    break;
                }
            };
            Ok(true)
        }
    }

    /// Write given items to given page, return the items that are not
    /// written due to items per bucket restriction
    fn write_items_to_page(
        &self,
        items: Vec<HashItem>,
        page: &mut BufPage,
    ) -> Result<Vec<HashItem>> {
        items
            .into_iter()
            .filter_map(|item| {
                if self.get_items_count(&page) >= ITEMS_PER_BUCKET {
                    Some(Ok(item))
                } else {
                    match bincode::serialize(&item) {
                        Ok(data) => {
                            match page.write_tuple_data(&data, None, None) {
                                Ok(_) => None,
                                Err(err) => Some(Err(Error::from(err))),
                            }
                        }
                        Err(err) => Some(Err(Error::from(err))),
                    }
                }
            })
            .collect::<Result<Vec<HashItem>>>()
    }

    fn get_overflow_key(
        &self,
        page: &mut BufPage,
        db_state: &mut DbState,
    ) -> Result<BufKey> {
        let overflow_key: BufKey = bincode::deserialize(
            &page.iter().next().unwrap())?;
        if self.is_valid_overflow(&overflow_key) {
            Ok(overflow_key)
        } else {
            let overflow_filename = db_state.buf_mgr.key_to_filename(
                BufKey::new(self.overflow_file_id, 0, BufType::Data));
            let overflow_num_pages = utils::num_pages(&overflow_filename)?;
            let overflow_key = BufKey::new(
                self.overflow_file_id,
                overflow_num_pages,
                BufType::Data,
            ).inc_offset();
            HashBucket::new(
                overflow_key.clone(),
                self.overflow_file_id,
                db_state,
            )?;
            page.write_tuple_data(
                &bincode::serialize(&overflow_key)?,
                Some(&TuplePtr::new(page.buf_key.clone(), 0)),
                None,
            )?;
            Ok(overflow_key)
        }
    }

    fn get_items(
        &self,
        hash: u128,
        db_state: &mut DbState,
    ) -> Result<Vec<HashItem>> {
        let bucket = db_state.buf_mgr.get_buf(&self.buf_key)?;
        let bucket_guard = bucket.read().unwrap();
        let (mut overflow_key, mut result) =
            self.get_items_from_page(hash, &bucket_guard)?;

        while self.is_valid_overflow(&overflow_key) {
            let page = db_state.buf_mgr.get_buf(&overflow_key)?;
            let guard = page.read().unwrap();
            let (new_overflow_key, mut new_result) =
                self.get_items_from_page(hash, &guard)?;
            overflow_key = new_overflow_key;
            result.append(&mut new_result);
        }

        Ok(result)
    }

    fn get_items_from_page(
        &self,
        hash: u128,
        page: &BufPage,
    ) -> Result<(BufKey, Vec<HashItem>)> {
        assert!(page.tuple_count() > 1);
        let mut iter = page.iter();
        let overflow_key: BufKey = bincode::deserialize(iter.next().unwrap())?;
        let items = iter
            .filter_map(|tuple| match bincode::deserialize::<HashItem>(tuple) {
                Ok(item) => {
                    if item.hash == hash {
                        Some(Ok(item))
                    } else {
                        None
                    }
                }
                Err(err) => Some(Err(Error::from(err)))
            }
            )
            .collect::<Result<Vec<HashItem>>>()?;
        Ok((overflow_key, items))
    }

    fn split(
        &self,
        other: &HashBucket,
        modulo: u128,
        db_state: &mut DbState,
    ) -> Result<()> {
        let bucket = db_state.buf_mgr.get_buf(&self.buf_key)?;
        let mut bucket_guard = bucket.write().unwrap();
        let (mut overflow_key, mut split_items) =
            self.get_split_items_from_page(&mut bucket_guard, modulo)?;

        while self.is_valid_overflow(&overflow_key) {
            let page = db_state.buf_mgr.get_buf(&overflow_key)?;
            let mut guard = page.write().unwrap();
            let (new_overflow_key, mut new_split_items) =
                self.get_split_items_from_page(&mut guard, modulo)?;
            overflow_key = new_overflow_key;
            split_items.append(&mut new_split_items);
        }
        other.write_items(split_items, db_state)?;
        Ok(())
    }

    fn get_split_items_from_page(
        &self,
        page: &mut BufPage,
        modulo: u128,
    ) -> Result<(BufKey, Vec<HashItem>)> {
        assert!(page.tuple_count() > 1);
        let (overflow_key, items, ptrs) = {
            let all_ptrs = page.get_all_ptrs();
            let mut iter = page.iter().zip(all_ptrs.into_iter());
            let overflow_key: BufKey = bincode::deserialize(
                iter.next().unwrap().0)?;
            let mut items = vec![];
            let mut ptrs = vec![];
            for (tuple, ptr) in iter {
                let item = bincode::deserialize::<HashItem>(tuple)?;
                if item.hash % modulo + 1 != self.buf_key.offset as u128 {
                    items.push(item);
                    ptrs.push(ptr);
                }
            }
            (overflow_key, items, ptrs)
        };
        for ptr in ptrs.iter() {
            page.remove_tuple(ptr, None)?;
        }
        Ok((overflow_key, items))
    }

    fn get_items_count(&self, page: &BufPage) -> usize {
        page.tuple_count() - 1
    }

    fn is_valid_overflow(&self, key: &BufKey) -> bool {
        key.file_id != 0
    }
}
