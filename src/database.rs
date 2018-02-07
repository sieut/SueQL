extern crate byteorder;

use table::Table;
use storage::{PageReader, Storable, PAGE_SIZE};
use utils;
use self::byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::collections::HashMap;

const MAP_ENTRY_SIZE: usize = 64;

/// A table is currently stored in a file with same name
/// Therefore, length of table_disk_ptrs' keys and values are 32 bytes each
pub struct Database {
    // Map from table's name to file's name
    table_disk_ptrs: HashMap<String, String>
}

/// Storage format for Database:
///     - table_count: 8 bytes
///     - padding: 56 bytes
///     - table_name - file_name key-value pairs:
///         - table_name: 32 bytes (max 31 bytes + NULL ending bytes)
///         - file_name: 32 bytes (same as table_name)
/// Total: 4096 bytes (a page)
impl Storable for Database {
    const SIZE: Option<usize> = Some(PAGE_SIZE);

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::SIZE.unwrap() {
            return None;
        }

        let mut count_rdr = Cursor::new(&bytes[0..8]);
        let table_count = count_rdr.read_u64::<LittleEndian>().unwrap() as usize;

        let mut table_disk_ptrs: HashMap<String, String> = HashMap::<String, String>::new();
        let mut idx:usize = 0;
        let mut iter = bytes.chunks(MAP_ENTRY_SIZE);
        iter.next();        // Skip the first chunk (the table_count + padding)

        while let Some(chunk)= iter.next() {
            let table_name = utils::string_from_bytes(&chunk[0..32]).unwrap();
            let disk_ptr = utils::string_from_bytes(&chunk[32..64]).unwrap();

            assert!(table_disk_ptrs.get(&table_name).is_none());
            table_disk_ptrs.insert(table_name, disk_ptr);

            idx += 1;
            if idx >= table_count { break; }
        }

        assert_eq!(idx, table_count);
        Some(Database{ table_disk_ptrs: table_disk_ptrs })
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret:Vec<u8> = vec![];

        // Write table_count and padding
        ret.write_u64::<LittleEndian>(self.table_disk_ptrs.len() as u64).unwrap();
        ret.append(&mut vec![0;56]);

        for (table_name, disk_ptr) in self.table_disk_ptrs.iter() {
            ret.append(&mut utils::string_to_bytes(&table_name, 32).unwrap());
            ret.append(&mut utils::string_to_bytes(&disk_ptr, 32).unwrap());
        }

        let cur_len = ret.len();
        ret.append(&mut vec![0; Self::SIZE.unwrap() - cur_len]);

        Some(ret)
    }
}

impl Database {
    // pub fn create_table(&self, table: &Table) {

    // }

    pub fn load_table(&self, name: &String) -> Option<Table> {
        match self.table_disk_ptrs.get(name) {
            Some(disk_ptr) => {
                let mut reader = PageReader::new((*disk_ptr).clone(), 0).unwrap();
                let table_info_page = reader.consume_page();
                Some(table_info_page.iter::<Table>().next().unwrap())
            }
            None => None
        }
    }
}
