use storage::{Storable, PAGE_SIZE};
use types;

/// Table's name is max 31 bytes long, aligning with Column's size, for now
pub struct Table {
    name: String,
    columns: Vec<Column>
}

/// Storage format for Table:
///     - name: 32 bytes (max 31 bytes + NULL ending bytes)
///     - columns: rest of page
/// Total: 4096 bytes (a page)
impl Storable for Table {
    type Item = Table;
    const SIZE: usize = PAGE_SIZE;

    fn from_bytes(bytes: &[u8]) -> Option<Self::Item> {
        if bytes.len() != Self::SIZE { return None; }

        let mut splitting_idx = 32;
        for (idx, val) in bytes[0..32].iter().enumerate() {
            if *val == 0 {
                splitting_idx = idx;
                break;
            }
        }

        match String::from_utf8(bytes[0..splitting_idx].iter().cloned().collect()) {
            Ok(name) => {
                let mut columns = vec![];
                for i in 1..PAGE_SIZE/Column::SIZE {
                    let col = Column::from_bytes(&bytes[i * Column::SIZE..(i + 1) * Column::SIZE]);
                    if col.is_some() { columns.push(col.unwrap()); }
                    else { break; }
                }

                Some(Table { name: name, columns: columns })
            }
            Err(_) => None
        }
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Column's name is max 30 bytes long for now
pub struct Column {
    name: String,
    column_type: types::ColumnType,
}

/// Storage format of Column:
///     - name: 31 bytes (max 30 bytes + NULL ending bytes)
///     - column_type: 1 byte
/// Total: 32 bytes
impl Storable for Column {
    type Item = Column;
    const SIZE: usize = 32;

    fn from_bytes(bytes: &[u8]) -> Option<Self::Item> {
        if bytes.len() != Self::SIZE { return None; }

        let column_type = types::ColumnType::from_bytes(&[bytes[31]]).unwrap();

        let mut splitting_idx = 31;
        for (idx, val) in bytes[0..31].iter().enumerate() {
            if *val == 0 {
                splitting_idx = idx;
                break;
            }
        }

        match String::from_utf8(bytes[0..splitting_idx].iter().cloned().collect()) {
            Ok(name) => Some(Column { name: name, column_type: column_type }),
            Err(_) => None
        }
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret = self.name.clone().into_bytes();
        assert!(ret.len() <= 30);

        let name_bytes_count = ret.len();
        ret.append(&mut vec![0; 31 - name_bytes_count]);
        ret.append(&mut self.column_type.to_bytes().unwrap());
        assert_eq!(ret.len(), 32);

        Some(ret)
    }
}
