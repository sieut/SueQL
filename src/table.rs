use storage::{Storable, PAGE_SIZE};
use types;
use utils;

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
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::get_size().unwrap() { return None; }

        let name = utils::string_from_bytes(&bytes[0..32]).unwrap();

        let mut columns = vec![];
        let mut iter = bytes.chunks(32);
        iter.next();                    // Skip the first chunk (table_name)
        while let Some(chunk) = iter.next() {
            let col = Column::from_bytes(&chunk);
            if col.is_some() { columns.push(col.unwrap()); }
            else { break; }
        }

        Some(Table { name: name, columns: columns })
    }

    // TODO implement
    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret:Vec<u8> = vec![];

        ret.append(&mut utils::string_to_bytes(&self.name, 32).unwrap());

        for col in self.columns.iter() {
            ret.append(&mut col.to_bytes().unwrap());
        }

        let cur_len = ret.len();
        ret.append(&mut vec![0; Self::get_size().unwrap() - cur_len]);

        Some(ret)
    }

    fn get_size() -> Option<usize> { Some(PAGE_SIZE) }
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
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::get_size().unwrap() { return None; }

        let column_type = types::ColumnType::from_bytes(&[bytes[31]]).unwrap();
        let name = utils::string_from_bytes(&bytes[0..31]).unwrap();

        Some(Column {
            name: name,
            column_type: column_type
        })
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret:Vec<u8> = vec![];
        ret.append(&mut utils::string_to_bytes(&self.name, 31).unwrap());
        ret.append(&mut self.column_type.to_bytes().unwrap());
        assert_eq!(ret.len(), Self::get_size().unwrap());

        Some(ret)
    }

    fn get_size() -> Option<usize> { Some(32) }
}
