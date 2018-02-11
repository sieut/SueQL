use storage::Storable;

pub use self::integer::Integer;
pub use self::char::Char;

mod integer;
mod char;

#[derive(Copy, Clone)]
pub enum ColumnType {
    Char,
    Int,
}

impl ColumnType {
    pub fn data_size(&self) -> usize {
        match self {
            &ColumnType::Int => integer::Integer::get_size().unwrap(),
            &ColumnType::Char => char::Char::get_size().unwrap(),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        match self {
            &ColumnType::Int => true,
            &ColumnType::Char => true,
        }
    }
}

impl Storable for ColumnType {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 1 {
            None
        }
        else {
            match bytes[0] {
                255 => Some(ColumnType::Int),
                254 => Some(ColumnType::Char),
                _ => None
            }
        }
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            &ColumnType::Int => Some(vec![255]),
            &ColumnType::Char => Some(vec![254]),
        }
    }

    fn get_size() -> Option<usize> { Some(1) }
}
