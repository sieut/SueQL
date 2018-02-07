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

impl Storable for ColumnType {
    const SIZE: Option<usize> = Some(1);

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
}
