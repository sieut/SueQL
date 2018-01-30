pub use self::integer::Integer;
pub use self::char::Char;

mod integer;
mod char;

pub trait Type {
    type SType;
    type CType;
    const SIZE:usize;

    fn from_bytes(bytes: &[u8]) -> Option<Self::SType>;
    fn to_bytes(&self) -> Option<Vec<u8>>;
    fn get_value(&self) -> Self::CType;
    fn get_size() -> usize { Self::SIZE }
}

#[derive(Copy, Clone)]
pub enum ColumnType {
    Int(Integer),
}
