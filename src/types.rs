pub trait Type {
    type SType;
    type CType;

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::CType>;
    fn get_value(&self) -> Option<Self::CType>;
    fn get_size(&self) -> usize;
}

pub enum ColumnType {
    Int(Integer),
}

struct Integer(i32);

impl Type for Integer {
    type SType = Integer;
    type CType = i32;

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::CType> {
        Some(0)
    }

    fn get_value(&self) -> Option<Self::CType> {
        Some(self.0)
    }

    fn get_size(&self) -> usize { 4 }
}
