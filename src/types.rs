use std::cmp::Ordering;

pub trait Type {
    type SType;
    type CType;

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::SType>;
    fn get_value(&self) -> Self::CType;
    fn get_size(&self) -> usize;

    fn compare(&self, rhs: Self::SType) -> Ordering;
}

pub enum ColumnType {
    Int(Integer),
}

struct Integer(i32);

impl Type for Integer {
    type SType = Integer;
    type CType = i32;

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::SType> {
        Some(Integer(0))
    }

    fn get_value(&self) -> Self::CType {
        self.0
    }

    fn get_size(&self) -> usize { 4 }

    fn compare(&self, rhs: Self::SType) -> Ordering {
        self.0.cmp(&rhs.get_value())
    }
}
