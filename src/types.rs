use std::cmp::Ordering;

pub trait Type {
    type SType;
    type CType;

    fn from_bytes(bytes: &[u8]) -> Option<Self::SType>;
    fn get_value(&self) -> Self::CType;
    fn get_size(&self) -> usize;

    fn compare(&self, rhs: Self::SType) -> Ordering;
}

pub enum ColumnType {
    Int(Integer),
}

pub struct Integer(i32);

impl Type for Integer {
    type SType = Integer;
    type CType = i32;

    fn from_bytes(bytes: &[u8]) -> Option<Self::SType> {
        if bytes.len() != 4 {
            None
        }
        else {
            let mut int_value:i32 = 0;
            int_value |= ((bytes[0] as i32) << 24);
            int_value |= ((bytes[1] as i32) << 16);
            int_value |= ((bytes[2] as i32) << 8);
            int_value |= (bytes[3] as i32);
            Some(Integer(int_value))
        }
    }

    fn get_value(&self) -> Self::CType {
        self.0
    }

    fn get_size(&self) -> usize { 4 }

    fn compare(&self, rhs: Self::SType) -> Ordering {
        self.0.cmp(&rhs.get_value())
    }
}
