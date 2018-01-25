use std::cmp::Ordering;
use std::mem::transmute;

pub trait Type {
    type SType;
    type CType;
    const SIZE:usize;

    fn from_bytes(bytes: &Vec<u8>) -> Option<Self::SType>;
    fn get_value(&self) -> Self::CType;
    fn get_size(&self) -> usize { Self::SIZE }

    fn compare(&self, rhs: Self::SType) -> Ordering;
}

#[derive(Copy, Clone)]
pub enum ColumnType {
    Int(Integer),
}

#[derive(Copy, Clone)]
pub struct Integer(i32);

impl Type for Integer {
    type SType = Integer;
    type CType = i32;
    const SIZE:usize = 4;

    fn from_bytes(bytes: &Vec<u8>) -> Option<Self::SType> {
        if bytes.len() != Self::SIZE {
            None
        }
        else {
            let int_value:i32 = unsafe { transmute::<[u8; Self::SIZE], i32>([bytes[0], bytes[1], bytes[2], bytes[3]]) };
            Some(Integer(int_value))
        }
    }

    fn get_value(&self) -> Self::CType {
        self.0
    }

    fn compare(&self, rhs: Self::SType) -> Ordering {
        self.0.cmp(&rhs.get_value())
    }
}
