use storage::Storable;
use std::cmp::{Eq,Ordering};

#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub struct Char(u8);

impl Char {
    pub fn new(value: u8) -> Char { Char(value) }
}

impl Storable for Char {
    type Item = Char;
    const SIZE: Option<usize> = Some(1);

    fn from_bytes(bytes: &[u8]) -> Option<Self::Item> {
        if bytes.len() != Self::SIZE.unwrap() {
            None
        }
        else {
            Some(Char(bytes[0]))
        }
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![self.0])
    }
}

impl Eq for Char {}

impl Ord for Char {
    fn cmp(&self, other: &Char) -> Ordering {
        self.0.cmp(&other.0)
    }
}
