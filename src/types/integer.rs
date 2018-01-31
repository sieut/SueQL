use storage::Storable;
use std::cmp::{Eq,Ordering};
use std::mem::transmute;

#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub struct Integer(i32);

impl Integer {
    pub fn new(value: i32) -> Integer { Integer(value) }
}

impl Storable for Integer {
    type Item = Integer;
    const SIZE:usize = 4;

    fn from_bytes(bytes: &[u8]) -> Option<Self::Item> {
        if bytes.len() != Self::SIZE {
            None
        }
        else {
            let int_value:i32 = unsafe { transmute::<[u8; Self::SIZE], i32>([bytes[0], bytes[1], bytes[2], bytes[3]]) };
            Some(Integer(int_value))
        }
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        let bytes_arr = unsafe { transmute::<i32, [u8; Self::SIZE]>(self.0) };
        Some(bytes_arr.to_vec())
    }
}

impl Eq for Integer {}

impl Ord for Integer {
    fn cmp(&self, other: &Integer) -> Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use types::Integer;

    #[test]
    fn test_integer_from_bytes() {
        let buffer = [1, 0, 0, 0];
        let int = Integer::from_bytes(&buffer);
        assert_eq!(int.unwrap(), Integer::new(1));
    }

    #[test]
    fn test_integer_to_bytes() {
        let int = Integer::new(10);
        let bytes = int.to_bytes().unwrap();

        assert_eq!(bytes.len(), 4);

        assert_eq!(bytes[0], 10);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 0);
        assert_eq!(bytes[3], 0);
    }
}
