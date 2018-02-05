extern crate byteorder;

use storage::Storable;
use self::byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::io::Cursor;
use std::cmp::{Eq,Ordering};

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
            let mut rdr = Cursor::new(&bytes[0..Self::SIZE]);
            let int_value = rdr.read_i32::<LittleEndian>().unwrap();
            Some(Integer(int_value))
        }
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret = vec![];
        ret.write_i32::<LittleEndian>(self.0).unwrap();
        Some(ret)
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
    use storage::Storable;

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
