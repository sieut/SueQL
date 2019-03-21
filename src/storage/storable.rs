use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use std::io::Cursor;

// Only for constant-sized items, tuples cannot implement this
pub trait Storable
where
    Self: std::marker::Sized,
{
    fn size() -> usize;
    fn from_data(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), std::io::Error>;
    fn to_data(&self) -> Vec<u8>;

    /// Helper fn for Storable types
    /// Parsing data will be done with Cursor and byteorder,
    /// giving this fn the cursor will return the extra data
    /// after the parsing is done
    fn leftover_data(cursor: std::io::Cursor<Vec<u8>>) -> Vec<u8> {
        cursor.get_ref()[cursor.position() as usize..].to_vec()
    }
}

#[macro_use]
macro_rules! storable_for_primitive {
    ($primitive:ty, $parse_fn:ident, $write_fn:ident) => {
        impl Storable for $primitive {
            fn size() -> usize {
                std::mem::size_of::<$primitive>()
            }

            fn from_data(bytes: Vec<u8>) -> Result<(Self, Vec<u8>), std::io::Error> {
                let mut cursor = Cursor::new(bytes);
                let val = cursor.$parse_fn::<LittleEndian>()?;
                let leftover_data = Self::leftover_data(cursor);
                Ok((val, leftover_data))
            }

            fn to_data(&self) -> Vec<u8> {
                let mut data = vec![0u8; Self::size()];
                LittleEndian::$write_fn(&mut data, *self);
                data
            }
        }
    };
}

storable_for_primitive!(u32, read_u32, write_u32);
storable_for_primitive!(u64, read_u64, write_u64);
