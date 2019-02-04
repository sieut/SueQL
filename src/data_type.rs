use byteorder::{ByteOrder, LittleEndian};

enum_from_primitive!{
    #[derive(Copy, Clone)]
    pub enum DataType {
        Char,
        Integer,
    }
}

impl DataType {
    pub fn size(&self) -> Option<usize> {
        match self {
            &DataType::Char => Some(1),
            &DataType::Integer => Some(4),
        }
    }

    pub fn string_to_bytes(&self, input: &str) -> Option<Vec<u8>> {
        match self {
            &DataType::Char => {
                if input.len() == 1 { Some(input.as_bytes().to_vec()) }
                else { None }
            },
            &DataType::Integer => {
                match input.parse::<i32>() {
                    Ok(int) => {
                        let mut bytes: Vec<u8> = vec![];
                        LittleEndian::write_i32(&mut bytes, int);
                        Some(bytes)
                    },
                    Err(_) => None
                }
            }
        }
    }
}
