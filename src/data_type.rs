use std::io::Cursor;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};

enum_from_primitive!{
    #[derive(Copy, Clone)]
    pub enum DataType {
        Char,
        Integer,
        VarChar,
    }
}

impl DataType {
    pub fn size(&self, bytes: Option<&[u8]>) -> Option<usize> {
        match self {
            &DataType::Char => Some(1),
            &DataType::Integer => Some(4),
            &DataType::VarChar => {
                match bytes {
                    Some(bytes) => {
                        if bytes.len() >= 4 {
                            let mut cursor = Cursor::new(bytes);
                            Some(cursor.read_u32::<LittleEndian>().unwrap()
                                 as usize)
                        }
                        else { None }
                    },
                    None => None
                }
            },
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
            },
            &DataType::VarChar => {
                let mut bytes = vec![];
                LittleEndian::write_u32(&mut bytes, input.len() as u32);
                bytes.append(&mut input.as_bytes().to_vec());
                Some(bytes)
            }
        }
    }

    pub fn bytes_to_string(&self, bytes: &[u8]) -> Option<String> {
        match self {
            &DataType::Char => {
                if bytes.len() == self.size(None).unwrap() {
                    from_utf8(bytes.to_vec())
                }
                else { None }
            },
            &DataType::Integer => {
                if bytes.len() == self.size(None).unwrap() {
                    let mut cursor = Cursor::new(bytes);
                    Some(cursor.read_i32::<LittleEndian>().unwrap().to_string())
                }
                else { None }
            },
            &DataType::VarChar => {
                if bytes.len() == self.size(Some(bytes)).unwrap() {
                    from_utf8(bytes[4..bytes.len()].to_vec())
                }
                else { None }
            }
        }
    }
}

fn from_utf8(bytes: Vec<u8>) -> Option<String> {
    match String::from_utf8(bytes) {
        Ok(string) => Some(string),
        Err(_) => None
    }
}
