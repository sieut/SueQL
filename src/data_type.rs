extern crate num;
use self::num::FromPrimitive;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use nom_sql::{Literal, SqlType};
use std::io::Cursor;

enum_from_primitive! {
    #[derive(Debug, Copy, Clone)]
    pub enum DataType {
        Char,
        U32,
        I32,
        U64,
        I64,
        VarChar,
    }
}

impl DataType {
    pub fn from_data(data: &[u8]) -> Result<DataType, std::io::Error> {
        use std::io::{Error, ErrorKind};

        let mut cursor = Cursor::new(&data);
        let id = cursor.read_u16::<LittleEndian>()?;
        match DataType::from_u16(id) {
            Some(t) => {
                // NOTE: matching t because we might support
                // types with argument in the future, eg. Char(len)
                match t.clone() {
                    _ => Ok(t),
                }
            }
            None => Err(Error::new(ErrorKind::InvalidData, "Invalid type ID")),
        }
    }

    pub fn to_data(&self) -> Vec<u8> {
        let mut data = vec![0u8; 2];
        let id = *self as u16;
        LittleEndian::write_u16(&mut data, id);
        // NOTE when types with argument are supported, update this fn
        data
    }

    pub fn id_len(&self) -> usize {
        match self {
            _ => 2,
        }
    }

    pub fn from_nom_type(nom_type: SqlType) -> Option<DataType> {
        match nom_type {
            SqlType::Char(len) => {
                if len == 1 {
                    Some(DataType::Char)
                } else {
                    Some(DataType::VarChar)
                }
            }
            SqlType::Int(_) => Some(DataType::I32),
            SqlType::Varchar(_) => Some(DataType::VarChar),
            _ => None,
        }
    }

    pub fn match_literal(&self, input: &Literal) -> bool {
        match self {
            &DataType::Char => match input {
                &Literal::String(ref string) => string.len() == 1,
                _ => false,
            },
            &DataType::I32 => match input {
                &Literal::Integer(_) => true,
                _ => false,
            },
            &DataType::VarChar => match input {
                &Literal::String(_) => true,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn data_from_literal(&self, input: &Literal) -> Option<Vec<u8>> {
        if !self.match_literal(input) {
            return None;
        }

        match self {
            &DataType::Char => {
                if let &Literal::String(ref string) = input {
                    Some(string.as_bytes().to_vec())
                } else {
                    None
                }
            }
            &DataType::I32 => {
                if let &Literal::Integer(int) = input {
                    let mut bytes = vec![0u8; 4];
                    LittleEndian::write_i32(&mut bytes, int as i32);
                    Some(bytes)
                } else {
                    None
                }
            }
            &DataType::VarChar => {
                if let &Literal::String(ref string) = input {
                    let mut bytes = vec![0u8; 2];
                    LittleEndian::write_u16(&mut bytes, string.len() as u16);
                    bytes.append(&mut string.as_bytes().to_vec());
                    Some(bytes)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn data_size(&self, bytes: Option<&[u8]>) -> Option<usize> {
        match self {
            &DataType::Char => Some(1),
            &DataType::U32 | &DataType::I32 => Some(4),
            &DataType::U64 | &DataType::I64 => Some(8),
            &DataType::VarChar => match bytes {
                Some(bytes) => {
                    if bytes.len() >= 2 {
                        let mut cursor = Cursor::new(bytes);
                        Some(
                            (cursor.read_u16::<LittleEndian>().unwrap() + 2)
                                as usize,
                        )
                    } else {
                        None
                    }
                }
                None => None,
            },
        }
    }

    pub fn string_to_data(&self, input: &str) -> Option<Vec<u8>> {
        match self {
            &DataType::Char => {
                if input.len() == 1 {
                    Some(input.as_bytes().to_vec())
                } else {
                    None
                }
            }
            &DataType::U32 => match input.parse::<u32>() {
                Ok(int) => {
                    let mut bytes = vec![0u8; 4];
                    LittleEndian::write_u32(&mut bytes, int);
                    Some(bytes)
                }
                Err(_) => None,
            },
            &DataType::I32 => match input.parse::<i32>() {
                Ok(int) => {
                    let mut bytes = vec![0u8; 4];
                    LittleEndian::write_i32(&mut bytes, int);
                    Some(bytes)
                }
                Err(_) => None,
            },
            &DataType::U64 => match input.parse::<u64>() {
                Ok(int) => {
                    let mut bytes = vec![0u8; 8];
                    LittleEndian::write_u64(&mut bytes, int);
                    Some(bytes)
                }
                Err(_) => None,
            },
            &DataType::I64 => match input.parse::<i64>() {
                Ok(int) => {
                    let mut bytes = vec![0u8; 8];
                    LittleEndian::write_i64(&mut bytes, int);
                    Some(bytes)
                }
                Err(_) => None,
            },
            &DataType::VarChar => {
                let mut bytes = vec![0u8; 2];
                LittleEndian::write_u16(&mut bytes, input.len() as u16);
                bytes.append(&mut input.as_bytes().to_vec());
                Some(bytes)
            }
        }
    }

    pub fn data_to_string(&self, bytes: &[u8]) -> Option<String> {
        if bytes.len() == 0
            || bytes.len() != self.data_size(Some(bytes)).unwrap_or(0)
        {
            return None;
        }

        match self {
            &DataType::Char => from_utf8(bytes.to_vec()),
            &DataType::U32 => {
                let mut cursor = Cursor::new(bytes);
                Some(cursor.read_u32::<LittleEndian>().unwrap().to_string())
            }
            &DataType::I32 => {
                let mut cursor = Cursor::new(bytes);
                Some(cursor.read_i32::<LittleEndian>().unwrap().to_string())
            }
            &DataType::U64 => {
                let mut cursor = Cursor::new(bytes);
                Some(cursor.read_u64::<LittleEndian>().unwrap().to_string())
            }
            &DataType::I64 => {
                let mut cursor = Cursor::new(bytes);
                Some(cursor.read_i64::<LittleEndian>().unwrap().to_string())
            }
            &DataType::VarChar => from_utf8(bytes[2..bytes.len()].to_vec()),
        }
    }
}

fn from_utf8(bytes: Vec<u8>) -> Option<String> {
    match String::from_utf8(bytes) {
        Ok(string) => Some(string),
        Err(_) => None,
    }
}
