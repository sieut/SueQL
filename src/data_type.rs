use bincode;
use error::{Error, Result};
use nom_sql::{Literal, SqlType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Char,
    U32,
    I32,
    U64,
    I64,
    VarChar,
    Bool,
}

impl DataType {
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
            SqlType::Bool => Some(DataType::Bool),
            _ => None,
        }
    }

    pub fn match_literal(&self, input: &Literal) -> bool {
        match (self, input) {
            (&DataType::Char, &Literal::String(ref string)) => {
                string.len() == 1
            }
            (&DataType::I32, &Literal::Integer(_))
            | (&DataType::U32, &Literal::Integer(_))
            | (&DataType::I64, &Literal::Integer(_))
            | (&DataType::U64, &Literal::Integer(_)) => true,
            (&DataType::VarChar, &Literal::String(_)) => true,
            (&DataType::Bool, &Literal::Integer(_)) => true,
            _ => false,
        }
    }

    pub fn literal_to_data(&self, input: &Literal) -> Result<Vec<u8>> {
        if !self.match_literal(input) {
            return Err(Error::Internal(String::from("Unmatched data type")));
        }

        match (self, input) {
            (&DataType::Char, &Literal::String(ref string)) => {
                Ok(string.as_bytes().to_vec())
            }
            (&DataType::VarChar, &Literal::String(ref string)) => {
                Ok(bincode::serialize(&string)?)
            }
            (&DataType::I32, &Literal::Integer(int)) => {
                Ok(bincode::serialize(&(int as i32))?)
            }
            (&DataType::U32, &Literal::Integer(int)) => {
                Ok(bincode::serialize(&(int as u32))?)
            }
            (&DataType::I64, &Literal::Integer(int)) => {
                Ok(bincode::serialize(&(int as i64))?)
            }
            (&DataType::U64, &Literal::Integer(int)) => {
                Ok(bincode::serialize(&(int as u64))?)
            }
            (&DataType::Bool, &Literal::Integer(int)) => {
                Ok(bincode::serialize(&((int != 0) as u8))?)
            }
            _ => Err(Error::Internal(String::from("Unmatched data type"))),
        }
    }

    pub fn data_size(&self, bytes: Option<&[u8]>) -> Result<usize> {
        match (self, bytes) {
            (&DataType::Char, _) => Ok(1),
            (&DataType::U32, _) | (&DataType::I32, _) => Ok(4),
            (&DataType::U64, _) | (&DataType::I64, _) => Ok(8),
            (&DataType::VarChar, Some(bytes)) => {
                let string: String = bincode::deserialize(bytes)?;
                Ok(bincode::serialized_size(&string)? as usize)
            }
            (&DataType::Bool, _) => Ok(1),
            _ => Err(Error::internal("Cannot determine data_size")),
        }
    }

    pub fn string_to_data(&self, input: &str) -> Result<Vec<u8>> {
        let err_string = format!("Failed to parse {}", input);
        match self {
            &DataType::Char => match input.len() {
                1 => Ok(input.as_bytes().to_vec()),
                _ => Err(Error::Internal(String::from("Unmatched data type"))),
            },
            &DataType::U32 => match input.parse::<u32>() {
                Ok(int) => Ok(bincode::serialize(&int)?),
                Err(_) => Err(Error::Internal(err_string)),
            },
            &DataType::I32 => match input.parse::<i32>() {
                Ok(int) => Ok(bincode::serialize(&int)?),
                Err(_) => Err(Error::Internal(err_string)),
            },
            &DataType::U64 => match input.parse::<u64>() {
                Ok(int) => Ok(bincode::serialize(&int)?),
                Err(_) => Err(Error::Internal(err_string)),
            },
            &DataType::I64 => match input.parse::<i64>() {
                Ok(int) => Ok(bincode::serialize(&int)?),
                Err(_) => Err(Error::Internal(err_string)),
            },
            &DataType::VarChar => Ok(bincode::serialize(input)?),
            &DataType::Bool => {
                if input == "true" {
                    Ok(bincode::serialize(&1u8)?)
                } else if input == "false" {
                    Ok(bincode::serialize(&0u8)?)
                } else {
                    match input.parse::<i32>() {
                        Ok(int) => Ok(bincode::serialize(&((int != 0) as u8))?),
                        Err(_) => Err(Error::Internal(err_string)),
                    }
                }
            }
        }
    }

    pub fn data_to_string(&self, bytes: &[u8]) -> Result<String> {
        match self {
            &DataType::Char => match String::from_utf8(bytes.to_vec()) {
                Ok(string) => Ok(string),
                Err(_) => {
                    Err(Error::Internal(String::from("Failed to parse data")))
                }
            },
            &DataType::U32 => {
                Ok(bincode::deserialize::<u32>(bytes)?.to_string())
            }
            &DataType::I32 => {
                Ok(bincode::deserialize::<i32>(bytes)?.to_string())
            }
            &DataType::U64 => {
                Ok(bincode::deserialize::<u64>(bytes)?.to_string())
            }
            &DataType::I64 => {
                Ok(bincode::deserialize::<i64>(bytes)?.to_string())
            }
            &DataType::VarChar => Ok(bincode::deserialize::<String>(bytes)?),
            &DataType::Bool => {
                let bool_u8: u8 = bincode::deserialize(bytes)?;
                match bool_u8 {
                    0 => Ok(String::from("false")),
                    _ => Ok(String::from("true")),
                }
            }
        }
    }
}
