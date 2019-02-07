extern crate num;
use self::num::FromPrimitive;

use data_type::DataType;
use tuple::TupleData;

pub struct TupleDesc {
    pub attr_types: Vec<DataType>,
}

impl TupleDesc {
    pub fn new(attr_types: Vec<DataType>) -> TupleDesc {
        assert!(attr_types.len() < 10000);
        TupleDesc { attr_types }
    }

    pub fn from_attr_ids(attr_ids: &Vec<u32>) -> Option<TupleDesc> {
        assert!(attr_ids.len() < 10000);
        let mut attr_types = vec![];
        for id in attr_ids.iter() {
            match DataType::from_u32(*id) {
                Some(t) => { attr_types.push(t); },
                None => return None
            }
        }

        Some(TupleDesc::new(attr_types))
    }

    pub fn create_tuple_data(&self, inputs: Vec<String>) -> TupleData {
        let mut data = vec![];
        for (index, input) in inputs.iter().enumerate() {
            let data_type = self.attr_types.get(index).unwrap();
            let mut bytes = data_type.string_to_bytes(&input).unwrap();
            data.append(&mut bytes);
        }

        data
    }

    pub fn assert_data_len(&self, data: &[u8]) -> Result<(), std::io::Error> {
        let mut sum = 0;
        for attr in self.attr_types.iter() {
            sum += match attr.size() {
                Some(size) => size,
                None => { return Ok(()); }
            }
        }

        if sum == data.len() {
            Ok(())
        }
        else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                                    "Data doesn't match with tuple desc"))
        }
    }

    pub fn num_attrs(&self) -> u32 {
        self.attr_types.len() as u32
    }
}
