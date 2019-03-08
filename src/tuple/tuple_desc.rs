extern crate num;
use self::num::FromPrimitive;

use data_type::DataType;
use nom_sql::Literal;
use tuple::TupleData;

#[derive(Clone)]
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

    pub fn data_from_literal(&self, inputs: Vec<Vec<Literal>>) -> Vec<TupleData> {
        let mut tuples = vec![];
        for tup in inputs.iter() {
            let mut data = vec![];
            for (index, input) in tup.iter().enumerate() {
                let data_type = self.attr_types.get(index).unwrap();
                let mut bytes = data_type.data_from_literal(&input).unwrap();
                data.append(&mut bytes);
            }
            tuples.push(data);
        }

        tuples
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

    pub fn data_to_strings(&self, bytes: &[u8]) -> Option<Vec<String>> {
        let mut result = vec![];
        let mut bytes_used = 0;
        for attr in self.attr_types.iter() {
            let attr_size = attr.size(
                Some(&bytes[bytes_used..bytes.len()])).unwrap();
            let slice = &bytes[bytes_used..bytes_used + attr_size];
            match attr.bytes_to_string(slice) {
                Some(string) => result.push(string),
                None => { return None; }
            };
            bytes_used += attr.size(Some(slice)).unwrap();
        }

        Some(result)
    }

    pub fn assert_data_len(&self, data: &[u8]) -> Result<(), std::io::Error> {
        let mut sum = 0;
        for attr in self.attr_types.iter() {
            sum += match attr.size(Some(&data[sum..data.len()])) {
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
