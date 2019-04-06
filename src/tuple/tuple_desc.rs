use data_type::DataType;
use internal_types::TupleData;
use nom_sql::Literal;

#[derive(Clone)]
pub struct TupleDesc {
    attr_types: Vec<DataType>,
    attr_names: Vec<String>,
}

impl TupleDesc {
    pub fn new<S>(attr_types: Vec<DataType>, attr_names: Vec<S>) -> TupleDesc
    where
        S: Into<String>,
    {
        assert!(attr_types.len() < 10000);
        assert_eq!(attr_types.len(), attr_names.len());
        TupleDesc {
            attr_types,
            attr_names: attr_names
                .into_iter()
                .map(|name| name.into())
                .collect(),
        }
    }

    pub fn from_data(data: &Vec<Vec<u8>>) -> Result<TupleDesc, std::io::Error> {
        let mut attr_types = vec![];
        let mut attr_names = vec![];
        for bytes in data.iter() {
            let attr_type = DataType::from_data(&bytes)?;
            let attr_name = DataType::VarChar
                .data_to_string(&bytes[attr_type.id_len()..bytes.len()])
                .unwrap();
            attr_types.push(attr_type);
            attr_names.push(attr_name);
        }

        Ok(TupleDesc::new(attr_types, attr_names))
    }

    pub fn to_data(&self) -> Vec<Vec<u8>> {
        let mut result = vec![];
        for i in 0..self.attr_types.len() {
            let mut data = self.attr_types[i].to_data();
            let mut name_data = DataType::VarChar
                .string_to_data(&self.attr_names[i])
                .unwrap();
            data.append(&mut name_data);
            result.push(data);
        }

        result
    }

    pub fn data_from_literal(
        &self,
        inputs: Vec<Vec<Literal>>,
    ) -> Vec<TupleData> {
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
            let mut col_data = data_type.string_to_data(&input).unwrap();
            data.append(&mut col_data);
        }

        data
    }

    pub fn data_to_strings(
        &self,
        bytes: &[u8],
        filter_indices: Option<Vec<usize>>,
    ) -> Option<Vec<String>> {
        let mut full_data = vec![];
        let mut bytes_used = 0;
        for attr in self.attr_types.iter() {
            let attr_size = attr
                .data_size(Some(&bytes[bytes_used..bytes.len()]))
                .unwrap();
            let slice = &bytes[bytes_used..bytes_used + attr_size];
            match attr.data_to_string(slice) {
                Some(string) => full_data.push(string),
                None => {
                    return None;
                }
            };
            bytes_used += attr.data_size(Some(slice)).unwrap();
        }

        let result = match filter_indices {
            Some(vec) => vec.iter().map(|i| full_data[*i].clone()).collect(),
            None => full_data,
        };
        Some(result)
    }

    pub fn assert_data_len(&self, data: &[u8]) -> Result<(), std::io::Error> {
        let mut sum = 0;
        for attr in self.attr_types.iter() {
            sum += match attr.data_size(Some(&data[sum..data.len()])) {
                Some(size) => size,
                None => {
                    return Ok(());
                }
            }
        }

        if sum == data.len() {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Data doesn't match with tuple desc",
            ))
        }
    }

    pub fn num_attrs(&self) -> u32 {
        self.attr_types.len() as u32
    }

    pub fn attr_names(&self) -> Vec<String> {
        self.attr_names.clone()
    }

    pub fn attr_index(&self, name: &str) -> Option<usize> {
        self.attr_names
            .iter()
            .position(|attr_name| attr_name == name)
    }
}
