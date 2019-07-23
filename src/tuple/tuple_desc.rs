use data_type::DataType;
use internal_types::TupleData;
use nom_sql::Literal;

#[derive(Clone, Debug, PartialEq, Eq)]
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

    pub fn from_data(data: Vec<u8>) -> Result<TupleDesc, std::io::Error> {
        use storage::Storable;
        let (num_attrs, mut data) = u16::from_data(data)?;
        let mut attr_types = vec![];
        let mut attr_names = vec![];
        for _ in 0..num_attrs {
            let (attr_type, leftover) = DataType::from_data(data)?;
            let (name_len, mut leftover) = u16::from_data(leftover)?;
            // TODO update this unwrap
            let attr_name = String::from_utf8(leftover.drain(..name_len as usize).collect::<Vec<_>>()).unwrap();
            data = leftover;
            attr_types.push(attr_type);
            attr_names.push(attr_name);
        }

        Ok(TupleDesc::new(attr_types, attr_names))
    }

    pub fn to_data(&self) -> Vec<u8> {
        use storage::Storable;
        let mut ret = (self.num_attrs() as u16).to_data();
        ret.append(&mut (0..self.num_attrs() as usize)
            .map(|i| {
                vec![
                    self.attr_types[i].to_data(),
                    DataType::VarChar
                        .string_to_data(&self.attr_names[i])
                        .unwrap(),
                ]
                .concat()
            })
            .collect::<Vec<_>>()
            .concat());
        ret
    }

    pub fn data_from_literal(
        &self,
        inputs: Vec<Vec<Literal>>,
    ) -> Vec<TupleData> {
        inputs
            .iter()
            .map(|tup| {
                tup.iter()
                    .enumerate()
                    .map(|(i, literal)| {
                        self.attr_types[i].data_from_literal(&literal).unwrap()
                    })
                    .collect::<Vec<_>>()
                    .concat()
            })
            .collect()
    }

    pub fn create_tuple_data(&self, inputs: Vec<String>) -> TupleData {
        inputs
            .iter()
            .enumerate()
            .map(|(i, input)| {
                self.attr_types[i].string_to_data(&input).unwrap()
            })
            .collect::<Vec<_>>()
            .concat()
    }

    pub fn data_to_strings(
        &self,
        bytes: &[u8],
        filter_indices: Option<Vec<usize>>,
    ) -> Option<Vec<String>> {
        let cols = self.cols(bytes);
        let result = match filter_indices {
            Some(vec) => vec
                .iter()
                .map(|&i| self.attr_types[i].data_to_string(&cols[i]).unwrap())
                .collect(),
            None => self
                .attr_types
                .iter()
                .enumerate()
                .map(|(i, attr)| attr.data_to_string(&cols[i]).unwrap())
                .collect(),
        };
        Some(result)
    }

    pub fn cols(&self, bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut cols = vec![];
        let mut cur_bytes = 0;
        for attr in self.attr_types.iter() {
            let attr_len = attr
                .data_size(Some(&bytes[cur_bytes..bytes.len()]))
                .unwrap();
            cols.push(bytes[cur_bytes..cur_bytes + attr_len].to_vec());
            cur_bytes += attr_len;
        }

        cols
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

    pub fn attr_types(&self) -> Vec<DataType> {
        self.attr_types.clone()
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
