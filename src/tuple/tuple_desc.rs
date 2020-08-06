use data_type::DataType;
use error::{Error, Result};
use internal_types::TupleData;
use nom_sql::Literal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn data_subset(
        &self,
        data: &TupleData,
        indices: &Vec<usize>,
    ) -> Result<TupleData> {
        let cols = self.cols(data)?;
        Ok(indices
            .iter()
            .map(|i| cols.get(*i).cloned())
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::Internal(
                "Invalid index for TupleDesc::data_subset".to_string()
            ))?
            .concat())
    }

    pub fn subset(&self, indices: &Vec<usize>) -> Result<TupleDesc> {
        let attr_types = indices
            .iter()
            .map(|i| self.attr_types.get(*i).cloned())
            .collect::<Option<Vec<_>>>();
        let attr_names = indices
            .iter()
            .map(|i| self.attr_names.get(*i).cloned())
            .collect::<Option<Vec<_>>>();

        match (attr_types, attr_names) {
            (Some(types), Some(names)) => Ok(TupleDesc::new(types, names)),
            _ => Err(Error::Internal(
                    "Invalid index for TupleDesc::subset".to_string()))
        }
    }

    pub fn literal_to_data(
        &self,
        inputs: Vec<Vec<Literal>>,
    ) -> Result<Vec<TupleData>> {
        let mut result = vec![];
        for tup in inputs.iter() {
            let mut data = vec![];
            for (i, literal) in tup.iter().enumerate() {
                data.append(&mut self.attr_types[i].literal_to_data(&literal)?);
            }
            result.push(data);
        }
        Ok(result)
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
    ) -> Result<Vec<String>> {
        let cols = self.cols(bytes)?;
        let result = match filter_indices {
            Some(indices) => indices
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
        Ok(result)
    }

    pub fn cols(&self, bytes: &[u8]) -> Result<Vec<TupleData>> {
        let mut cols = vec![];
        let mut cur_bytes = 0;
        for attr in self.attr_types.iter() {
            let attr_len =
                attr.data_size(Some(&bytes[cur_bytes..bytes.len()]))?;
            cols.push(bytes[cur_bytes..cur_bytes + attr_len].to_vec());
            cur_bytes += attr_len;
        }

        Ok(cols)
    }

    pub fn assert_data_len(&self, data: &[u8]) -> Result<()> {
        let mut sum = 0;
        for attr in self.attr_types.iter() {
            sum += attr.data_size(Some(&data[sum..data.len()]))?;
        }

        if sum == data.len() {
            Ok(())
        } else {
            Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Data doesn't match with tuple desc",
            )))
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
