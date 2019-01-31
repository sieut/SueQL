use data_type::DataType;
use tuple::TupleData;

pub struct TupleDesc {
    attrs_types: Vec<DataType>,
}

impl TupleDesc {
    pub fn new(attrs_types: Vec<DataType>) -> TupleDesc {
        TupleDesc { attrs_types }
    }

    pub fn create_tuple_data(&self, inputs: Vec<String>) -> TupleData {
        let mut data = vec![];
        for (index, input) in inputs.iter().enumerate() {
            let data_type = self.attrs_types.get(index).unwrap();
            let mut bytes = data_type.string_to_bytes(&input).unwrap();
            data.append(&mut bytes);
        }

        data
    }
}
