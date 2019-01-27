use data_type::DataType;

pub struct TupleDesc {
    attrs_types: Vec<DataType>,
}

impl TupleDesc {
    pub fn new(attrs_types: Vec<DataType>) -> TupleDesc {
        TupleDesc { attrs_types }
    }
}
