use types;
use std::string::String;
use std::vec::Vec;

pub struct Table {
    name: String,
    columns: Vec<Column>
}

pub struct Column {
    name: String,
    column_type: types::ColumnType,
}
