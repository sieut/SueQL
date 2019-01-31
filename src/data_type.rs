pub enum DataType {
    Char,
}

impl DataType {
    pub fn size(&self) -> Option<usize> {
        match self {
            &DataType::Char => Some(1),
        }
    }
}
