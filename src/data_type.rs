pub enum DataType {
    Char,
}

impl DataType {
    pub fn size(&self) -> Option<usize> {
        match self {
            &DataType::Char => Some(1),
        }
    }

    pub fn string_to_bytes(&self, input: &str) -> Option<Vec<u8>> {
        match self {
            &DataType::Char => {
                if input.len() == 1 { Some(input.as_bytes().to_vec()) }
                else { None }
            },
        }
    }
}
