use types;
use utils;
use storage::Storable;

/// Column's name is max 30 bytes long for now
#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub column_type: types::ColumnType,
}

/// Storage format of Column:
///     - name: 31 bytes (max 30 bytes + NULL ending bytes)
///     - column_type: 1 byte
/// Total: 32 bytes
impl Storable for Column {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::get_size().unwrap() { return None; }

        let column_type = types::ColumnType::from_bytes(&[bytes[31]]).unwrap();
        let name = utils::string_from_bytes(&bytes[0..31]).unwrap();

        Some(Column {
            name: name,
            column_type: column_type
        })
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut ret:Vec<u8> = vec![];
        ret.append(&mut utils::string_to_bytes(&self.name, 31).unwrap());
        ret.append(&mut self.column_type.to_bytes().unwrap());
        assert_eq!(ret.len(), Self::get_size().unwrap());

        Some(ret)
    }

    fn get_size() -> Option<usize> { Some(32) }
}
