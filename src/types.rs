pub trait Castable {
    type Type;

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::Type>;
}
