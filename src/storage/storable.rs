pub trait Storable {
    type Item;
    const SIZE: usize;

    fn from_bytes(bytes: &[u8]) -> Option<Self::Item>;
    fn to_bytes(&self) -> Option<Vec<u8>>;
}
