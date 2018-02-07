pub trait Storable {
    fn from_bytes(bytes: &[u8]) -> Option<Self> where Self: Sized;
    fn to_bytes(&self) -> Option<Vec<u8>>;
    fn get_size() -> Option<usize>;
}
