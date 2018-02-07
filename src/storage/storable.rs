pub trait Storable: Sized {
    const SIZE: Option<usize>;

    fn from_bytes(bytes: &[u8]) -> Option<Self>;
    fn to_bytes(&self) -> Option<Vec<u8>>;

    fn get_size() -> usize { Self::SIZE.unwrap() }
}
