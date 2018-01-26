use storage::PAGE_SIZE;
use types::Type;
use std::iter::Iterator;
use std::marker::PhantomData;

pub struct BufPage<T>
where T: Type {
    data: Vec<u8>,
    index: usize,
    data_type: PhantomData<T>
}

impl<T> Iterator for BufPage<T>
where T: Type {
    type Item = T::SType;

    fn next(&mut self) -> Option<Self::Item> {
        let item:Self::Item = T::from_bytes(&self.data[self.index * T::SIZE..(self.index + 1) * T::SIZE]).unwrap();
        self.index += 1;

        Some(item)
    }
}
