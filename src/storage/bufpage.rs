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

impl<T> BufPage<T>
where T: Type {
    pub fn new(data_buffer: &[u8; PAGE_SIZE], data_size: usize) -> BufPage<T> {
        assert_eq!(data_size % T::SIZE, 0);
        BufPage::<T> {
            data: data_buffer[0..data_size].to_vec(),
            index: 0,
            data_type: PhantomData
        }
    }

    pub fn data(&self) -> &Vec<u8> { &self.data }

    pub fn iter(&self) -> Iter<T> {
        Iter {
            buf_page: &self,
            index: 0
        }
    }
}

pub struct Iter<'a, T: 'a>
where T: Type {
    buf_page: &'a BufPage<T>,
    index: usize,
}

impl<'a, T> Iterator for Iter<'a, T>
where T: Type {
    type Item = T::SType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.buf_page.data.len() / T::SIZE {
            None
        }
        else {
            let item:Self::Item = T::from_bytes(&self.buf_page.data[self.index * T::SIZE..(self.index + 1) * T::SIZE]).unwrap();
            self.index += 1;

            Some(item)
        }
    }

    fn count(self) -> usize { self.buf_page.data.len() / T::SIZE }
}
