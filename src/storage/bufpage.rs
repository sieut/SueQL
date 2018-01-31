use storage::{Storable, PAGE_SIZE};
use std::iter::Iterator;
use std::marker::PhantomData;

pub struct BufPage<T>
where T: Storable {
    data: Vec<u8>,
    data_type: PhantomData<T>
}

impl<T> BufPage<T>
where T: Storable {
    pub fn new(data_buffer: &[u8; PAGE_SIZE], data_size: usize) -> BufPage<T> {
        assert_eq!(data_size % T::SIZE, 0);
        BufPage::<T> {
            data: data_buffer[0..data_size].to_vec(),
            data_type: PhantomData
        }
    }

    pub fn push(&mut self, value: &T) {
        assert!(self.data.len() < PAGE_SIZE);
        self.data.append(&mut value.to_bytes().unwrap());
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn data(&self) -> &Vec<u8> { &self.data }
    pub fn len(&self) -> usize { self.data.len() }
    pub fn is_full(&self) -> bool { self.data.len() == PAGE_SIZE }

    // TODO is passing self to Iter, instead of &self right?
    pub fn iter(&self) -> Iter<T> {
        Iter {
            buf_page: self,
            index: 0
        }
    }

    // TODO is passing self to Iter, instead of &self right?
    pub fn iter_mut(&mut self) -> IterMut<T> {
        IterMut {
            buf_page: self,
            index: 0
        }
    }
}

pub struct Iter<'a, T: 'a>
where T: Storable {
    buf_page: &'a BufPage<T>,
    index: usize,
}

impl<'a, T> Iterator for Iter<'a, T>
where T: Storable {
    type Item = T::Item;

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

pub struct IterMut<'a, T: 'a>
where T: Storable {
    buf_page: &'a mut BufPage<T>,
    index: usize,
}

impl<'a, T> IterMut<'a, T>
where T: Storable {
    /// Update buf_page's underlying data buffer at self.index - 1
    /// The reason is that an item should be consumed and processed, before getting updated
    pub fn update(&mut self, new_value: &T) {
        // An item should be consumed before getting updated
        assert!(self.index != 0);

        let new_bytes = new_value.to_bytes().unwrap();
        assert_eq!(new_bytes.len(), T::SIZE);

        for i in 0..new_bytes.len() {
            let buf_page_index = (self.index - 1) * T::SIZE + i;
            self.buf_page.data[buf_page_index] = new_bytes[i];
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T>
where T: Storable {
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.buf_page.data.len() / T::SIZE {
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

mod test {
    extern crate rand;
    use storage::PAGE_SIZE;
    use storage::bufpage;
    use types::{Integer};

    #[test]
    fn test_iter() {
        let mut test_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        test_buf[0] = 1; test_buf[4] = 3; test_buf[8] = 10;

        let page = bufpage::BufPage::<Integer>::new(&test_buf, 12);
        let mut iter = page.iter();

        let mut val;
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), 1);
        assert_eq!(val.unwrap(), Integer::new(1));
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), 3);
        assert_eq!(val.unwrap(), Integer::new(3));
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), 10);
        assert_eq!(val.unwrap(), Integer::new(10));

        val = iter.next();
        assert!(val.is_none());
    }

    #[test]
    fn test_iter_empty() {
        let test_buf = [0; PAGE_SIZE];
        let page = bufpage::BufPage::<Integer>::new(&test_buf, 0);
        let mut iter = page.iter();
        let val = iter.next();
        assert!(val.is_none());
    }

    #[test]
    fn test_push() {
        let test_buf = [0; PAGE_SIZE];
        let mut page = bufpage::BufPage::<Integer>::new(&test_buf, 0);

        page.push(&Integer::new(20));
        page.push(&Integer::new(-100));

        let mut iter = page.iter();
        let mut val;
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), 20);
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), -100);
    }

    #[test]
    fn test_iter_mut() {
        let mut page = bufpage::BufPage::<Integer>::new(&[0; PAGE_SIZE], 0);

        page.push(&Integer::new(20));
        page.push(&Integer::new(-100));

        {
            let mut iter_mut = page.iter_mut();
            iter_mut.next();
            iter_mut.update(&Integer::new(40));
        }

        let mut iter = page.iter();
        let mut val;
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), 40);
        val = iter.next();
        assert_eq!(val.unwrap().get_value(), -100);
    }
}
