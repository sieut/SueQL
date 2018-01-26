use types::Type;
use storage::{PAGE_SIZE,bufpage};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct PageReader {
    file_name: String,
    page_offset: usize,
    file: File
}

impl PageReader {
    pub fn new(file_name: String, page_offset: usize) -> Option<PageReader> {
        match File::open(file_name.clone()) {
            Ok(mut file) => {
                file.seek(SeekFrom::Start((page_offset * PAGE_SIZE) as u64));
                Some(PageReader {
                    file_name: file_name.clone(),
                    page_offset: page_offset,
                    file: file
                })
            },
            Err(_) => None
        }
    }

    pub fn consume_page<T>(&mut self) -> bufpage::BufPage<T>
    where T: Type {
        let mut buffer = [0; PAGE_SIZE];
        let mut bytes_read = 0;

        while let Ok(b) = self.file.read(&mut buffer[bytes_read..PAGE_SIZE]) {
            bytes_read += b;
            if bytes_read == PAGE_SIZE || b == 0 {
                self.page_offset += 1;
                break;
            }
        }

        bufpage::BufPage::<T>::new(&buffer, bytes_read)
    }
}
