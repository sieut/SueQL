use storage::PAGE_SIZE;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct PageReader {
    file_name: String,
    page_offset: usize,
    file: File
}

impl PageReader {
    fn new(file_name: String, page_offset: usize) -> Option<PageReader> {
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
}
