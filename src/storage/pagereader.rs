use std::fs::File;

pub struct PageReader {
    file_name: String,
    page_offset: usize,
    file: File
}

impl PageReader {
    fn new(file_name: String, page_offset: usize) -> Option<PageReader> {
        match File::open(file_name.clone()) {
            Ok(file) => Some(PageReader {
                    file_name: file_name.clone(),
                    page_offset: page_offset,
                    file: file
                }),
            Err(_) => None
        }
    }
}
