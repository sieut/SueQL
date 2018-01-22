use std::fs::File;

pub struct PageWriter {
    file_name: String,
    page_offset: usize,
    file: File
}

impl PageWriter {
    fn new(file_name: String, page_offset: usize, new_file: bool) -> Option<PageWriter> {
        if new_file {
            match File::create(file_name.clone()) {
                Ok(file) => Some(PageWriter {
                        file_name: file_name.clone(),
                        page_offset: page_offset,
                        file: file
                    }),
                Err(_) => None
            }
        } else {
            match File::open(file_name.clone()) {
                Ok(file) => Some(PageWriter {
                        file_name: file_name.clone(),
                        page_offset: page_offset,
                        file: file
                    }),
                Err(_) => None
            }
        }
    }
}
