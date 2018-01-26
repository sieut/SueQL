use storage::{PAGE_SIZE, bufpage};
use types::Type;
use std::fs::File;
use std::io::{Write, Seek, SeekFrom, Result};

pub struct PageWriter {
    file_name: String,
    page_offset: usize,
    file: File
}

impl PageWriter {
    pub fn new(file_name: String, page_offset: usize, new_file: bool) -> Option<PageWriter> {
        // TODO code repetition
        if new_file {
            assert!(page_offset == 0);
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
                Ok(mut file) => {
                    file.seek(SeekFrom::Start((page_offset * PAGE_SIZE) as u64));
                    Some(PageWriter {
                        file_name: file_name.clone(),
                        page_offset: page_offset,
                        file: file
                    })
                },
                Err(_) => None
            }
        }
    }

    // Type T doesn't really matter here, it's just required
    pub fn store<T>(&mut self, page: &bufpage::BufPage<T>) -> Result<()>
    where T: Type {
        self.file.write_all(page.data().as_slice())
    }
}
