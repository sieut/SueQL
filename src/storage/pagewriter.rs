use storage::{Storable, PAGE_SIZE, bufpage};
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
                    file.seek(SeekFrom::Start((page_offset * PAGE_SIZE) as u64)).unwrap();
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

    // TODO update page offset
    pub fn store(&mut self, page: &bufpage::BufPage) -> Result<()> {
        match self.file.write_all(page.data().as_slice()) {
            Ok(ok) => {
                self.page_offset += 1;
                Ok(ok)
            }
            Err(err) => Err(err)
        }
    }
}
