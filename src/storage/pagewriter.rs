use storage::{Storable, PAGE_SIZE, bufpage};
use std::fs::{File, OpenOptions};
use std::io::{Write, Seek, SeekFrom, Result};

pub struct PageWriter {
    file_name: String,
    page_offset: usize,
    file: File
}

impl PageWriter {
    pub fn new(file_name: String, page_offset: usize) -> Option<PageWriter> {
        match OpenOptions::new().create(true).write(true).read(true).open(file_name.clone()) {
            Ok(mut file) => {
                file.seek(SeekFrom::Start((page_offset * PAGE_SIZE) as u64)).unwrap();
                Some(PageWriter {
                    file_name: file_name.clone(),
                    page_offset: page_offset,
                    file: file
                })
            }
            Err(err) => None
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
