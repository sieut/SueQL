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

#[cfg(test)]
mod tests {
    use storage::{PageReader, PageWriter, PAGE_SIZE};
    use storage::bufpage::BufPage;
    use std::fs::remove_file;

    #[test]
    fn test_write_new_file() {
        {
            let mut writer = PageWriter::new(String::from("writer_test_write_new_file"), 0).unwrap();
            let buffer = BufPage::new(&[5; PAGE_SIZE], PAGE_SIZE);

            writer.store(&buffer).unwrap();
            writer.store(&buffer).unwrap();
        }
        {
            let mut reader = PageReader::new(String::from("writer_test_write_new_file"), 0).unwrap();
            let mut buffer;

            buffer = reader.consume_page();
            for byte in buffer.data().iter() { assert_eq!(*byte, 5); }
            buffer = reader.consume_page();
            for byte in buffer.data().iter() { assert_eq!(*byte, 5); }
        }

        remove_file("writer_test_write_new_file").unwrap();
    }

    #[test]
    fn test_overwrite() {
        {
            let mut writer = PageWriter::new(String::from("writer_test_overwrite"), 0).unwrap();
            let buffer = BufPage::new(&[5; PAGE_SIZE], PAGE_SIZE);

            writer.store(&buffer).unwrap();
            writer.store(&buffer).unwrap();
            writer.store(&buffer).unwrap();
            writer.store(&buffer).unwrap();
        }
        {
            let mut writer = PageWriter::new(String::from("writer_test_overwrite"), 1).unwrap();
            let buffer = BufPage::new(&[1; PAGE_SIZE], PAGE_SIZE);

            writer.store(&buffer).unwrap();
            writer.store(&buffer).unwrap();
        }
        // After these 2 blocks, the file should look like
        //      5's: 1 page
        //      1's: 2 pages
        //      5's: 1 page
        {
            let mut reader = PageReader::new(String::from("writer_test_overwrite"), 0).unwrap();
            let mut buffer;

            buffer = reader.consume_page();
            for byte in buffer.data().iter() { assert_eq!(*byte, 5); }
            buffer = reader.consume_page();
            for byte in buffer.data().iter() { assert_eq!(*byte, 1); }
            buffer = reader.consume_page();
            for byte in buffer.data().iter() { assert_eq!(*byte, 1); }
            buffer = reader.consume_page();
            for byte in buffer.data().iter() { assert_eq!(*byte, 5); }

            buffer = reader.consume_page();
            assert!(buffer.data().len() == 0);
        }

        remove_file("writer_test_overwrite").unwrap();
    }
}
