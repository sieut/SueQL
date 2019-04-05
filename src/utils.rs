use std::fs::File;
use std::io::Write;

#[macro_export]
macro_rules! dbg_log {
    ($($log_expr:expr),+) => {
        dbg!();
        println!($($log_expr),+);
    }
}

pub fn assert_data_len(data: &[u8], desired_len: usize) -> Result<(), std::io::Error> {
    if data.len() == desired_len {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Data does not have desired length",
        ))
    }
}

pub fn create_file(fname: &str) -> Result<(), std::io::Error> {
    use storage::buf_page::BufPage;

    let mut file = File::create(fname)?;
    file.write_all(&BufPage::default_buf())?;
    Ok(())
}

pub fn file_len(fname: &str) -> Result<u64, std::io::Error> {
    use std::fs::metadata;
    use std::io::{Error, ErrorKind};

    let file_meta = metadata(fname)?;
    if !file_meta.is_file() {
        Err(Error::new(ErrorKind::InvalidInput, "Path is not a file"))
    } else {
        Ok(file_meta.len())
    }
}
