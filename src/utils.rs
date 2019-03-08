use std::fs::File;
use std::io::Write;

pub fn assert_data_len(data: &[u8], desired_len: usize) -> Result<(), std::io::Error> {
    if data.len() == desired_len {
        Ok(())
    }
    else {
        Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                                "Data does not have desired length"))
    }
}

pub fn create_file(fname: &str) -> Result<(), std::io::Error> {
    use storage::buf_page::BufPage;

    let mut file = File::create(fname)?;
    file.write_all(&BufPage::default_buf())?;
    Ok(())
}
