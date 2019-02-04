use std::fs::File;
use std::io::Write;
use storage::PAGE_SIZE;

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
    let mut file = File::create(fname)?;
    file.write_all(&[0; PAGE_SIZE as usize])?;
    Ok(())
}
