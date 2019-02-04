pub fn assert_data_len(data: &[u8], desired_len: usize) -> Result<(), std::io::Error> {
    if data.len() == desired_len {
        Ok(())
    }
    else {
        Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                                "Data does not have desired length"))
    }
}
