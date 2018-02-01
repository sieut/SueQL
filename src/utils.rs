/// Convert a byte slice, representing a null-ending string, to a String
pub fn string_from_bytes(bytes: &[u8]) -> Option<String> {
    // Find the first 0 byte to split
    let mut splitting_idx = bytes.len();
    for (idx, val) in bytes.iter().enumerate() {
        if *val == 0 {
            splitting_idx = idx;
            break;
        }
    }

    match String::from_utf8(bytes[0..splitting_idx].iter().cloned().collect()) {
        Ok(val) => Some(val),
        Err(_) => None
    }
}

/// Convert a String to bytes vec, max_len includes the NULL ending byte
pub fn string_to_bytes(string: &String, max_len: usize) -> Option<Vec<u8>> {
    let mut ret:Vec<u8> = string.clone().into_bytes();
    if ret.len() >= max_len {
        None
    }
    else {
        let ret_len = ret.len();
        ret.append(&mut vec![0; max_len - ret_len]);
        Some(ret)
    }
}
