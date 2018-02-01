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
