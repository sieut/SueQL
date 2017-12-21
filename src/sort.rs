use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::fs::File;
use std::mem::transmute;

const PAGE_SIZE:usize = 4096;
const SIZE_OF_I32:usize = 4;
const _TEMP_FILE1:&str = ".temp_sort1";
const _TEMP_FILE2:&str = ".temp_sort2";

// External sort
// v1: first pass quick sort
pub fn sort(_file: String) -> Result<bool, String> {
    let file: File;
    match File::open(_file) {
        Ok(opened_file) => file = opened_file,
        Err(_) => return Err(String::from("Error reading file"))
    }
    let mut f_reader = BufReader::new(file);

    match first_pass(&mut f_reader) {
        Ok(_) => {
            // subsequent passes here
        },
        Err(error) => return Err(error)
    }

    Ok(true)
}

fn first_pass(f_reader: &mut BufReader<File>) -> Result<bool, String> {
    let buffer_file: File;
    match File::create(_TEMP_FILE1) {
        Ok(file) => buffer_file = file,
        Err(_) => return Err(String::from("Error creating buffer file"))
    }
    let mut buffer_f_writer = BufWriter::new(buffer_file);

    let mut buffer = [0; PAGE_SIZE];
    let mut current_buf_size = 0;
    while let Ok(bytes_read) = f_reader.read(&mut buffer[current_buf_size..PAGE_SIZE]) {
        current_buf_size += bytes_read;

        // If we currently have a full page or the last page of file
        if current_buf_size == PAGE_SIZE || (bytes_read == 0 && current_buf_size > 0) {
            // TODO file is messed up?
            if current_buf_size % SIZE_OF_I32 != 0 { assert!(false, "Wrong buffer size"); }

            // cast the bytes into ints
            let mut ints_buffer = bytes_to_ints(&buffer, current_buf_size);

            ints_buffer.sort();

            // cast the sort ints back to bytes
            ints_to_bytes(&ints_buffer, &mut buffer);

            // write the page into temp file
            match buffer_f_writer.write_all(&buffer[0..current_buf_size]) {
                Ok(_) => {}
                //TODO write is messed up
                Err(_) => break
            }
        }

        // bytes_read = 0 when at EOF, so break
        if bytes_read == 0 { break; }
        // reset buffer for a new page when we've read a full page
        if current_buf_size == PAGE_SIZE { current_buf_size = 0; }
    };

    Ok(true)
}

fn bytes_to_ints(bytes_buffer: &[u8], size: usize) -> Vec<i32> {
    let mut ints_buffer = Vec::new();
    for i in 0..size/SIZE_OF_I32 {
        let mut slice_copy:[u8; SIZE_OF_I32] = [0,0,0,0];
        slice_copy.clone_from_slice(&bytes_buffer[i * SIZE_OF_I32..(i + 1) * SIZE_OF_I32]);
        ints_buffer.push(unsafe { transmute::<[u8; SIZE_OF_I32], i32>(slice_copy) } );
    }

    ints_buffer
}

fn ints_to_bytes(ints_buffer: &Vec<i32>, bytes_buffer: &mut [u8]) {
    for i in 0..ints_buffer.len() {
        let slice:[u8; SIZE_OF_I32] = unsafe { transmute::<i32, [u8; SIZE_OF_I32]>(ints_buffer[i]) };
        &bytes_buffer[i * SIZE_OF_I32..(i + 1) * SIZE_OF_I32].clone_from_slice(&slice);
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;

    use std::io::BufReader;
    use std::io::Read;
    use std::fs::File;
    use sort;

    #[test]
    fn test_conversions() {
        let mut test_buffer:[u8; 512] = [0; 512];
        // for i in 0..512 {
        //     test_buffer[i] = rand::random::<u8>();
        // }
        for x in test_buffer.iter_mut() { *x = rand::random::<u8>(); }

        let clone_buffer:[u8; 512] = test_buffer.clone();

        let ints_array = sort::bytes_to_ints(&test_buffer, 512);
        sort::ints_to_bytes(&ints_array, &mut test_buffer);

        for i in 0..512 { assert_eq!(clone_buffer[i], test_buffer[i]); }
    }

    // #[test]
    // fn test_1() {
    //     // test_file content: 1, 4, 25, 19, 32, 11, 72, 80
    //     let _test_file = String::from("sort_test");
    //     sort::sort(_test_file);

    //     let _first_pass = ".temp_sort_1";
    //     match File::open(_first_pass) {
    //         Ok(file) => {
    //             let mut reader = BufReader::new(file);
    //             let mut file_content:[u8; 32] = [0; 32];
    //             reader.read_exact(&mut file_content);
    //             assert!(true);
    //         }
    //         Err(_) => assert!(false, "failed to read .temp_sort_1 for first pass test")
    //     }
    // }
}
