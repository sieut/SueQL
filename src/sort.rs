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
    match first_pass(_file) {
        Ok(mut runs) => {
            // subsequent passes here
        },
        Err(error) => return Err(error)
    }

    Ok(true)
}

fn first_pass(_file: String) -> Result<Vec<usize>, String> {
    let file: File;
    match File::open(_file) {
        Ok(opened_file) => file = opened_file,
        Err(_) => return Err(String::from("Error reading file"))
    }
    let mut f_reader = BufReader::new(file);

    let buffer_file: File;
    match File::create(_TEMP_FILE1) {
        Ok(file) => buffer_file = file,
        Err(_) => return Err(String::from("Error creating buffer file"))
    }
    let mut buffer_f_writer = BufWriter::new(buffer_file);

    let mut buffer = [0; PAGE_SIZE];
    let mut current_buf_size = 0;
    let mut total_read = 0;
    let mut ret = vec![0];

    while let Ok(bytes_read) = f_reader.read(&mut buffer[current_buf_size..PAGE_SIZE]) {
        current_buf_size += bytes_read;
        total_read += bytes_read;

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
                Ok(_) => ret.push(total_read),
                //TODO write is messed up
                Err(_) => break
            }
        }

        // bytes_read = 0 when at EOF, so break
        if bytes_read == 0 { break; }
        // reset buffer for a new page when we've read a full page
        if current_buf_size == PAGE_SIZE { current_buf_size = 0; }
    };

    Ok(ret)
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
    use std::io::Write;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::fs::File;
    use sort;

    #[test]
    fn test_conversions() {
        let mut test_buffer:[u8; 512] = [0; 512];
        for x in test_buffer.iter_mut() { *x = rand::random::<u8>(); }

        let clone_buffer:[u8; 512] = test_buffer.clone();

        let ints_array = sort::bytes_to_ints(&test_buffer, 512);
        sort::ints_to_bytes(&ints_array, &mut test_buffer);

        for i in 0..512 { assert_eq!(clone_buffer[i], test_buffer[i]); }
    }

    #[test]
    fn test_first_pass() {
        {
            let mut ints_array = Vec::new();
            for i in 0..512 { ints_array.push(512 - i); }

            let mut chars_array:[u8; 2048]= [0; 2048];
            sort::ints_to_bytes(&ints_array, &mut chars_array);

            let create_and_write_result = File::create("test_data").and_then(|mut data_file| data_file.write_all(&chars_array));
            assert!(create_and_write_result.is_ok(), "Can't create test data file");
        }

        // open the file so f_reader is not uninitialized
        let data_file: File;
        match File::open("test_data") {
            Ok(opened_file) => data_file = opened_file,
            Err(_) => { assert!(false, "Can't open test data file"); return; }
        }

        let mut f_reader = BufReader::new(data_file);
        let mut chars_array:[u8; 2048] = [0; 2048];

        let test_result = sort::first_pass(String::from("test_data"))
            .and_then(|_| {
                match File::open(".temp_sort1") {
                    Ok(file) => {
                        f_reader = BufReader::new(file);
                        Ok(true)
                    }
                    Err(_) => Err(String::from("Failed to open output file"))
                }
            })
            .and_then(|_| {
                let read_result = f_reader.read_exact(&mut chars_array);
                if read_result.is_ok() { Ok(true) }
                else { Err(String::from("Failed to read output of 1st pass")) }
            })
            .and_then(|_| {
                let ints_array = sort::bytes_to_ints(&chars_array, 2048);
                let mut result = Ok(true);
                for i in 1..512 {
                    println!("{}",ints_array[i]);
                    if ints_array[i] < ints_array[i-1] { result = Err(String::from("Array not sorted")); }
                }
                result
            });

        match test_result {
            Ok(_) => assert!(true),
            Err(error) => assert!(false, error)
        }
    }
}
