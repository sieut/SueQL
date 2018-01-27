use types;
use types::Type;
use storage::{pagereader,pagewriter};

use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::File;
use std::fs::remove_file;
use std::mem::transmute;

const PAGE_SIZE:usize = 4096;
const SIZE_OF_I32:usize = 4;
const _TEMP_FILE1:&str = ".temp_sort_1";
const FILE_PREFIX:&str = ".temp_sort_";

struct Run {
    offset: usize,
    size: usize
}

// External sort
// v1: first pass quick sort
pub fn sort(_file: String) -> Result<bool, String> {
    match first_pass(_file) {
        Ok(mut runs) => {
            // Subsequent passes
            let mut pass:u32 = 2;
            loop {
                let merge_result = merge(&runs, pass)
                    .and_then(|new_runs| {
                        runs = new_runs;
                        Ok(true)
                    });
                // First run should always be at beginning of file
                assert_eq!(runs[0], 0);

                if !merge_result.is_ok() { return Err(merge_result.unwrap_err()); }
                if runs.len() == 1 { break; }
                pass += 1;
            }
        }
        Err(error) => return Err(error)
    }

    Ok(true)
}

fn first_pass(_file: String) -> Result<Vec<usize>, String> {
    let mut f_reader = pagereader::PageReader::new(_file, 0).unwrap();
    let mut buffer_writer = pagewriter::PageWriter::new(String::from(_TEMP_FILE1), 0, true).unwrap();

    let mut total_read = 0;
    let mut ret = vec![0];

    loop {
        let mut buffer:Vec<types::Integer> = vec![];

        {
            let byte_buffer = f_reader.consume_page();
            total_read += byte_buffer.len();

            // TODO file is messed up?
            if byte_buffer.len() % types::Integer::get_size() != 0 { assert!(false, "Wrong byte_buffer size"); }
            if byte_buffer.len() == 0 { break; }

            for i in 0..byte_buffer.len()/types::Integer::get_size() {
                let new_val = types::Integer::from_bytes(&byte_buffer[i * types::Integer::get_size()..(i+1) * types::Integer::get_size()]).unwrap();
                buffer.push(new_val);
            }
        }

        buffer.sort();

        {
            let mut output_buffer:Vec<u8> = vec![];
            for int in buffer.iter() {
                output_buffer.append(&mut int.to_bytes().unwrap());
            }

            buffer_writer.store(&output_buffer);
            ret.push(total_read);
        }
    }

    Ok(ret)
}

fn merge(runs: &Vec<usize>, pass: u32) -> Result<Vec<usize>, String> {
    let last_pass_fname:String = String::from(FILE_PREFIX) + &(pass-1).to_string();
    let cur_pass_fname:String = String::from(FILE_PREFIX) + &(pass).to_string();

    // Open last pass' file
    let lpass_file:File;
    match File::open(last_pass_fname.clone()) {
        Ok(file) => lpass_file = file,
        Err(_) => return Err(String::from("Can't open last pass' file"))
    }
    // Create new file for this pass
    let cpass_file:File;
    match File::create(cur_pass_fname) {
        Ok(file) => cpass_file = file,
        Err(_) => return Err(String::from("Can't create current pass' file"))
    }

    let mut ret = Vec::new();

    for mut i in 0..runs.len() {

        // Hack to step i by 2, step_by is not stable yet
        i += 1;
    }

    remove_file(last_pass_fname);

    Ok(ret)
}

/// Merge 2 given runs of input file 'file' and write to output file with 'writer'
/// 'run_1_size' is offset_2 - offset_1, 'run_2_size' is 0 if run_2 goes to EOF
fn merge_runs(file: &File, writer: &mut BufWriter<File>, r1: &Run, r2: &Run) {
    // Create readers and move them to runs' offsets
    let mut reader_1 = BufReader::new(file);
    reader_1.seek(SeekFrom::Start(r1.offset as u64));
    let mut reader_2 = BufReader::new(file);
    reader_2.seek(SeekFrom::Start(r2.offset as u64));

    // Buffers and total bytes read for each run
    let mut r1_buffer:[u8; PAGE_SIZE] = [0; PAGE_SIZE];
    let mut r1_bytes_read = 0;
    let mut r2_buffer:[u8; PAGE_SIZE] = [0; PAGE_SIZE];
    let mut r2_bytes_read = 0;

    // Merged buffer
    let mut m_buffer:[u8; PAGE_SIZE] = [0; PAGE_SIZE];
    let mut m_size = 0;

    loop {
        // if r1_bytes_read >= r1.size && r2_bytes_read >= r2.size { break; }

        // let mut r1_cur_size = 0;
        // let mut r2_cur_size = 0;
        // if r1_bytes_read < r1.size { r1_cur_size = read_page(&mut reader_1, &mut r1_buffer); }
        // if r2_bytes_read < r2.size { r2_cur_size = read_page(&mut reader_2, &mut r2_buffer); }

        // r1_bytes_read += r1_cur_size;
        // r2_bytes_read += r2_cur_size;

        // let r1_vec = bytes_to_ints(&r1_buffer, r1_cur_size);
        // let r2_vec = bytes_to_ints(&r2_buffer, r2_cur_size);
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;

    use std::io::BufReader;
    use std::io::Read;
    use std::io::Write;
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
