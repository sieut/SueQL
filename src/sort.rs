use types;
use types::Type;
use storage::{PAGE_SIZE, BufPage, PageReader, PageWriter};

use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use std::fs::{File, remove_file};
use std::mem::transmute;

const FILE_PREFIX:&str = ".temp_sort_";

struct Run {
    offset: usize,
    size: usize
}

// External sort
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

/// Replacement sort basically
fn first_pass(_file: String) -> Result<Vec<usize>, String> {
    let mut f_reader = PageReader::new(_file, 0).unwrap();
    let mut buffer_writer = PageWriter::new(String::from(FILE_PREFIX.to_owned() + "1"), 0, true).unwrap();

    let mut for_next_run: Vec<BufPage::<types::Integer>> = vec![BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0)];
    let mut output_buf = BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0);
    let mut heap: BinaryHeap<types::Integer> = BinaryHeap::<types::Integer>::new();

    let mut max_in_run: types::Integer = types::Integer::new(<i32>::min_value());
    let mut run_len: usize = 0;

    let mut total_read = 0;
    let mut ret = vec![0];

    loop {
        let mut input_buf: BufPage<types::Integer> = f_reader.consume_page::<types::Integer>();
        total_read += input_buf.len();

        if input_buf.len() == 0 { break; }

        // Loop through new data and
        //  - if value >= max_in_run: add it to the heap, later to the run
        //  - else: keep it for the next run
        for i in input_buf.iter() {
            if output_buf.len() == 0 || i >= max_in_run {
                heap.push(i);
                run_len += types::Integer::SIZE;
            }
            else {
                if for_next_run.last().unwrap().is_full() {
                    for_next_run.push(BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0));
                }

                for_next_run.last_mut().unwrap().push(&i);
            }
        }

        // If heap is empty, the current run has ended
        if heap.is_empty() {
            ret.push(run_len);

            for buf in for_next_run.iter() {
                for val in buf.iter() {
                    heap.push(val);
                    run_len += types::Integer::SIZE;
                }
            }

            for_next_run.clear();
        }

        // Add value in heap to output_buf
        while let Some(val) = heap.pop() {
            // Write to disk if buffer is full
            if output_buf.is_full() {
                buffer_writer.store(&output_buf);
                output_buf.clear();
            }

            output_buf.push(&val);
            max_in_run = val;
        }
    }

    if output_buf.len() > 0 {
        ret.push(run_len);
        buffer_writer.store(&output_buf);
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
}
