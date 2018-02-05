use types;
use storage::{FixedStorable, PAGE_SIZE, bufpage, PageReader, PageWriter};

use std::collections::BinaryHeap;
use std::fs::remove_file;
use std::iter::Iterator;

const FILE_PREFIX:&str = ".temp_sort_";

// External sort
pub fn sort(_file: String) -> Result<bool, String> {
    match first_pass(_file) {
        Ok(mut runs) => {
            // Subsequent passes
            let mut pass:u32 = 2;
            while runs.len() > 1 {
                let merge_result = merge(&runs, pass)
                    .and_then(|new_runs| {
                        runs = new_runs;
                        Ok(true)
                    });
                // First run should always be at beginning of file
                assert_eq!(runs[0].offset, 0);

                if !merge_result.is_ok() { return Err(merge_result.unwrap_err()); }
                pass += 1;
            }
        }
        Err(error) => return Err(error)
    }

    Ok(true)
}

/// Replacement sort basically
fn first_pass(_file: String) -> Result<Vec<Run>, String> {
    let mut f_reader = PageReader::new(_file, 0).unwrap();
    let mut buffer_writer = PageWriter::new(String::from(FILE_PREFIX.to_owned() + "1"), 0, true).unwrap();

    let mut for_next_run: Vec<bufpage::BufPage<types::Integer>> = vec![bufpage::BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0)];
    let mut output_buf = bufpage::BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0);
    let mut heap: BinaryHeap<types::Integer> = BinaryHeap::<types::Integer>::new();

    let mut max_in_run: types::Integer = types::Integer::new(<i32>::min_value());
    let mut run_len: usize = 0;

    let mut total_read = 0;
    let mut ret = vec![Run { offset: 0, len: 0 }];

    loop {
        let input_buf: bufpage::BufPage<types::Integer> = f_reader.consume_page::<types::Integer>();
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
                    for_next_run.push(bufpage::BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0));
                }

                for_next_run.last_mut().unwrap().push(&i);
            }
        }

        // If heap is empty, the current run has ended
        if heap.is_empty() {
            ret.last_mut().unwrap().len = run_len;
            ret.push(Run { offset: total_read, len: 0 });

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
            store_if_full(&mut output_buf, &mut buffer_writer);
            output_buf.push(&val);
            max_in_run = val;
        }
    }

    if output_buf.len() > 0 {
        ret.last_mut().unwrap().len = run_len;
        buffer_writer.store(&output_buf);
    }

    Ok(ret)
}

fn merge(runs: &Vec<Run>, pass: u32) -> Result<Vec<Run>, String> {
    let last_pass_fname:String = String::from(FILE_PREFIX) + &(pass-1).to_string();
    let cur_pass_fname:String = String::from(FILE_PREFIX) + &(pass).to_string();

    let mut buffer_writer = PageWriter::new(cur_pass_fname, 0, true).unwrap();
    let mut output_buf = bufpage::BufPage::<types::Integer>::new(&[0; PAGE_SIZE], 0);

    let mut ret = vec![];

    for mut i in 0..runs.len() {
        // If there's an odd number of runs, just write that run to a new file
        if i + 1 >= runs.len() {
            for val in runs[i].iter::<types::Integer>(last_pass_fname.clone()) {
                store_if_full(&mut output_buf, &mut buffer_writer);
                output_buf.push(&val);
            }

            ret.push(Run { ..runs[i] });
        }
        else {
            let mut iter1 = runs[i].iter::<types::Integer>(last_pass_fname.clone());
            let mut iter2 = runs[i+1].iter::<types::Integer>(last_pass_fname.clone());

            let mut val1 = iter1.next();
            let mut val2 = iter2.next();

            loop {
                store_if_full(&mut output_buf, &mut buffer_writer);

                if val1.is_none() && val2.is_none() {
                    break;
                }
                else if val1.is_none() {
                    output_buf.push(&val2.unwrap());
                    val2 = iter2.next();
                }
                else if val2.is_none() {
                    output_buf.push(&val1.unwrap());
                    val1 = iter1.next();
                }
                else {
                    if val1.unwrap() < val2.unwrap() {
                        output_buf.push(&val1.unwrap());
                        store_if_full(&mut output_buf, &mut buffer_writer);
                        output_buf.push(&val2.unwrap());
                    }
                    else {
                        output_buf.push(&val2.unwrap());
                        store_if_full(&mut output_buf, &mut buffer_writer);
                        output_buf.push(&val1.unwrap());
                    }
                }
            }

            ret.push(Run { offset: runs[i].offset, len: runs[i].len + runs[i+1].len });
        }

        // Hack to step i by 2, step_by is not stable yet
        i += 1;
    }

    if output_buf.len() > 0 {
        buffer_writer.store(&output_buf);
        output_buf.clear();
    }

    remove_file(last_pass_fname);
    Ok(ret)
}

fn store_if_full<T>(buf_page: &mut bufpage::BufPage<T>, writer: &mut PageWriter)
where T: FixedStorable {
    if buf_page.is_full() {
        writer.store(&buf_page);
        buf_page.clear();
    }
}

struct Run {
    offset: usize,
    len: usize
}

impl Run {
    fn iter<T>(&self, file_name: String) -> RunIterator<T>
    where T: FixedStorable {
        let mut reader: PageReader = PageReader::new(file_name, self.offset / PAGE_SIZE).unwrap();
        let first_page = reader.consume_page::<T>();

        RunIterator {
            len: self.len,
            consumed: 0,
            reader: reader,
            buf_page: first_page,
            buf_page_index: (self.offset % PAGE_SIZE) / T::SIZE
        }
    }
}

struct RunIterator<T>
where T: FixedStorable {
    len: usize,
    consumed: usize,
    reader: PageReader,
    buf_page: bufpage::BufPage<T>,
    buf_page_index: usize,
}

impl<T> Iterator for RunIterator<T>
where T: FixedStorable {
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.consumed == self.len {
            None
        }
        else {
            // If we have gone through the current page, read a new one in
            if self.buf_page_index == self.buf_page.len() / T::SIZE {
                self.buf_page = self.reader.consume_page::<T>();
                self.buf_page_index = 0;
            }

            let item:Self::Item = T::from_bytes(&self.buf_page.data()[self.buf_page_index * T::SIZE..(self.buf_page_index + 1) * T::SIZE]).unwrap();
            self.buf_page_index += 1;
            self.consumed += T::SIZE;

            Some(item)
        }
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
