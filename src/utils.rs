use db_state::DbState;
use error::{Error, Result};
use internal_types::ID;
use meta::TABLE_REL_ID;
use rel::Rel;
use std::fs::{File, metadata};
use std::io::{Write, ErrorKind};

#[macro_export]
macro_rules! dbg_log {
    ($($log_expr:expr),+) => {
        dbg!();
        println!($($log_expr),+);
    }
}

pub fn assert_data_len(data: &[u8], desired_len: usize) -> Result<()> {
    if data.len() == desired_len {
        Ok(())
    } else {
        Err(Error::CorruptedData)
    }
}

pub fn file_exists(fname: &str) -> bool {
    match metadata(fname) {
        Ok(_) => true,
        Err(err) => match err.kind() {
            ErrorKind::NotFound => false,
            _ => true,
        }
    }
}

pub fn create_file(fname: &str) -> Result<()> {
    use storage::buf_page::BufPage;

    let mut file = File::create(fname)?;
    file.write_all(&BufPage::default_buf())?;
    Ok(())
}

pub fn num_pages(fname: &str) -> Result<u64> {
    use storage::PAGE_SIZE;
    Ok(file_len(fname)? / PAGE_SIZE as u64 - 1)
}

pub fn file_len(fname: &str) -> Result<u64> {
    let file_meta = metadata(fname)?;
    if !file_meta.is_file() {
        Err(Error::from(std::io::Error::new(
            ErrorKind::InvalidInput,
            "Path is not a file",
        )))
    } else {
        Ok(file_meta.len())
    }
}

pub fn get_table_id(name: String, db_state: &mut DbState) -> Result<ID> {
    use storage::BufType;

    let rel = Rel::load(TABLE_REL_ID, BufType::Data, db_state)?;
    let mut id = String::from("");
    rel.scan(
        db_state,
        |data| {
            let vals = rel.data_to_strings(data, None).unwrap();
            Ok(vals[0].clone() == name)
        },
        |data, _db_state| {
            id = rel.data_to_strings(data, None).unwrap()[1].clone();
            Ok(())
        },
    )?;
    Ok(id.parse::<ID>().unwrap())
}
