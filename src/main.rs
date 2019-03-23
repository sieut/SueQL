#![allow(dead_code)]

extern crate byteorder;
extern crate evmap;
#[macro_use]
extern crate enum_primitive;
extern crate linenoise;
extern crate nom_sql;

mod data_type;
mod db_state;
mod exec;
mod internal_types;
mod log;
mod meta;
mod rel;
mod storage;
mod tuple;
mod utils;

use db_state::{DbSettings, DbState};

fn main() {
    let mut db_state = DbState::start_db(DbSettings::default()).unwrap();

    let mut query = String::from("");
    loop {
        let prompt = if query.len() == 0 { "> " } else { "... " };
        let input = linenoise::input(prompt);

        match input {
            Some(input) => {
                query.push_str(&input);
                if input.find(';').is_some() {
                    match nom_sql::parse_query(&query) {
                        Ok(query) => {
                            let mut state = db_state.clone();
                            std::thread::spawn(move || {
                                exec::exec(query, &mut state).unwrap();
                            });
                        }
                        Err(e) => {
                            println!("{}", e);
                        }
                    }
                    query.clear();
                }
            }
            None => {
                db_state.shutdown().unwrap();
                break;
            }
        };
    }
}
