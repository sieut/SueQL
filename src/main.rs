#![allow(dead_code)]

extern crate byteorder;
extern crate evmap;
#[macro_use] extern crate enum_primitive;
extern crate linenoise;
extern crate nom_sql;

mod common;
mod data_type;
mod db_state;
mod exec;
mod storage;
mod tuple;
mod rel;
mod utils;

use db_state::{DbState, DbSettings};

fn main() {
    let db_state = DbState::start_db(DbSettings::default()).unwrap();

    let mut query = String::from("");
    loop {
        let prompt = if query.len() == 0 { "> " }
                     else { "... " };
        let input = linenoise::input(prompt);

        match input {
            Some(input) => {
                query.push_str(&input);
                if input.find(';').is_some() {
                    match nom_sql::parse_query(&query) {
                        Ok(query) => {
                            let mut state = db_state.clone();
                            std::thread::spawn(move || {
                                exec::exec(query, &mut state.buf_mgr).unwrap();
                            });
                        },
                        Err(e) => {
                            println!("{}", e);
                        }
                    }
                    query.clear();
                }
            },
            None => { break; }
        };
    }
}
