#![allow(dead_code)]

extern crate bincode;
extern crate evmap;
extern crate linenoise;
extern crate nom_sql;
extern crate serde;
extern crate sha2;

#[macro_use]
mod utils;
#[macro_use]
mod rel;

mod data_type;
mod db_state;
mod error;
mod exec;
mod index;
mod internal_types;
mod log;
mod meta;
mod storage;
mod tuple;

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
                            exec::exec(query, &mut db_state).unwrap();
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
