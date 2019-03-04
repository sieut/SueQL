#![allow(dead_code)]

extern crate byteorder;
extern crate crossbeam;
extern crate evmap;
#[macro_use] extern crate enum_primitive;

mod common;
mod data_type;
mod db_state;
mod storage;
mod tuple;
mod rel;
mod utils;

use crossbeam::thread;
use db_state::{DbState, DbSettings};

fn main() {
    let mut db_state = DbState::start_db(DbSettings::default()).unwrap();

    thread::scope(|s| {
        let mut state1 = db_state.clone();
        s.spawn(move |_| {
            state1.buf_mgr.get_buf(&storage::buf_key::BufKey::new(0, 0));
        });

        let mut state2 = db_state.clone();
        s.spawn(move |_| {
            state2.buf_mgr.get_buf(&storage::buf_key::BufKey::new(0, 0));
        });
    }).unwrap();
}
