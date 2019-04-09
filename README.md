## SueQL
After taking a database class, I want to try implement a database in Rust.
Also thanks [Nick](https://github.com/schainic) for the name idea.

## TODO
 - [X] BufMgr, a pager to cache pages from disk
 - [X] Exec for basic queries (create, insert, select)
 - [ ] WAL
    - [ ] LogMgr
        - [X] Write entries to disk
        - [X] Checkpointing
            - [X] Only if there are new log entries
        - [ ] Recovery
    - [X] WAL on writes
    - [ ] Tests
 - [ ] Advanced exec nodes
 - [ ] Optimizer
 - [ ] Misc
    - [X] Name data files based on DbSettings
    - [X] Debug logging
    - [ ] Make a Result type that converts various errors (eg. IO Error) and prints helpful debug info
