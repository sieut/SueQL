## SueQL
After taking a database class, I want to try implement a database in Rust.
Also thanks [Nick](https://github.com/schainic) for the name idea.

## TODO
 - [X] BufMgr, a pager to cache pages from disk
 - [X] Exec for basic queries (create, insert, select)
 - [ ] WAL
    - [ ] LogMgr
        - [X] Write entries to disk
        - [ ] Checkpointing
            - [ ] Only if there are new log entries
        - [ ] Recovery
    - [ ] WAL on writes
    - [ ] Tests
 - [ ] Advanced exec nodes
 - [ ] Optimizer
 - [ ] Misc
    - [X] Name data files based on DbSettings
    - [X] Debug logging
