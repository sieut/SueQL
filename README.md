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
    - [ ] WAL on writes
    - [ ] Recovery
    - [ ] Tests
 - [ ] Advanced exec nodes
 - [ ] Optimizer
 - [ ] Misc
    - [ ] Name data files based on DbSettings
