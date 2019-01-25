pub const PAGE_SIZE:u64 = 4096;

pub mod bufkey;
pub mod bufmgr;
pub mod bufpage;

#[cfg(test)]
mod tests;
