pub const PAGE_SIZE: usize = 4096;

pub mod buf_key;
pub mod buf_mgr;
pub mod buf_page;

#[cfg(test)]
mod tests;
