pub const PAGE_SIZE: usize = 4096;

pub mod buf_key;
pub mod buf_mgr;
pub mod buf_page;
pub mod storable;

pub use self::buf_key::BufKey;
pub use self::buf_mgr::BufMgr;
pub use self::buf_page::BufPage;
pub use self::storable::Storable;

#[cfg(test)]
mod tests;
