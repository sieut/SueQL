pub const PAGE_SIZE:usize = 4096;

pub use self::storable::Storable;

pub mod bufpage;
mod storable;
