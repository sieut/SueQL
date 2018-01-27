pub const PAGE_SIZE:usize = 4096;

pub use self::bufpage::BufPage;

mod bufpage;
pub mod pagereader;
pub mod pagewriter;
