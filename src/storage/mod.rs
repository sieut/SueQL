pub const PAGE_SIZE:usize = 4096;

pub use self::pagereader::PageReader;
pub use self::pagewriter::PageWriter;
pub use self::storable::Storable;

pub mod bufpage;
mod pagereader;
mod pagewriter;
mod storable;
