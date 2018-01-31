pub use self::integer::Integer;
pub use self::char::Char;

mod integer;
mod char;

#[derive(Copy, Clone)]
pub enum ColumnType {
    Char,
    Int,
}
