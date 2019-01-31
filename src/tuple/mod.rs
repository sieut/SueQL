pub mod tuple_desc;
pub mod tuple_ptr;

use self::tuple_desc::TupleDesc;
use self::tuple_ptr::TuplePtr;

pub type TupleData = Vec<u8>;
