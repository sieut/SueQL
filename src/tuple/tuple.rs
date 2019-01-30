use tuple::tuple_desc::TupleDesc;
use tuple::tuple_ptr::TuplePtr;

pub struct Tuple {
    tuple_ptr: TuplePtr,
    tuple_desc: TupleDesc,
}

impl Tuple {
    pub fn new(tuple_ptr: TuplePtr, tuple_desc: TupleDesc) -> Tuple {
        Tuple { tuple_ptr, tuple_desc }
    }
}
