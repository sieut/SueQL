extern crate num;

enum_from_primitive!{
    #[derive(Debug, Copy, Clone)]
    pub enum OpType {
        InsertTuple,
    }
}
