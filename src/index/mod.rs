pub mod hash;

pub use self::hash::HashIndex;

use db_state::DbState;
use error::Result;
use internal_types::TupleData;
use serde::{Deserialize, Serialize};
use tuple::TuplePtr;

pub trait Index {
    fn get(&self, data: &TupleData, db_state: &mut DbState) -> Result<Vec<TuplePtr>>;
    fn insert(&self, items: Vec<(&TupleData, TuplePtr)>, db_state: &mut DbState) -> Result<()>;
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum IndexType {
    Hash,
}
