pub mod hash;

pub use self::hash::HashIndex;

use db_state::DbState;
use error::Result;
use internal_types::TupleData;
use serde::{Deserialize, Serialize};
use tuple::{TupleDesc, TuplePtr};

pub trait Index {
    fn get(
        &self,
        data: &TupleData,
        db_state: &mut DbState,
    ) -> Result<Vec<TuplePtr>>;

    fn insert(
        &self,
        items: &mut dyn Iterator<Item=(TupleData, TuplePtr)>,
        db_state: &mut DbState,
    ) -> Result<()>;

    fn key_desc(&self) -> TupleDesc;
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum IndexType {
    Hash,
}
