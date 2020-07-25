pub mod hash;

pub use self::hash::HashIndex;

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum IndexType {
    Hash,
}
