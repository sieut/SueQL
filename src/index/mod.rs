pub mod hash;

pub use self::hash::HashIndex;

#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum IndexType {
    Hash,
}
