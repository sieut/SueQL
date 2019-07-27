use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpType {
    InsertTuple,
    // UpdateTuple,
    Checkpoint,
    PendingCheckpoint,
}
