use db_state::DbState;
use error::Result;
use exec::DataStore;
use std::sync::Arc;

pub trait ExecNode {
    fn exec(&self, db_state: &mut DbState) -> Result<()>;
    fn inputs(&self) -> Vec<Arc<ExecNode>>;
    fn output(&self) -> DataStore;
}
