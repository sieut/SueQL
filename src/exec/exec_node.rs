use db_state::DbState;
use exec::DataStore;
use std::sync::Arc;

pub trait ExecNode {
    fn exec(&self, db_state: &mut DbState) -> Result<(), std::io::Error>;
    fn inputs(&self) -> Vec<Arc<ExecNode>>;
    fn output(&self) -> DataStore;
}
