use db_state::DbState;
use exec::ExecNode;
use rel::Rel;
use std::sync::Arc;

#[derive(Clone)]
pub enum DataStore {
    Literal,
    Rel(Rel),
    Out,
}

impl ExecNode for DataStore {
    fn exec(&self, _db_state: &mut DbState) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<ExecNode>> {
        vec![]
    }

    fn output(&self) -> DataStore {
        self.clone()
    }
}
