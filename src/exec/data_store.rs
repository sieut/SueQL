use db_state::DbState;
use exec::ExecNode;
use internal_types::TupleData;
use rel::Rel;
use std::sync::Arc;
use tuple::TupleDesc;

#[derive(Clone)]
pub enum DataStore {
    Data {
        tuples: Vec<TupleData>,
        desc: TupleDesc,
    },
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
