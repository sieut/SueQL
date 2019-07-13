use db_state::DbState;
use exec::{DataStore, ExecNode};
use std::sync::Arc;

pub struct Insert {
    data: Arc<ExecNode>,
    rel: DataStore,
}

impl Insert {
    pub fn new(data: Arc<ExecNode>, rel: DataStore) -> Insert {
        Insert { data, rel }
    }
}

impl ExecNode for Insert {
    fn exec(&self, db_state: &mut DbState) -> Result<(), std::io::Error> {
        match (self.data.output(), self.output()) {
            (DataStore::Data { tuples, desc }, DataStore::Rel(rel)) => {
                assert_eq!(desc.attr_types(), rel.tuple_desc().attr_types());
                rel.write_tuples(tuples, db_state)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn inputs(&self) -> Vec<Arc<ExecNode>> {
        vec![self.data.clone()]
    }

    fn output(&self) -> DataStore {
        self.rel.clone()
    }
}
