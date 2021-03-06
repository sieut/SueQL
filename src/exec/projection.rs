use db_state::DbState;
use error::Result;
use exec::{DataStore, ExecNode};
use std::sync::Arc;

pub struct Projection {
    src: Arc<dyn ExecNode>,
    dest: DataStore,
    indices: Vec<usize>,
}

impl Projection {
    pub fn new(
        src: Arc<dyn ExecNode>,
        dest: DataStore,
        indices: Vec<usize>,
    ) -> Projection {
        Projection { src, dest, indices }
    }
}

impl ExecNode for Projection {
    fn exec(&self, db_state: &mut DbState) -> Result<()> {
        self.src.exec(db_state)?;

        match (self.src.output(), self.output()) {
            (DataStore::Rel(input), DataStore::Rel(output)) => {
                let buf = db_state.buf_mgr.new_mem_buf()?;
                let mut buf_guard = buf.write().unwrap();

                input.scan(
                    db_state,
                    |_| Ok(true),
                    |data, db_state| {
                        let cols = input.tuple_desc().cols(data)?;
                        let cols: Vec<Vec<u8>> = self
                            .indices
                            .iter()
                            .map(|i| cols[*i].to_vec())
                            .collect();
                        let projected = cols.concat();

                        if buf_guard.available_data_space() < projected.len() {
                            let mut iter = buf_guard
                                .iter()
                                .map(|slice| slice.to_vec())
                                .chain(vec![projected].into_iter());
                            output.write_tuples(&mut iter, db_state)?;
                            buf_guard.clear();
                        }
                        Ok(())
                    },
                )?;

                Ok(())
            }
            (DataStore::Rel(input), DataStore::Out) => {
                input.scan(
                    db_state,
                    |_| Ok(true),
                    |data, _db_state| {
                        let outputs = input.data_to_strings(
                            data,
                            Some(self.indices.clone()),
                        )?;
                        println!("{:?}", outputs);
                        Ok(())
                    },
                )?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn inputs(&self) -> Vec<Arc<dyn ExecNode>> {
        vec![self.src.clone()]
    }

    fn output(&self) -> DataStore {
        self.dest.clone()
    }
}
