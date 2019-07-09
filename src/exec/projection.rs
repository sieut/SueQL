use db_state::DbState;
use exec::{DataStore, ExecNode};
use std::sync::Arc;

pub struct Projection {
    src: Arc<ExecNode>,
    dest: DataStore,
    indices: Vec<usize>,
}

impl ExecNode for Projection {
    fn exec(&self, db_state: &mut DbState) -> Result<(), std::io::Error> {
        self.src.exec(db_state)?;

        match (self.src.output(), self.output()) {
            (DataStore::Rel(input), DataStore::Rel(output)) => {
                rel_write_lock!(output, db_state.buf_mgr);

                let buf = db_state.buf_mgr.new_mem_buf()?;
                let mut buf_guard = buf.write().unwrap();

                input.scan(
                    db_state,
                    |_| true,
                    |data, db_state| {
                        let cols = input.tuple_desc().cols(data);
                        let cols: Vec<Vec<u8>> = self
                            .indices
                            .iter()
                            .map(|i| cols[*i].clone())
                            .collect();
                        let projected = cols.concat();

                        // TODO handle the unwraps
                        if buf_guard.available_data_space() < projected.len() {
                            output.append_page(&buf_guard, db_state).unwrap();
                            buf_guard.clear();
                        }
                        buf_guard
                            .write_tuple_data(&projected, None, None)
                            .unwrap();
                    },
                )?;

                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn inputs(&self) -> Vec<Arc<ExecNode>> {
        vec![self.src.clone()]
    }

    fn output(&self) -> DataStore {
        self.dest.clone()
    }
}
