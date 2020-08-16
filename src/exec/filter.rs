use db_state::DbState;
use error::{Error, Result};
use exec::{DataStore, ExecNode, Expr};
use index::{HashIndex, Index, IndexType};
use internal_types::TupleData;
use nom_sql::{ConditionExpression, Operator};
use rel::rel::{Rel, IndexInfo};
use std::collections::HashMap;
use std::sync::Arc;
use storage::BufKey;
use tuple::TuplePtr;

pub struct Filter {
    data: Rel,
    dest: DataStore,
    clause: ConditionExpression,
}

impl Filter {
    pub fn new(
        data: Rel,
        dest: DataStore,
        clause: ConditionExpression,
    ) -> Filter {
        Filter { data, dest, clause }
    }

    fn maybe_index(&self) -> Result<Option<IndexInfo>> {
        let indices = self.data.indices();
        let indices = indices.iter().filter(|index| {
            if index.key.len() > 1 {
                todo!("Multi-key indices are not yet supported");
            }
            let key = index.key[0].clone();
            match index.index_type {
                IndexType::Hash => self.maybe_hash_index(&self.data, key)
            }
        }).collect::<Vec<_>>();

        match indices.len() {
            0 => Ok(None),
            1 => {
                let index = indices[0].clone();
                Ok(Some(index))
            }
            _ => todo!("Picking between indices for filter is not yet supported")
        }
    }

    fn maybe_hash_index(&self, rel: &Rel, key: usize) -> bool {
        if let ConditionExpression::ComparisonOp(ref tree) = self.clause {
            if let Operator::Equal = tree.operator {
                match (Expr::is_only_col((*tree.left).clone(), rel),
                    Expr::is_no_col((*tree.right).clone())) {
                    (Some(col_index), true) => return key == col_index,
                    (Some(_), false) => {
                        todo!("Non-constant right expr is \
                            not yet supported for indexing")
                    }
                    _ => return false,
                }
            }
        }
        return false;
    }

    fn hash_index_data(&self) -> Result<TupleData> {
        let err = Err(Error::Internal(
                "Invalid expression for hash indexing".to_string()));
        if let ConditionExpression::ComparisonOp(ref tree) = self.clause {
            if let Operator::Equal = tree.operator {
                if Expr::is_no_col((*tree.right).clone()) {
                    let expr = Expr::from_nom(
                        (*tree.right).clone(), &self.data)?;
                    return (expr.function)(&vec![]);
                }
                return err;
            }
        }
        return err;
    }

    fn hash_index(
        &self,
        info: IndexInfo,
        db_state: &mut DbState,
    ) -> Result<()> {
        let index = HashIndex::load(info.file_id, db_state)?;
        let index_data = self.hash_index_data()?;
        let ptrs = self.group_ptrs(index.get(&index_data, db_state)?);
        match self.output() {
            DataStore::Rel(rel) => {
                for (buf_key, buf_ptrs) in ptrs.into_iter() {
                    let buf = db_state.buf_mgr.get_buf(&buf_key)?;
                    let guard = buf.read().unwrap();
                    let mut tuple_iter = buf_ptrs
                        .iter()
                        .map(|ptr| {
                            guard.get_tuple_data(ptr).unwrap().to_vec()
                        });
                    rel.write_tuples(&mut tuple_iter, db_state)?;
                }
            }
            DataStore::Out => {
                for (buf_key, buf_ptrs) in ptrs.into_iter() {
                    let buf = db_state.buf_mgr.get_buf(&buf_key)?;
                    let guard = buf.read().unwrap();
                    for ptr in buf_ptrs.iter() {
                        let data = guard.get_tuple_data(ptr)?;
                        println!(
                            "{:?}", self.data.data_to_strings(data, None));
                    }
                }
            }
            _ => panic!("Invalid output destination for filter"),
        };
        Ok(())
    }

    fn group_ptrs(&self, ptrs: Vec<TuplePtr>) -> HashMap<BufKey, Vec<TuplePtr>> {
        let mut map = HashMap::new();
        for ptr in ptrs.into_iter() {
            if let None = map.get(&ptr.buf_key) {
                map.insert(ptr.buf_key.clone(), vec![]);
            }
            map.get_mut(&ptr.buf_key.clone()).unwrap().push(ptr);
        }
        map
    }
}

impl ExecNode for Filter {
    fn exec(&self, db_state: &mut DbState) -> Result<()> {
        if let Some(index) = self.maybe_index()? {
            match index.index_type {
                IndexType::Hash => self.hash_index(index, db_state),
            }
        } else {
            use data_type::DataType;
            let expr = Expr::from_nom(self.clause.clone(), &self.data)?;
            assert_eq!(expr.output_type, DataType::Bool);
            let then_fn: Box<dyn FnMut(&[u8], &mut DbState) -> Result<()>> =
                    match self.output() {
                DataStore::Rel(output) => {
                    Box::new(move |data, db_state| {
                        output.write_tuples(
                            &mut vec![data.to_vec()].into_iter(),
                            db_state)?;
                        Ok(())
                    })
                }
                DataStore::Out => {
                    Box::new(|data, _| {
                        println!(
                            "{:?}",
                            self.data.data_to_strings(data, None));
                        Ok(())
                    })
                }
                _ => panic!("Invalid output destination for Filter")
            };
            self.data.scan(
                db_state,
                |data| {
                    Ok(bincode::deserialize(&(expr.function)(data)?)?)
                },
                then_fn
            )
        }
    }

    fn inputs(&self) -> Vec<Arc<dyn ExecNode>> {
        vec![Arc::new(DataStore::Rel(self.data.clone()))]
    }

    fn output(&self) -> DataStore {
        self.dest.clone()
    }
}
