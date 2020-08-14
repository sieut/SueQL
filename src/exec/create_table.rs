use data_type::DataType;
use db_state::DbState;
use error::{Error, Result};
use exec::{DataStore, ExecNode};
use index::IndexType;
use nom_sql::{Column, CreateTableStatement, TableKey};
use rel::Rel;
use std::sync::Arc;
use tuple::TupleDesc;

pub struct CreateTable {
    stmt: CreateTableStatement,
}

impl CreateTable {
    pub fn new(stmt: CreateTableStatement) -> CreateTable {
        CreateTable { stmt }
    }

    fn create_hash_index(
        &self,
        rel: &mut Rel,
        cols: &Vec<Column>,
        db_state: &mut DbState,
    ) -> Result<()> {
        let desc = rel.tuple_desc();
        let cols = cols
            .iter()
            .map(|col| desc.attr_index(&col.name))
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::Internal("Invalid column in key.".to_string()))?;
        rel.new_index(cols, IndexType::Hash, db_state)?;
        Ok(())
    }
}

impl ExecNode for CreateTable {
    fn exec(&self, db_state: &mut DbState) -> Result<()> {
        let attr_types: Vec<DataType> = self
            .stmt
            .fields
            .iter()
            .map(|ref field| {
                DataType::from_nom_type(field.sql_type.clone()).unwrap()
            })
            .collect();
        let attr_names: Vec<String> = self
            .stmt
            .fields
            .iter()
            .map(|ref field| field.column.name.clone())
            .collect();
        let tuple_desc = TupleDesc::new(attr_types, attr_names);
        let mut rel = Rel::new(self.stmt.table.name.clone(), tuple_desc, db_state)?;
        if let Some(ref keys) = self.stmt.keys {
            for key in keys.iter() {
                match key {
                    TableKey::PrimaryKey(cols) => {
                        self.create_hash_index(&mut rel, cols, db_state)?
                    }
                    _ => {
                        todo!("Only primary keys are supported.");
                    }
                }
            }
        }
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn ExecNode>> {
        vec![]
    }

    fn output(&self) -> DataStore {
        // Not sure about this
        DataStore::Out
    }
}
