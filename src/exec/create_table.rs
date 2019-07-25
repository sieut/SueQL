use data_type::DataType;
use db_state::DbState;
use error::Result;
use exec::{DataStore, ExecNode};
use nom_sql::CreateTableStatement;
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
        Rel::new(self.stmt.table.name.clone(), tuple_desc, db_state)?;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<ExecNode>> {
        vec![]
    }

    fn output(&self) -> DataStore {
        // Not sure about this
        DataStore::Out
    }
}
