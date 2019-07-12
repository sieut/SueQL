pub mod data_store;
pub mod exec_node;
mod planner;
pub mod projection;

pub use self::data_store::DataStore;
pub use self::exec_node::ExecNode;
pub use self::projection::Projection;

use db_state::DbState;
use nom_sql;
use nom_sql::SqlQuery;
use rel::Rel;
use tuple;
use utils;

pub fn exec(
    query: SqlQuery,
    db_state: &mut DbState,
) -> Result<(), std::io::Error> {
    match query {
        SqlQuery::CreateTable(stmt) => create_table(stmt, db_state),
        SqlQuery::Insert(stmt) => insert(stmt, db_state),
        SqlQuery::Select(stmt) => match planner::plan_select(stmt, db_state)? {
            Some(node) => node.exec(db_state),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}

fn create_table(
    stmt: nom_sql::CreateTableStatement,
    db_state: &mut DbState,
) -> Result<(), std::io::Error> {
    use data_type::DataType;
    use tuple::tuple_desc::TupleDesc;

    let attr_types: Vec<DataType> = stmt
        .fields
        .iter()
        .map(|ref field| {
            DataType::from_nom_type(field.sql_type.clone()).unwrap()
        })
        .collect();
    let attr_names: Vec<String> = stmt
        .fields
        .iter()
        .map(|ref field| field.column.name.clone())
        .collect();
    let tuple_desc = TupleDesc::new(attr_types, attr_names);
    Rel::new(stmt.table.name, tuple_desc, db_state)?;
    Ok(())
}

fn insert(
    stmt: nom_sql::InsertStatement,
    db_state: &mut DbState,
) -> Result<(), std::io::Error> {
    use storage::BufType;

    let table_id = utils::get_table_id(stmt.table.name.clone(), db_state)?;
    let rel = Rel::load(table_id, BufType::Data, db_state)?;
    let tuples = rel.data_from_literal(stmt.data.clone());
    for tup in tuples.iter() {
        rel.write_new_tuple(&*tup, db_state)?;
    }
    Ok(())
}
