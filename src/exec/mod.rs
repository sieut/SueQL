pub mod create_table;
pub mod data_store;
pub mod exec_node;
pub mod insert;
mod planner;
pub mod projection;

pub use self::create_table::CreateTable;
pub use self::data_store::DataStore;
pub use self::exec_node::ExecNode;
pub use self::insert::Insert;
pub use self::projection::Projection;

use db_state::DbState;
use nom_sql;
use nom_sql::SqlQuery;
use rel::Rel;
use utils;

pub fn exec(
    query: SqlQuery,
    db_state: &mut DbState,
) -> Result<(), std::io::Error> {
    match query {
        SqlQuery::CreateTable(stmt) => match planner::plan_create(stmt)? {
            Some(node) => node.exec(db_state),
            None => Ok(()),
        },
        SqlQuery::Insert(stmt) => match planner::plan_insert(stmt, db_state)? {
            Some(node) => node.exec(db_state),
            None => Ok(()),
        },
        SqlQuery::Select(stmt) => match planner::plan_select(stmt, db_state)? {
            Some(node) => node.exec(db_state),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}
