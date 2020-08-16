use db_state::DbState;
use error::Result;
use exec::{DataStore, ExecNode};
use nom_sql::{
    CreateTableStatement, FieldDefinitionExpression, InsertStatement,
    SelectStatement,
};
use rel::Rel;
use std::sync::Arc;
use storage::BufType;
use tuple::TupleDesc;
use utils;

pub fn plan_create(
    stmt: CreateTableStatement,
) -> Result<Option<Box<dyn ExecNode>>> {
    use exec::CreateTable;
    Ok(Some(Box::new(CreateTable::new(stmt))))
}

pub fn plan_insert(
    stmt: InsertStatement,
    db_state: &mut DbState,
) -> Result<Option<Box<dyn ExecNode>>> {
    use exec::Insert;

    let rel_id = utils::get_table_id(stmt.table.name.clone(), db_state)?;
    let rel = Rel::load(rel_id, BufType::Data, db_state)?;
    let tuples = rel.literal_to_data(stmt.data.clone())?;
    Ok(Some(Box::new(Insert::new(
        Arc::new(DataStore::Data {
            tuples,
            desc: rel.tuple_desc(),
        }),
        DataStore::Rel(rel),
    ))))
}

pub fn plan_select(
    stmt: SelectStatement,
    db_state: &mut DbState,
) -> Result<Option<Box<dyn ExecNode>>> {
    use super::{Filter, Projection};

    let rel_id = match stmt.tables.len() {
        1 => utils::get_table_id(stmt.tables[0].name.clone(), db_state)?,
        _ => todo!("Join is not supported yet"),
    };

    let rel = Rel::load(rel_id, BufType::Data, db_state)?;
    let fields = build_select_fields(&stmt.fields, rel.tuple_desc());
    let projection_src = match stmt.where_clause {
        Some(clause) => {
            let temp_rel = Rel::new_temp_rel(rel.tuple_desc(), db_state)?;
            Arc::new(
                Filter::new(
                    rel,
                    DataStore::Rel(temp_rel),
                    clause)) as Arc<dyn ExecNode>
        }
        None => Arc::new(DataStore::Rel(rel)),
    };
    Ok(Some(Box::new(Projection::new(
        projection_src,
        DataStore::Out,
        fields,
    ))))
}

fn build_select_fields(
    fields: &Vec<FieldDefinitionExpression>,
    tuple_desc: TupleDesc,
) -> Vec<usize> {
    fields
        .iter()
        .map(|field| match field {
            FieldDefinitionExpression::All => {
                (0..tuple_desc.num_attrs() as usize).collect()
            }
            FieldDefinitionExpression::Col(column) => {
                vec![tuple_desc.attr_index(&column.name).unwrap()]
            }
            _ => vec![],
        })
        .collect::<Vec<_>>()
        .concat()
}
