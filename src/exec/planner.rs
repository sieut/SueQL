use db_state::DbState;
use exec::{DataStore, ExecNode};
use nom_sql::{FieldDefinitionExpression, SelectStatement};
use rel::Rel;
use std::sync::Arc;
use storage::BufType;
use tuple::TupleDesc;
use utils;

pub fn plan_select(
    stmt: SelectStatement,
    db_state: &mut DbState,
) -> Result<Option<Box<ExecNode>>, std::io::Error> {
    use exec::Projection;

    let rel_id = match stmt.tables.len() {
        1 => utils::get_table_id(stmt.tables[0].name.clone(), db_state)?,
        // Join is not supported yet
        _ => {
            return Ok(None);
        }
    };

    let rel = Rel::load(rel_id, BufType::Data, db_state)?;
    let fields = build_select_fields(&stmt.fields, rel.tuple_desc());

    Ok(Some(Box::new(Projection::new(
        Arc::new(DataStore::Rel(rel)),
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
