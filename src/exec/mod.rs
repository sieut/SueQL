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
        SqlQuery::Select(stmt) => select(stmt, db_state),
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

fn select(
    stmt: nom_sql::SelectStatement,
    db_state: &mut DbState,
) -> Result<(), std::io::Error> {
    // Only Select from 1 table rn
    let table_id = utils::get_table_id(stmt.tables[0].name.clone(), db_state)?;
    let rel = Rel::load(table_id, false, db_state)?;
    let fields = build_select_fields(&stmt.fields, rel.tuple_desc());

    rel.scan(
        db_state,
        |_| true,
        |data| {
            println!(
                "{:?}",
                rel.data_to_strings(data, Some(fields.clone())).unwrap()
            )
        },
    )?;
    Ok(())
}

fn insert(
    stmt: nom_sql::InsertStatement,
    db_state: &mut DbState,
) -> Result<(), std::io::Error> {
    let table_id = utils::get_table_id(stmt.table.name.clone(), db_state)?;
    let rel = Rel::load(table_id, false, db_state)?;
    let tuples = rel.data_from_literal(stmt.data.clone());
    for tup in tuples.iter() {
        rel.write_new_tuple(&*tup, db_state)?;
    }
    Ok(())
}

fn build_select_fields(
    fields: &Vec<nom_sql::FieldDefinitionExpression>,
    tuple_desc: tuple::tuple_desc::TupleDesc,
) -> Vec<usize> {
    use nom_sql::FieldDefinitionExpression;

    let mut result = vec![];
    for field in fields.iter() {
        match field {
            FieldDefinitionExpression::All => {
                result.append(
                    &mut (0..tuple_desc.num_attrs() as usize).collect(),
                );
            }
            FieldDefinitionExpression::Col(column) => {
                result.push(tuple_desc.attr_index(&column.name).unwrap());
            }
            _ => {}
        }
    }

    result
}
