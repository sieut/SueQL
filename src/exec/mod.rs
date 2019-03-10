use common;
use nom_sql;
use nom_sql::SqlQuery;
use rel::Rel;
use storage::buf_mgr::BufMgr;
use tuple;

pub fn exec(query: SqlQuery, buf_mgr: &mut BufMgr) -> Result<(), std::io::Error> {
    match query {
        SqlQuery::CreateTable(stmt) => { create_table(stmt, buf_mgr) },
        SqlQuery::Insert(stmt) => { insert(stmt, buf_mgr) },
        SqlQuery::Select(stmt) => { select(stmt, buf_mgr) },
        _ => {
            Ok(())
        }
    }
}

fn create_table(stmt: nom_sql::CreateTableStatement, buf_mgr: &mut BufMgr)
        -> Result<(), std::io::Error> {
    use data_type::DataType;
    use tuple::tuple_desc::TupleDesc;

    let attr_types: Vec<DataType> = stmt.fields
        .iter()
        .map(|ref field|
             DataType::from_nom_type(field.sql_type.clone()).unwrap())
        .collect();
    let attr_names: Vec<String> = stmt.fields
        .iter()
        .map(|ref field| field.column.name.clone())
        .collect();
    let tuple_desc = TupleDesc::new(attr_types, attr_names);
    Rel::new(tuple_desc, buf_mgr, Some(stmt.table.name))?;
    Ok(())
}

fn select(stmt: nom_sql::SelectStatement, buf_mgr: &mut BufMgr)
        -> Result<(), std::io::Error> {
    // Only Select from 1 table rn
    let table_id = get_table_id(stmt.tables[0].name.clone(), buf_mgr)?;
    let rel = Rel::load(table_id, buf_mgr)?;
    let fields = build_select_fields(&stmt.fields, rel.tuple_desc());

    rel.scan(
        buf_mgr,
        |_| { true },
        |data| {
            println!(
                "{:?}",
                rel.data_to_strings(data, Some(fields.clone())).unwrap())
        })?;
    Ok(())
}

fn insert(stmt: nom_sql::InsertStatement, buf_mgr: &mut BufMgr)
        -> Result<(), std::io::Error> {
    let table_id = get_table_id(stmt.table.name.clone(), buf_mgr)?;
    let rel = Rel::load(table_id, buf_mgr)?;
    let tuples = rel.data_from_literal(stmt.data.clone());
    for tup in tuples.iter() {
        rel.write_tuple(&*tup, buf_mgr)?;
    }
    Ok(())
}

fn get_table_id(name: String, buf_mgr: &mut BufMgr)
        -> Result<common::ID, std::io::Error> {
    let rel = Rel::load(common::TABLE_REL_ID, buf_mgr)?;
    let mut id = String::from("");
    rel.scan(
        buf_mgr,
        |data| {
            let vals = rel.data_to_strings(data, None).unwrap();
            vals[0].clone() == name
        },
        |data| { id = rel.data_to_strings(data, None).unwrap()[1].clone(); })?;
    Ok(id.parse::<common::ID>().unwrap())
}

fn build_select_fields(
        fields: &Vec<nom_sql::FieldDefinitionExpression>,
        tuple_desc: tuple::tuple_desc::TupleDesc) -> Vec<usize> {
    use nom_sql::FieldDefinitionExpression;

    let mut result = vec![];
    for field in fields.iter() {
        match field {
            FieldDefinitionExpression::All => {
                result.append(
                    &mut (0..tuple_desc.num_attrs() as usize).collect());
            },
            FieldDefinitionExpression::Col(column) => {
                result.push(
                    tuple_desc.attr_index(&column.name).unwrap());
            },
            _ => {}
        }
    }

    result
}
