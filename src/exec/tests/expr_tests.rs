use bincode;
use data_type::DataType;
use exec::Expr;
use nom_sql;
use tuple::TupleDesc;

#[test]
fn test_one_eq_one() {
    test_query_output(
        "select * from test where 1 = 1",
        None,
        &vec![],
        DataType::Bool,
        true_bytes(),
    );
}

#[test]
fn test_one_plus_one() {
    test_query_output(
        "select * from test where 1 + 1 = 2",
        None,
        &vec![],
        DataType::Bool,
        true_bytes(),
    );
}

#[test]
fn test_select_col() {
    let desc = TupleDesc::new(
        vec![DataType::I32, DataType::VarChar],
        vec!["test_int", "test_str"],
    );
    let test_int = bincode::serialize(&10i32).unwrap();
    let test_str = bincode::serialize(&String::from("Hello")).unwrap();
    let data = vec![test_int, test_str].concat();

    test_query_output(
        "select * from test where test_int = 5",
        Some(&desc),
        &data,
        DataType::Bool,
        false_bytes(),
    );
    test_query_output(
        "select * from test where test_int = 10",
        Some(&desc),
        &data,
        DataType::Bool,
        true_bytes(),
    );
    test_query_output(
        "select * from test where test_str = 'World'",
        Some(&desc),
        &data,
        DataType::Bool,
        false_bytes(),
    );
    test_query_output(
        "select * from test where test_str = 'Hello'",
        Some(&desc),
        &data,
        DataType::Bool,
        true_bytes(),
    );
}

fn test_query_output(
    query: &str,
    desc: Option<&TupleDesc>,
    input: &Vec<u8>,
    expect_ty: DataType,
    expect_out: Vec<u8>,
) {
    let query = nom_sql::parse_query(query).unwrap();
    match query {
        nom_sql::SqlQuery::Select(stmt) => {
            let cond = stmt.where_clause.unwrap();
            let expr = Expr::from_nom(cond, desc).unwrap();
            assert_eq!(expr.output_type, expect_ty);
            let expr_out = (expr.function)(input).unwrap();
            assert_eq!(expr_out, expect_out);
        }
        _ => panic!("Test query is not a Select statement"),
    }
}

fn true_bytes() -> Vec<u8> {
    bincode::serialize(&1u8).unwrap()
}

fn false_bytes() -> Vec<u8> {
    bincode::serialize(&0u8).unwrap()
}
