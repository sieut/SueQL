use bincode;
use data_type::DataType;
use super::Expr;
use nom_sql;
use tuple::TupleDesc;
use test_utils::{setup, teardown};
use rel::Rel;

#[test]
fn test_col_helpers() {
    let mut db_state = setup("test_col_helpers");
    let rel = Rel::new(
        "test_col_helpers",
        TupleDesc::new(vec![DataType::I32], vec!["col"]),
        &mut db_state).unwrap();
    let query = nom_sql::parse_query("select * from test where col = 1 + 1").unwrap();
    let (is_only_col, is_no_col) = match query {
        nom_sql::SqlQuery::Select(stmt) => {
            let cond = stmt.where_clause.unwrap();
            match cond {
                nom_sql::ConditionExpression::ComparisonOp(tree) => {
                    (Expr::is_only_col((*tree.left).clone(), &rel),
                     Expr::is_no_col((*tree.right).clone()))
                }
                _ => (None, false),
            }
        }
        _ => (None, false),
    };
    teardown(db_state);
    assert!(is_only_col.is_some());
    assert_eq!(is_only_col.unwrap(), 0);
    assert!(is_no_col);
}

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
        Some(desc.clone()),
        &data,
        DataType::Bool,
        false_bytes(),
    );
    test_query_output(
        "select * from test where test_int = 10",
        Some(desc.clone()),
        &data,
        DataType::Bool,
        true_bytes(),
    );
    test_query_output(
        "select * from test where test_str = 'World'",
        Some(desc.clone()),
        &data,
        DataType::Bool,
        false_bytes(),
    );
    test_query_output(
        "select * from test where test_str = 'Hello'",
        Some(desc.clone()),
        &data,
        DataType::Bool,
        true_bytes(),
    );
}

fn test_query_output(
    query: &str,
    desc: Option<TupleDesc>,
    input: &Vec<u8>,
    expect_ty: DataType,
    expect_out: Vec<u8>,
) {
    let mut db_state = setup(query);
    let rel = Rel::new(
        query,
        desc.unwrap_or(TupleDesc::new(vec![DataType::Char], vec!["dummy"])),
        &mut db_state).unwrap();
    let query = nom_sql::parse_query(query).unwrap();
    match query {
        nom_sql::SqlQuery::Select(stmt) => {
            let cond = stmt.where_clause.unwrap();
            let expr = Expr::from_nom(cond, &rel).unwrap();
            assert_eq!(expr.output_type, expect_ty);
            let expr_out = (expr.function)(input).unwrap();
            assert_eq!(expr_out, expect_out);
        }
        _ => panic!("Test query is not a Select statement"),
    }
    teardown(db_state);
}

fn true_bytes() -> Vec<u8> {
    bincode::serialize(&1u8).unwrap()
}

fn false_bytes() -> Vec<u8> {
    bincode::serialize(&0u8).unwrap()
}
