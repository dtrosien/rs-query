use rquery::execution::ExecutionContext;
use rquery::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
use rquery::logical_plan::expressions::literal_expr::lit_str;
use rquery::logical_plan::expressions::{alias, col};
use rquery::logical_plan::LogicalPlanPrinter;
use std::collections::HashMap;
use std::ops::Deref;

mod common;
#[test]
fn test_project_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .project(vec![col("first_name")]);

    // println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.to_csv().unwrap();
        print!("{result_str}");
    });
}

#[test]
fn test_filter_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(col("state").eq(lit_str("CO")))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    // println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.to_csv().unwrap();
        print!("{result_str}");
    });
}
