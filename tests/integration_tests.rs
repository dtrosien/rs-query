use rquery::execution::ExecutionContext;
use rquery::logical_plan::expressions::col;
use std::collections::HashMap;

mod common;
#[test]
fn test_select_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx.csv("testdata/employee.csv", true);
    //  .project(vec![col("first_name")]);

    let _ = ctx.execute(df);
}
