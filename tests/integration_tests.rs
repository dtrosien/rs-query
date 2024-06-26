use rquery::execution::ExecutionContext;
use rquery::logical_plan::expressions::col;
use rquery::logical_plan::LogicalPlanPrinter;
use std::collections::HashMap;
use std::ops::Deref;

mod common;
#[test]
fn test_select_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .project(vec![col("first_name")]);

    // println!("{}", df.clone().logical_plan().pretty());
    // let r = ctx.execute(df).for_each(|result| {
    //     let result_str = result.to_csv().unwrap();
    //     print!("{result_str}");
    // });
}
