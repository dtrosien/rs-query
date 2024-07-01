use rquery::datatypes::arrow_types::ArrowType;
use rquery::execution::ExecutionContext;
use rquery::logical_plan::expressions::aggr_expr::{max, min, sum};
use rquery::logical_plan::expressions::binary_expr::{and, or, BooleanBinaryExprExt};
use rquery::logical_plan::expressions::literal_expr::{lit_double, lit_long, lit_str};
use rquery::logical_plan::expressions::math_expr::MathExprExt;
use rquery::logical_plan::expressions::{alias, cast, col};
use rquery::logical_plan::LogicalPlanPrinter;
use rquery::physical_plan::PhysicalPlanPrinter;
use rquery::query_planner::QueryPlanner;
use std::any::Any;
use std::collections::HashMap;
use std::ops::Deref;

mod common;
#[test]
fn test_project_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .project(vec![col("first_name")]);

    //  println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        //    print!("{result_str}");
    });
}

#[test]
fn test_filter_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(col("state").eq(lit_str("CO")))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    //  println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        //    print!("{result_str}");
    });
}

#[test]
fn test_filter_or_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(or(
            col("state").eq(lit_str("CO")),
            col("state").eq(lit_str("CA")),
        ))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    //  println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        //   print!("{result_str}");
    });
}

#[test]
fn test_filter_and_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(and(
            col("state").eq(lit_str("CO")),
            cast(col("salary"), ArrowType::Int64Type).eq(lit_long(11500)),
        ))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    //   println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        //  print!("{result_str}");
    });
}

#[test]
fn test_cast_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(cast(col("salary"), ArrowType::Int64Type).eq(lit_long(10000)));

    //  println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        // print!("{result_str}");
    });
}

#[test]
fn test_math_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx.csv("testdata/employee.csv", true).project(vec![
        alias(
            cast(col("salary"), ArrowType::Int64Type).mult(lit_long(100)),
            "multi_salary",
        ),
        alias(
            cast(col("salary"), ArrowType::Int64Type).div(lit_long(100)),
            "div_salary",
        ),
        alias(
            cast(col("salary"), ArrowType::Int64Type).add(lit_long(100)),
            "add_salary",
        ),
        alias(
            cast(col("salary"), ArrowType::Int64Type).subtract(lit_long(100)),
            "subs_salary",
        ),
        alias(
            cast(col("salary"), ArrowType::DoubleType).modulus(lit_double(17_f64)),
            "mod_salary",
        ),
    ]);

    //println!("{}", df.clone().logical_plan().pretty());
    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        //  print!("{result_str}");
    });
}

#[test]
fn test_aggregate_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx.csv("testdata/employee.csv", true).aggregate(
        vec![col("state")],
        vec![
            sum(cast(col("salary"), ArrowType::Int64Type)),
            max(cast(col("salary"), ArrowType::Int64Type)),
            min(cast(col("salary"), ArrowType::Int64Type)),
        ],
    );

    println!("{}", df.clone().logical_plan().pretty());

    println!(
        "{}",
        QueryPlanner::create_physical_plan(df.clone().logical_plan().deref()).pretty()
    );

    let r = ctx.execute(df).for_each(|result| {
        let result_str = result.show().unwrap();
        print!("{result_str}");
    });
}
