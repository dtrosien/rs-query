use rs_query::datatypes::arrow_types::ArrowType;
use rs_query::execution::ExecutionContext;
use rs_query::logical_plan::expressions::aggr_expr::{max, min, sum};
use rs_query::logical_plan::expressions::binary_expr::{and, or, BooleanBinaryExprExt};
use rs_query::logical_plan::expressions::literal_expr::{lit_double, lit_long, lit_str};
use rs_query::logical_plan::expressions::math_expr::MathExprExt;
use rs_query::logical_plan::expressions::{alias, cast, col};
use rs_query::logical_plan::LogicalPlanPrinter;
use rs_query::optimizer::Optimizer;
use rs_query::physical_plan::PhysicalPlanPrinter;
use rs_query::query_planner::QueryPlanner;
use std::any::Any;
use std::collections::HashMap;
use std::ops::Deref;

mod common;
#[test]
fn project_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .project(vec![col("first_name")]);

    let batch = ctx.execute(df, false).next().unwrap();

    assert_eq!("Bill\nGregg\nJohn\nVon\n", batch.to_csv().unwrap());
}

#[test]
fn filter_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(col("state").eq(lit_str("CO")))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    let batch = ctx.execute(df, false).next().unwrap();

    assert_eq!(batch.schema.fields.first().unwrap().name, "name");
    assert_eq!("Langford,Gregg\nTravis,John\n", batch.to_csv().unwrap());
}

#[test]
fn filter_with_or_expression_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(or(
            col("state").eq(lit_str("CO")),
            col("state").eq(lit_str("CA")),
        ))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    let batch = ctx.execute(df, false).next().unwrap();

    assert_eq!(
        "Hopkins,Bill\nLangford,Gregg\nTravis,John\n",
        batch.to_csv().unwrap()
    );
}

#[test]
fn filter_with_and_expression_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(and(
            col("state").eq(lit_str("CO")),
            cast(col("salary"), ArrowType::Int64Type).eq(lit_long(11500)),
        ))
        .project(vec![alias(col("last_name"), "name"), col("first_name")]);

    let batch = ctx.execute(df, false).next().unwrap();
    assert_eq!("Travis,John\n", batch.to_csv().unwrap());
}

#[test]
fn filter_with_cast_to_number_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(cast(col("salary"), ArrowType::Int64Type).eq(lit_long(10000)));

    let batch = ctx.execute(df, false).next().unwrap();

    assert_eq!(
        "2,Gregg,Langford,CO,Driver,10000\n",
        batch.to_csv().unwrap()
    );
}

#[test]
fn math_expressions_from_csv() {
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

    let batch = ctx.execute(df, false).next().unwrap();

    assert_eq!("1200000,120,12100,11900,15\n1000000,100,10100,9900,4\n1150000,115,11600,11400,8\n1150000,115,11600,11400,8\n",
               batch.to_csv().unwrap()
    );
}

#[test]
fn aggregate_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .aggregate(
            vec![col("state")],
            vec![
                sum(cast(col("salary"), ArrowType::DoubleType)),
                max(cast(col("id"), ArrowType::UInt16Type)),
                min(cast(col("salary"), ArrowType::Int64Type)),
            ],
        )
        .filter(col("state").eq(lit_str("CO"))); // required because otherwise test is flaky since order of states can change

    let batch = ctx.execute(df, false).next().unwrap();
    assert_eq!("CO,21500,3,10000\n", batch.to_csv().unwrap());
}

#[test]
fn optimized_multi_plan_query_from_csv() {
    let ctx = ExecutionContext::new(HashMap::default());

    let df = ctx
        .csv("testdata/employee.csv", true)
        .filter(col("state").eq(lit_str("CO")))
        // .project(vec![alias(col("state"), "state_alias"), col("salary")])
        .aggregate(
            vec![col("state")],
            vec![
                sum(cast(col("salary"), ArrowType::DoubleType)),
                min(cast(col("salary"), ArrowType::Int64Type)),
            ],
        );

    // let log_plan = df.clone().logical_plan();
    //
    // println!("plan:  {}", log_plan.pretty());
    //
    // let optimized_plan = Optimizer::optimize(log_plan);
    //
    // println!("optimized plan: {}", optimized_plan.pretty());

    let batch = ctx.execute(df, true).next().unwrap();

    assert_eq!("CO,21500,10000\n", batch.to_csv().unwrap());
    // println!("{}", batch.show().unwrap());
}
