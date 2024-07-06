# rs-query

Welcome to rs-query, an educational query engine project inspired
by [How Query Engines Work](https://github.com/andygrove/how-query-engines-work).

## Overview

The query process in rs-query involves the following steps:

1. **Logical Plan Construction**: Using the DataFrame API, users can define their queries, which are then translated
   into a logical plan.
2. **Optimization**: An optional optimizer can be applied to the logical plan, utilizing techniques such as projection
   pushdown to enhance performance.
3. **Physical Plan Generation**: The optimized logical plan is converted into a physical plan.
4. **Execution**: The physical plan is executed against the data source, producing the desired results.

## Supported Operations

rs-query currently supports a small variety of operations to query data:

- **Table Scans**: Read data from CSV files.
- **Projections**: Select specific columns from the data.
- **Filtering**: Apply conditions to filter rows.
- **Aggregation**: Perform aggregate operations like min, max, and sum.

### Expressions

rs-query includes a set of expressions to perform operations and transformations on the data:

- **Mathematical Operations**:
    - Addition: `add`
    - Subtraction: `subtract`
    - Multiplication: `multiply`
    - Division: `divide`

- **Boolean Expressions**:
    - Logical OR: `or`
    - Logical AND: `and`

- **Comparison Operations**:
    - Equal: `eq`
    - Less Than: `lt`
    - Greater Than: `gt`
    - ... and more

- **Aggregates**:
    - Minimum: `min`
    - Maximum: `max`
    - Sum: `sum`

- **Other Operations**:
    - Cast: Convert data types.
    - Alias: Rename columns.
    - Literals: Use constant values in queries.

## Example

The following example demonstrates how to use the Dataframe API to query from a csv data source.
More examples can be found in the integration tests directory.

```rust
let ctx = ExecutionContext::new(HashMap::default ());

let df = ctx
.csv("testdata/employee.csv", true)
.filter(col("state").eq(lit_str("CO")))
.aggregate(
vec![col("state")],
vec![
    sum(cast(col("salary"), ArrowType::DoubleType)),
    min(cast(col("salary"), ArrowType::Int64Type)),
],
);

let log_plan = df.clone().logical_plan();
println!("plan:  {}", log_plan.pretty());

let optimized_plan = Optimizer::optimize(log_plan);
println!("optimized plan: {}", optimized_plan.pretty());

let batch = ctx.execute(df, true).next().unwrap();
println!("{}", batch.show().unwrap());

```

## To-Do

- [ ] impl sql parser
- [ ] predicate push down optimizer rule
- [ ] write fuzz tests for physical plans (create a RecordBatches generator using crates like fake or quickcheck)