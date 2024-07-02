use crate::logical_plan::aggregate::Aggregate;
use crate::logical_plan::projection::Projection;
use crate::logical_plan::scan::Scan;
use crate::logical_plan::selection::Selection;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::{extract_all_columns, extract_columns, OptimizerRule};
use std::collections::HashSet;
use std::sync::Arc;

pub struct ProjectionPushDownRule;

impl OptimizerRule for ProjectionPushDownRule {
    fn optimize(&self, plan: Arc<dyn LogicalPlan>) -> Arc<dyn LogicalPlan> {
        let mut col_set = HashSet::new();
        Self::push_down(plan, &mut col_set)
    }
}

impl ProjectionPushDownRule {
    fn push_down(
        plan: Arc<dyn LogicalPlan>,
        column_names: &mut HashSet<String>,
    ) -> Arc<dyn LogicalPlan> {
        if let Some(projection) = plan.as_any().downcast_ref::<Projection>() {
            extract_all_columns(
                projection.expr.clone(),
                projection.input.clone(),
                column_names,
            );
            let input = Self::push_down(projection.input.clone(), column_names);
            Projection::new(input, projection.expr.clone())
        } else if let Some(selection) = plan.as_any().downcast_ref::<Selection>() {
            extract_columns(
                selection.expr.clone(),
                selection.input.clone(),
                column_names,
            );
            let input = Self::push_down(selection.input.clone(), column_names);
            Selection::new(input, selection.expr.clone())
        } else if let Some(aggregate) = plan.as_any().downcast_ref::<Aggregate>() {
            extract_all_columns(
                aggregate.group_expr.clone(),
                aggregate.input.clone(),
                column_names,
            );
            extract_all_columns(
                aggregate.aggregate_expr.clone(),
                aggregate.input.clone(),
                column_names,
            );
            let input = Self::push_down(aggregate.input.clone(), column_names);
            Aggregate::new(
                input,
                aggregate.group_expr.clone(),
                aggregate.aggregate_expr.clone(),
            )
        } else if let Some(scan) = plan.as_any().downcast_ref::<Scan>() {
            let valid_field_names: HashSet<String> = scan
                .datasource
                .schema()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect();

            let push_down_set: HashSet<String> = valid_field_names
                .iter()
                .filter(|s| column_names.contains(*s))
                .cloned()
                .collect();

            let mut push_down_vec: Vec<String> = push_down_set.into_iter().collect();
            push_down_vec.sort();

            Scan::new(&scan.path, scan.datasource.clone(), push_down_vec)
        } else {
            panic!(
                "ProjectionPushDownRule does not support plan: {}",
                plan.to_string()
            )
        }
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::arrow_types::ArrowType;
    use crate::execution::ExecutionContext;
    use crate::logical_plan::expressions::aggr_expr::{min, sum};
    use crate::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
    use crate::logical_plan::expressions::literal_expr::lit_str;
    use crate::logical_plan::expressions::{cast, col};
    use crate::logical_plan::LogicalPlanPrinter;
    use crate::optimizer::Optimizer;
    use std::collections::HashMap;

    #[test]
    fn projection_push_down() {
        let ctx = ExecutionContext::new(HashMap::default());

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

        let plan = df.logical_plan();

        let optimized_plan = Optimizer::optimize(plan);

        let expected_plan = "Aggregate: group_expr=state, aggregate_expr=SUM(CAST(salary AS DoubleType)), MIN(CAST(salary AS Int64Type))\n\
        \tSelection: state = CO\n\
        \t\tScan: testdata/employee.csv; projection=[\"salary\", \"state\"]\n";

        assert_eq!(expected_plan, optimized_plan.pretty())
    }
}
