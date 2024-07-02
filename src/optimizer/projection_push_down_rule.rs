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

            let push_down: HashSet<String> = valid_field_names
                .iter()
                .filter(|s| column_names.contains(*s))
                .cloned()
                .collect();

            Scan::new(
                &scan.path,
                scan.datasource.clone(),
                push_down.into_iter().collect(),
            )
        } else {
            panic!(
                "ProjectionPushDownRule does not support plan: {}",
                plan.to_string()
            )
        }
    }
}
