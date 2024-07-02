mod projection_push_down_rule;

use crate::logical_plan::expressions::binary_expr::Base;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::LogicalPlan;
use std::collections::HashSet;
use std::sync::Arc;

pub struct Optimizer;

impl Optimizer {
    pub fn optimize(plan: Arc<dyn LogicalPlan>) -> Arc<dyn LogicalPlan> {
        todo!()
    }
}

trait OptimizerRule {
    fn optimize(&self, plan: Arc<dyn LogicalPlan>) -> Arc<dyn LogicalPlan>;
}

fn extract_all_columns(
    expr: Vec<Arc<Expr>>,
    input: Arc<dyn LogicalPlan>,
    accum: &mut HashSet<String>,
) {
    expr.iter()
        .for_each(|e| extract_columns(e.clone(), input.clone(), accum))
}

fn extract_columns(expr: Arc<Expr>, input: Arc<dyn LogicalPlan>, accum: &mut HashSet<String>) {
    match &*expr {
        Expr::Column(col) => {
            accum.insert(col.name.clone());
        }
        Expr::ColumnIndex(col_index) => {
            accum.insert(input.schema().fields.get(col_index.i).unwrap().name.clone());
        }
        Expr::Literal(_) => {}
        Expr::Cast(e) => extract_columns(e.expr.clone(), input.clone(), accum),
        Expr::Binary(bin) => {
            extract_columns(bin.get_left(), input.clone(), accum);
            extract_columns(bin.get_right(), input.clone(), accum);
        }
        Expr::Alias(a) => extract_columns(a.expr.clone(), input.clone(), accum),
        _ => panic!("extract_columns does not support expression: {}", expr),
    }
}
