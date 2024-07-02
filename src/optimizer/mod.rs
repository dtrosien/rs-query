mod projection_push_down_rule;

use crate::logical_plan::expressions::aggr_expr::AggrExpr;
use crate::logical_plan::expressions::binary_expr::Base;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::projection_push_down_rule::ProjectionPushDownRule;
use std::collections::HashSet;
use std::sync::Arc;

pub struct Optimizer;

impl Optimizer {
    pub fn optimize(plan: Arc<dyn LogicalPlan>) -> Arc<dyn LogicalPlan> {
        // use here a list of rules when new rules are implemented
        let rule = ProjectionPushDownRule;
        rule.optimize(plan)
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
        Expr::Aggr(aggr) => match aggr {
            AggrExpr::Max(max) => extract_columns(max.base.expr.clone(), input.clone(), accum),
            AggrExpr::Min(min) => extract_columns(min.base.expr.clone(), input.clone(), accum),
            AggrExpr::Sum(sum) => extract_columns(sum.base.expr.clone(), input.clone(), accum),
            AggrExpr::Avg(avg) => extract_columns(avg.base.expr.clone(), input.clone(), accum),
            AggrExpr::Count(count) => {
                extract_columns(count.base.expr.clone(), input.clone(), accum)
            }
            AggrExpr::CountDistinct(count_d) => {
                extract_columns(count_d.base.expr.clone(), input.clone(), accum)
            }
        },
        Expr::Alias(a) => extract_columns(a.expr.clone(), input.clone(), accum),
        _ => panic!("extract_columns does not support expression: {}", expr),
    }
}
