use crate::datatypes::schema::{Field, Schema};
use crate::logical_plan::expressions::literal_expr::LiteralExpr;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::projection::Projection;
use crate::logical_plan::scan::Scan;
use crate::logical_plan::selection::Selection;
use crate::logical_plan::LogicalPlan;
use crate::physical_plan::expressions::{
    Expression, LiteralDoubleExpression, LiteralLongExpression, LiteralStringExpression,
};
use crate::physical_plan::projection_exec::ProjectionExec;
use crate::physical_plan::scan_exec::ScanExec;
use crate::physical_plan::selection_exec::SelectionExec;
use crate::physical_plan::PhysicalPlan;
use std::sync::Arc;

pub struct QueryPlanner;
// if there is still a problem maybe look here:
// https://stackoverflow.com/questions/33687447/how-to-get-a-reference-to-a-concrete-type-from-a-trait-object
// arc must be sync + send to be able to downcast  e.g.  Arc::downcast::<Scan>(plan)
impl QueryPlanner {
    pub fn create_physical_plan(plan: Arc<dyn LogicalPlan>) -> Arc<dyn PhysicalPlan> {
        if let Some(scan) = plan.as_any().downcast_ref::<Scan>() {
            return Arc::new(ScanExec {
                ds: scan.datasource.clone(),
                projection: scan.projection.clone(),
            });
        }
        if let Some(selection) = plan.as_any().downcast_ref::<Arc<Selection>>() {
            let input = QueryPlanner::create_physical_plan(selection.clone());
            let filter_expr =
                Self::create_physical_expr(selection.expr.clone(), selection.input.clone());
            return Arc::new(SelectionExec {
                input,
                expr: filter_expr,
            });
        }
        if let Some(projection) = plan.as_any().downcast_ref::<Arc<Projection>>() {
            let input = QueryPlanner::create_physical_plan(projection.clone());
            let projection_expr: Vec<Arc<dyn Expression>> = projection
                .expr
                .iter()
                .map(|e| Self::create_physical_expr(e.clone(), projection.input.clone()))
                .collect();

            let projection_fields: Vec<Arc<Field>> = projection
                .expr
                .iter()
                .map(|e| e.to_field(projection.input.clone()).unwrap())
                .collect();
            let projection_schema = Schema {
                fields: projection_fields,
            };
            return Arc::new(ProjectionExec {
                input,
                schema: Arc::new(projection_schema),
                expr: projection_expr,
            });
        }
        if let Some(aggregate) = plan.as_any().downcast_ref::<Arc<Projection>>() {
            todo!()
        } else {
            panic!("not supported physical plan") // todo errorhandling
        }
    }

    pub fn create_physical_expr(
        expr: Arc<Expr>,
        input: Arc<dyn LogicalPlan>,
    ) -> Arc<dyn Expression> {
        match &*expr {
            Expr::Column(col) => {
                todo!()
            }
            Expr::ColumnIndex(col_index) => {
                todo!()
            }
            Expr::Literal(lit) => match lit {
                LiteralExpr::LiteralString(s) => {
                    return Arc::new(LiteralStringExpression {
                        value: s.str.clone(),
                    })
                }
                LiteralExpr::LiteralLong(l) => {
                    return Arc::new(LiteralLongExpression { value: l.i })
                }
                LiteralExpr::LiteralFloat(f) => {
                    todo!()
                }
                LiteralExpr::LiteralDouble(d) => {
                    return Arc::new(LiteralDoubleExpression { value: d.i })
                }
            },
            Expr::Cast(cast) => {
                todo!()
            }
            Expr::Binary(bin) => {
                todo!()
            }
            Expr::Unary(unary) => {
                todo!()
            }
            Expr::Math(math) => {
                todo!()
            }
            Expr::Aggr(aggr) => {
                todo!()
            }
            Expr::Alias(alias) => {
                // note that there is no physical expression for an alias since the alias
                // only affects the name using in the planning phase and not how the aliased
                // expression is executed
                todo!()
            }
        }
    }
}
