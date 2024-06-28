use crate::datatypes::schema::{Field, Schema};
use crate::logical_plan::aggregate::Aggregate;
use crate::logical_plan::expressions::binary_expr::{Base, BinaryExpr};
use crate::logical_plan::expressions::literal_expr::LiteralExpr;
use crate::logical_plan::expressions::math_expr::MathExpr;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::projection::Projection;
use crate::logical_plan::scan::Scan;
use crate::logical_plan::selection::Selection;
use crate::logical_plan::LogicalPlan;
use crate::physical_plan::expressions::boolean_expression::{
    AndExpression, EqExpression, GtEqExpression, GtExpression, LtEqExpression, LtExpression,
    NeqExpression, OrExpression,
};
use crate::physical_plan::expressions::{
    Expression, LiteralDoubleExpression, LiteralFloatExpression, LiteralLongExpression,
    LiteralStringExpression,
};
use crate::physical_plan::projection_exec::ProjectionExec;
use crate::physical_plan::scan_exec::ScanExec;
use crate::physical_plan::selection_exec::SelectionExec;
use crate::physical_plan::PhysicalPlan;
use std::ops::Deref;
use std::sync::Arc;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn create_physical_plan(plan: &dyn LogicalPlan) -> Arc<dyn PhysicalPlan> {
        if let Some(scan) = plan.as_any().downcast_ref::<Scan>() {
            return Arc::new(ScanExec {
                ds: scan.datasource.clone(),
                projection: scan.projection.clone(),
            });
        }
        if let Some(selection) = plan.as_any().downcast_ref::<Selection>() {
            let input = QueryPlanner::create_physical_plan(selection.input.deref());
            let filter_expr =
                Self::create_physical_expr(selection.expr.clone(), selection.input.deref());
            return Arc::new(SelectionExec {
                input,
                expr: filter_expr,
            });
        }
        if let Some(projection) = plan.as_any().downcast_ref::<Projection>() {
            let input = QueryPlanner::create_physical_plan(projection.input.deref());
            let projection_expr: Vec<Arc<dyn Expression>> = projection
                .expr
                .iter()
                .map(|e| Self::create_physical_expr(e.clone(), projection.input.deref()))
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
        if let Some(aggregate) = plan.as_any().downcast_ref::<Aggregate>() {
            todo!()
        } else {
            panic!("not supported physical plan") // todo errorhandling
        }
    }

    pub fn create_physical_expr(expr: Arc<Expr>, input: &dyn LogicalPlan) -> Arc<dyn Expression> {
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
                    return Arc::new(LiteralFloatExpression { value: f.i })
                }
                LiteralExpr::LiteralDouble(d) => {
                    return Arc::new(LiteralDoubleExpression { value: d.i })
                }
            },
            Expr::Cast(cast) => {
                todo!()
            }
            Expr::Binary(bin) => {
                let l = Self::create_physical_expr(bin.get_left().clone(), input);
                let r = Self::create_physical_expr(bin.get_right().clone(), input);
                match bin {
                    BinaryExpr::And(a) => Arc::new(AndExpression { l, r }),
                    BinaryExpr::Or(_) => Arc::new(OrExpression { l, r }),
                    BinaryExpr::Eq(_) => Arc::new(EqExpression { l, r }),
                    BinaryExpr::Neq(_) => Arc::new(NeqExpression { l, r }),
                    BinaryExpr::Gt(_) => Arc::new(GtExpression { l, r }),
                    BinaryExpr::GtEq(_) => Arc::new(GtEqExpression { l, r }),
                    BinaryExpr::Lt(_) => Arc::new(LtExpression { l, r }),
                    BinaryExpr::LtEq(_) => Arc::new(LtEqExpression { l, r }),
                }
            }
            Expr::Unary(unary) => {
                todo!()
            }
            Expr::Math(math) => {
                let l = Self::create_physical_expr(math.get_left().clone(), input);
                let r = Self::create_physical_expr(math.get_right().clone(), input);
                match math {
                    MathExpr::Add(_) => {
                        todo!()
                    }
                    MathExpr::Subtract(_) => {
                        todo!()
                    }
                    MathExpr::Multiply(_) => {
                        todo!()
                    }
                    MathExpr::Divide(_) => {
                        todo!()
                    }
                    MathExpr::Modulus(_) => {
                        todo!()
                    }
                }
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
