use crate::datatypes::schema::{Field, Schema};
use crate::logical_plan::aggregate::Aggregate;
use crate::logical_plan::expressions::aggr_expr::AggrExpr;
use crate::logical_plan::expressions::binary_expr::{Base, BinaryExpr};
use crate::logical_plan::expressions::literal_expr::LiteralExpr;
use crate::logical_plan::expressions::math_expr::MathExpr;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::projection::Projection;
use crate::logical_plan::scan::Scan;
use crate::logical_plan::selection::Selection;
use crate::logical_plan::LogicalPlan;
use crate::physical_plan::expressions::aggregate_expression::AggregateExpression;
use crate::physical_plan::expressions::boolean_expression::{
    AndExpression, EqExpression, GtEqExpression, GtExpression, LtEqExpression, LtExpression,
    NeqExpression, OrExpression,
};
use crate::physical_plan::expressions::cast_expression::CastExpression;
use crate::physical_plan::expressions::column_expression::ColumnExpression;
use crate::physical_plan::expressions::math_expression::{
    AddExpression, DivideExpression, ModulusExpression, MultiplyExpression, SubtractExpression,
};
use crate::physical_plan::expressions::max_expression::MaxExpression;
use crate::physical_plan::expressions::min_expression::MinExpression;
use crate::physical_plan::expressions::sum_expression::SumExpression;
use crate::physical_plan::expressions::{
    Expression, LiteralDoubleExpression, LiteralFloatExpression, LiteralLongExpression,
    LiteralStringExpression,
};
use crate::physical_plan::hash_aggregate_exec::HashAggregateExec;
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
            let input = QueryPlanner::create_physical_plan(aggregate.input.deref());
            let group_expr: Vec<Arc<dyn Expression>> = aggregate
                .group_expr
                .iter()
                .map(|e| Self::create_physical_expr(e.clone(), aggregate.input.deref()))
                .collect();
            let aggregate_expr: Vec<Arc<dyn AggregateExpression>> = aggregate
                .aggregate_expr
                .iter()
                .map(|e| match e.deref() {
                    Expr::Aggr(AggrExpr::Sum(sum)) => Arc::new(SumExpression {
                        expr: Self::create_physical_expr(
                            sum.base.expr.clone(),
                            aggregate.input.deref(),
                        ),
                    })
                        as Arc<dyn AggregateExpression>,

                    Expr::Aggr(AggrExpr::Max(max)) => Arc::new(MaxExpression {
                        expr: Self::create_physical_expr(
                            max.base.expr.clone(),
                            aggregate.input.deref(),
                        ),
                    })
                        as Arc<dyn AggregateExpression>,

                    Expr::Aggr(AggrExpr::Min(min)) => Arc::new(MinExpression {
                        expr: Self::create_physical_expr(
                            min.base.expr.clone(),
                            aggregate.input.deref(),
                        ),
                    })
                        as Arc<dyn AggregateExpression>,

                    _ => panic!("NOT SUPPORTED AGGREGATE"),
                })
                .collect();

            return Arc::new(HashAggregateExec {
                input,
                group_expr,
                aggregate_expr,
                schema: aggregate.schema(),
            });
        } else {
            panic!("not supported physical plan: {}", plan.to_string())
        }
    }

    pub fn create_physical_expr(expr: Arc<Expr>, input: &dyn LogicalPlan) -> Arc<dyn Expression> {
        match &*expr {
            Expr::Column(col) => {
                let i = input
                    .schema()
                    .fields
                    .iter()
                    .position(|f| f.name.eq(&col.name))
                    .expect(format!("No column with name {}", col.name).as_str());
                Arc::new(ColumnExpression { i })
            }
            Expr::ColumnIndex(col_index) => Arc::new(ColumnExpression { i: col_index.i }),
            Expr::Literal(lit) => match lit {
                LiteralExpr::LiteralString(s) => Arc::new(LiteralStringExpression {
                    value: s.str.clone(),
                }),
                LiteralExpr::LiteralLong(l) => Arc::new(LiteralLongExpression { value: l.i }),
                LiteralExpr::LiteralFloat(f) => Arc::new(LiteralFloatExpression { value: f.i }),
                LiteralExpr::LiteralDouble(d) => Arc::new(LiteralDoubleExpression { value: d.i }),
            },
            Expr::Cast(cast) => {
                let expr = Self::create_physical_expr(cast.expr.clone(), input);
                Arc::new(CastExpression {
                    expr,
                    data_type: cast.data_type.clone(),
                })
            }
            Expr::Binary(bin) => {
                let l = Self::create_physical_expr(bin.get_left().clone(), input);
                let r = Self::create_physical_expr(bin.get_right().clone(), input);
                match bin {
                    BinaryExpr::And(_) => Arc::new(AndExpression { l, r }),
                    BinaryExpr::Or(_) => Arc::new(OrExpression { l, r }),
                    BinaryExpr::Eq(_) => Arc::new(EqExpression { l, r }),
                    BinaryExpr::Neq(_) => Arc::new(NeqExpression { l, r }),
                    BinaryExpr::Gt(_) => Arc::new(GtExpression { l, r }),
                    BinaryExpr::GtEq(_) => Arc::new(GtEqExpression { l, r }),
                    BinaryExpr::Lt(_) => Arc::new(LtExpression { l, r }),
                    BinaryExpr::LtEq(_) => Arc::new(LtEqExpression { l, r }),
                }
            }
            Expr::Math(math) => {
                let l = Self::create_physical_expr(math.get_left().clone(), input);
                let r = Self::create_physical_expr(math.get_right().clone(), input);
                match math {
                    MathExpr::Add(_) => Arc::new(AddExpression { l, r }),
                    MathExpr::Subtract(_) => Arc::new(SubtractExpression { l, r }),
                    MathExpr::Multiply(_) => Arc::new(MultiplyExpression { l, r }),
                    MathExpr::Divide(_) => Arc::new(DivideExpression { l, r }),
                    MathExpr::Modulus(_) => Arc::new(ModulusExpression { l, r }),
                }
            }
            Expr::Alias(alias) => {
                // note that there is no physical expression for an alias since the alias
                // only affects the name using in the planning phase and not how the aliased
                // expression is executed
                Self::create_physical_expr(alias.expr.clone(), input)
            }
            _ => panic!("not supported physical expression"),
        }
    }
}
