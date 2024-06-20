use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub enum UnaryExpr {
    Not(Not),
}

impl Display for UnaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryExpr::Not(u) => u.fmt(f),
        }
    }
}

impl LogicalExpr for UnaryExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        match self {
            UnaryExpr::Not(u) => u.to_field(input),
        }
    }
}

pub struct UnaryExprBase {
    name: String,
    op: String,
    expr: Arc<Expr>,
}

impl UnaryExprBase {
    fn new(name: String, op: String, expr: Arc<Expr>) -> Self {
        Self { name, op, expr }
    }
}

impl Display for UnaryExprBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.op, self.expr.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Not {
    base: UnaryExprBase,
}

impl Not {
    fn new(expr: Arc<Expr>) -> Self {
        Self {
            base: UnaryExprBase::new("not".to_string(), "NOT".to_string(), expr),
        }
    }
}

impl LogicalExpr for Not {
    fn to_field(&self, _input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: "NOT".to_string(),
            data_type: ArrowType::BooleanType,
        }))
    }
}

impl Display for Not {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}
