use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub enum LiteralExpr {
    LiteralString(LiteralString),
    LiteralLong(LiteralLong),
    LiteralFloat(LiteralFloat),
    LiteralDouble(LiteralDouble),
}
impl Display for LiteralExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LiteralExpr::LiteralString(l) => l.fmt(f),
            LiteralExpr::LiteralLong(l) => l.fmt(f),
            LiteralExpr::LiteralFloat(l) => l.fmt(f),
            LiteralExpr::LiteralDouble(l) => l.fmt(f),
        }
    }
}

impl LogicalExpr for LiteralExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        match self {
            LiteralExpr::LiteralString(l) => l.to_field(input),
            LiteralExpr::LiteralLong(l) => l.to_field(input),
            LiteralExpr::LiteralFloat(l) => l.to_field(input),
            LiteralExpr::LiteralDouble(l) => l.to_field(input),
        }
    }
}

/// Logical expression representing a literal string value.
pub struct LiteralString {
    str: String,
}

/// Convenience method to create a LiteralString
pub fn lit_str(value: &str) -> Arc<Expr> {
    Arc::from(Expr::Literal(LiteralExpr::LiteralString(LiteralString {
        str: value.to_string(),
    })))
}

impl Display for LiteralString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.str)
    }
}

impl LogicalExpr for LiteralString {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.str.clone(),
            data_type: ArrowType::StringType,
        }))
    }
}
////////////////////////////////////////////////////////////////////////////
/// Logical expression representing a literal long value.
pub struct LiteralLong {
    pub i: i64,
}

/// Convenience method to create a LiteralLong
pub fn lit_long(value: i64) -> Arc<Expr> {
    Arc::from(Expr::Literal(LiteralExpr::LiteralLong(LiteralLong {
        i: value,
    })))
}

impl Display for LiteralLong {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.i)
    }
}

impl LogicalExpr for LiteralLong {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.i.to_string(),
            data_type: ArrowType::Int64Type,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a literal float value.
pub struct LiteralFloat {
    i: f32,
}

/// Convenience method to create a LiteralFloat
pub fn lit_float(value: f32) -> Arc<Expr> {
    Arc::from(Expr::Literal(LiteralExpr::LiteralFloat(LiteralFloat {
        i: value,
    })))
}

impl Display for LiteralFloat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.i)
    }
}

impl LogicalExpr for LiteralFloat {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.i.to_string(),
            data_type: ArrowType::FloatType,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a literal double value.
pub struct LiteralDouble {
    i: f64,
}

/// Convenience method to create a LiteralDouble
pub fn lit_double(value: f64) -> Arc<Expr> {
    Arc::from(Expr::Literal(LiteralExpr::LiteralDouble(LiteralDouble {
        i: value,
    })))
}

impl Display for LiteralDouble {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.i)
    }
}

impl LogicalExpr for LiteralDouble {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.i.to_string(),
            data_type: ArrowType::DoubleType,
        }))
    }
}
