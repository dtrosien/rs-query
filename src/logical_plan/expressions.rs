use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use anyhow::anyhow;
use std::fmt::Display;
use std::sync::Arc;

/// Logical expression representing a reference to a column by name.
pub struct Column {
    pub name: String,
}

/// Convenience method to create a Column reference
pub fn col(name: &str) -> Column {
    Column {
        name: name.to_string(),
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl LogicalExpr for Column {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(input
            .schema()
            .fields
            .iter()
            .find(|f| self.name == f.name)
            .ok_or(anyhow!("Column {} not present in input", self.name))?
            .clone())
    }
}

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a reference to a column by index.
pub struct ColumnIndex {
    i: usize,
}

impl Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.i)
    }
}

impl LogicalExpr for ColumnIndex {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(input
            .schema()
            .fields
            .get(self.i)
            .ok_or(anyhow!("ColumnIndex {} not present in input", self.i))?
            .clone())
    }
}

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a literal string value.
pub struct LiteralString {
    str: String,
}

/// Convenience method to create a LiteralString
pub fn lit_str(value: &str) -> LiteralString {
    LiteralString {
        str: value.to_string(),
    }
}

impl Display for LiteralString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    i: i64,
}

/// Convenience method to create a LiteralLong
pub fn lit_long(value: i64) -> LiteralLong {
    LiteralLong { i: value }
}

impl Display for LiteralLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
pub fn lit_float(value: f32) -> LiteralFloat {
    LiteralFloat { i: value }
}

impl Display for LiteralFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
pub fn lit_double(value: f64) -> LiteralDouble {
    LiteralDouble { i: value }
}

impl Display for LiteralDouble {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a cast of datatypes
pub struct CastExpr {
    expr: Arc<dyn LogicalExpr>,
    data_type: ArrowType,
}

/// Convenience method to create a CastExpr
pub fn cast(expr: Arc<dyn LogicalExpr>, data_type: ArrowType) -> CastExpr {
    CastExpr { expr, data_type }
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cast({} AS {:?})", self.expr.to_string(), self.data_type)
    }
}

impl LogicalExpr for CastExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.expr.to_field(input)?.name.clone(),
            data_type: self.data_type.clone(),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct BinaryExprBase {
    name: String,
    op: String,
    l: Arc<dyn LogicalExpr>,
    r: Arc<dyn LogicalExpr>,
}

impl Display for BinaryExprBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.l.to_string(),
            self.op,
            self.r.to_string()
        )
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct UnaryExprBase {
    name: String,
    op: String,
    expr: Arc<dyn LogicalExpr>,
}

impl Display for UnaryExprBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.op, self.expr.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////

/// Aliased expression e.g. `expr.alias`.
struct Alias {
    expr: Arc<dyn LogicalExpr>,
    alias: String,
}

impl Alias {
    fn new(expr: Arc<dyn LogicalExpr>, alias: String) -> Self {
        Self { expr, alias }
    }
}

impl Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AS {}", self.expr.to_string(), self.alias)
    }
}

impl LogicalExpr for Alias {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.alias.clone(),
            data_type: self.expr.to_field(input)?.data_type.clone(),
        }))
    }
}

trait AliasExt {
    fn alias(self: Arc<Self>, alias: String) -> Arc<Alias>;
}

impl<T> AliasExt for T
where
    T: LogicalExpr + 'static,
{
    fn alias(self: Arc<Self>, alias: String) -> Arc<Alias> {
        Arc::new(Alias::new(self, alias))
    }
}
