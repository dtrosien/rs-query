pub mod aggr_expr;
pub mod binary_expr;
pub mod literal_expr;
pub mod math_expr;
pub mod unary_expr;

use crate::datatypes::schema::Field;
use std::fmt;

use crate::datatypes::arrow_types::ArrowType;
use crate::logical_plan::expressions::aggr_expr::AggrExpr;
use crate::logical_plan::expressions::binary_expr::BinaryExpr;
use crate::logical_plan::expressions::literal_expr::LiteralExpr;
use crate::logical_plan::expressions::math_expr::MathExpr;
use crate::logical_plan::expressions::unary_expr::UnaryExpr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use anyhow::anyhow;
use std::fmt::{Display, Formatter, Pointer};
use std::sync::Arc;

// todo rename to not confuse with physical expression
pub enum Expr {
    Column(Column),
    ColumnIndex(ColumnIndex),
    Literal(LiteralExpr),
    Cast(CastExpr),
    Binary(BinaryExpr),
    Unary(UnaryExpr),
    Math(MathExpr),
    Aggr(AggrExpr),
    Alias(Alias),
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(e) => e.fmt(f),
            Expr::ColumnIndex(e) => e.fmt(f),
            Expr::Literal(e) => e.fmt(f),
            Expr::Cast(e) => e.fmt(f),
            Expr::Binary(e) => e.fmt(f),
            Expr::Unary(e) => e.fmt(f),
            Expr::Math(e) => e.fmt(f),
            Expr::Aggr(e) => e.fmt(f),
            Expr::Alias(e) => e.fmt(f),
        }
    }
}

impl LogicalExpr for Expr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        match self {
            Expr::Column(e) => e.to_field(input),
            Expr::Literal(e) => e.to_field(input),
            Expr::Math(e) => e.to_field(input),
            Expr::ColumnIndex(e) => e.to_field(input),
            Expr::Cast(e) => e.to_field(input),
            Expr::Binary(e) => e.to_field(input),
            Expr::Unary(e) => e.to_field(input),
            Expr::Aggr(e) => e.to_field(input),
            Expr::Alias(e) => e.to_field(input),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a reference to a column by name.
pub struct Column {
    pub name: String,
}

/// Convenience method to create a Column reference
pub fn col(name: impl Into<String>) -> Arc<Expr> {
    Arc::from(Expr::Column(Column { name: name.into() }))
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

trait AliasExprExt {
    fn alias(self: Arc<Self>, alias: &str) -> Arc<Expr>;
}

impl AliasExprExt for Expr {
    fn alias(self: Arc<Self>, alias: &str) -> Arc<Expr> {
        Arc::new(Expr::Alias(Alias::new(self, alias.to_string())))
    }
}

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a cast of datatypes
pub struct CastExpr {
    expr: Arc<Expr>,
    data_type: ArrowType,
}

/// Convenience method to create a CastExpr
pub fn cast(expr: Arc<Expr>, data_type: ArrowType) -> Arc<Expr> {
    Arc::from(Expr::Cast(CastExpr { expr, data_type }))
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr.to_string(), self.data_type)
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

/// Aliased expression e.g. `expr.alias`.
pub struct Alias {
    expr: Arc<Expr>,
    alias: String,
}

/// Convenience method to create a Alias
pub fn alias(expr: Arc<Expr>, alias: impl Into<String>) -> Arc<Expr> {
    Arc::from(Expr::Alias(Alias {
        expr,
        alias: alias.into(),
    }))
}

impl Alias {
    pub(crate) fn new(expr: Arc<Expr>, alias: String) -> Self {
        Self { expr, alias }
    }
}

impl Display for Alias {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

#[cfg(test)]
mod test {
    use crate::logical_plan::expressions::aggr_expr::{max, min};
    use crate::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
    use crate::logical_plan::expressions::col;
    use crate::logical_plan::expressions::literal_expr::lit_long;
    use crate::logical_plan::expressions::math_expr::MathExprExt;
    use crate::logical_plan::expressions::AliasExprExt;
    use std::sync::Arc;

    #[test]
    fn test_display_expressions() {
        let lit1 = lit_long(5);
        let lit2 = lit_long(10);

        let add_result = lit1.clone().add(lit2.clone());
        let sub_result = lit1.clone().subtract(lit2.clone());
        let div_result = lit1.clone().div(lit2.clone());
        let mult_result = lit1.clone().mult(lit2.clone());
        let mod_result = lit1.clone().modulus(lit2.clone());
        let eq_result = lit1.clone().eq(lit2.clone());
        let gt_result = lit1.clone().gt(lit2.clone());
        let gteq_result = lit1.clone().gteq(lit2.clone());
        let neq_result = lit1.clone().neq(lit2.clone());
        let lt_result = lit1.clone().lt(lit2.clone());
        let lteq_result = lit1.clone().lteq(lit2.clone());

        assert_eq!("5 + 10", add_result.to_string());
        assert_eq!("5 - 10", sub_result.to_string());
        assert_eq!("5 / 10", div_result.to_string());
        assert_eq!("5 * 10", mult_result.to_string());
        assert_eq!("5 % 10", mod_result.to_string());
        assert_eq!("5 = 10", eq_result.to_string());
        assert_eq!("5 > 10", gt_result.to_string());
        assert_eq!("5 >= 10", gteq_result.to_string());
        assert_eq!("5 != 10", neq_result.to_string());
        assert_eq!("5 < 10", lt_result.to_string());
        assert_eq!("5 <= 10", lteq_result.to_string());
    }

    #[test]
    fn test_chained_expression() {
        let col1 = col("COL_1");
        let col2 = col("COL_2");

        let col_result = col1.clone().eq(col2.clone());

        let max = max(col1);
        let col_result2 = max.div(min(col2).alias("min2"));

        assert_eq!("COL_1 = COL_2", col_result.to_string());
        assert_eq!("MAX(COL_1) / MIN(COL_2) AS min2", col_result2.to_string())
    }
}
