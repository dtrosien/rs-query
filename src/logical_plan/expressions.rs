use crate::datatypes::schema::Field;
use crate::logical_plan::expression_types::{
    Add, Alias, And, Avg, CastExpr, Column, ColumnIndex, Count, CountDistinct, Divide, Eq, Gt,
    GtEq, LiteralDouble, LiteralFloat, LiteralLong, LiteralString, Lt, LtEq, Max, Min, Modulus,
    Multiply, Neq, Not, Or, Subtract, Sum,
};
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt::{Display, Formatter, Pointer};
use std::sync::Arc;

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

pub enum LiteralExpr {
    LiteralString(LiteralString),
    LiteralLong(LiteralLong),
    LiteralFloat(LiteralFloat),
    LiteralDouble(LiteralDouble),
}

pub enum BinaryExpr {
    And(And),
    Or(Or),
    Eq(Eq),
    Neq(Neq),
    Gt(Gt),
    GtEq(GtEq),
    Lt(Lt),
    LtEq(LtEq),
}

pub enum UnaryExpr {
    Not(Not),
}

pub enum MathExpr {
    Add(Add),
    Subtract(Subtract),
    Multiply(Multiply),
    Divide(Divide),
    Modulus(Modulus),
}

pub enum AggrExpr {
    Max(Max),
    Min(Min),
    Sum(Sum),
    Avg(Avg),
    Count(Count),
    CountDistinct(CountDistinct),
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

impl Display for LiteralExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl Display for MathExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MathExpr::Add(m) => m.fmt(f),
            MathExpr::Divide(m) => m.fmt(f),
            MathExpr::Subtract(m) => m.fmt(f),
            MathExpr::Multiply(m) => m.fmt(f),
            MathExpr::Modulus(m) => m.fmt(f),
        }
    }
}

impl LogicalExpr for MathExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        match self {
            MathExpr::Add(m) => m.to_field(input),
            MathExpr::Divide(m) => m.to_field(input),
            MathExpr::Subtract(m) => m.to_field(input),
            MathExpr::Multiply(m) => m.to_field(input),
            MathExpr::Modulus(m) => m.to_field(input),
        }
    }
}

impl Display for AggrExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggrExpr::Max(a) => a.fmt(f),
            AggrExpr::Min(a) => a.fmt(f),
            AggrExpr::Sum(a) => a.fmt(f),
            AggrExpr::Avg(a) => a.fmt(f),
            AggrExpr::Count(a) => a.fmt(f),
            AggrExpr::CountDistinct(a) => a.fmt(f),
        }
    }
}

impl LogicalExpr for AggrExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        match self {
            AggrExpr::Max(a) => a.to_field(input),
            AggrExpr::Min(a) => a.to_field(input),
            AggrExpr::Sum(a) => a.to_field(input),
            AggrExpr::Avg(a) => a.to_field(input),
            AggrExpr::Count(a) => a.to_field(input),
            AggrExpr::CountDistinct(a) => a.to_field(input),
        }
    }
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

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryExpr::And(b) => b.fmt(f),
            BinaryExpr::Or(b) => b.fmt(f),
            BinaryExpr::Eq(b) => b.fmt(f),
            BinaryExpr::Neq(b) => b.fmt(f),
            BinaryExpr::Gt(b) => b.fmt(f),
            BinaryExpr::GtEq(b) => b.fmt(f),
            BinaryExpr::Lt(b) => b.fmt(f),
            BinaryExpr::LtEq(b) => b.fmt(f),
        }
    }
}

impl LogicalExpr for BinaryExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        match self {
            BinaryExpr::And(b) => b.to_field(input),
            BinaryExpr::Or(b) => b.to_field(input),
            BinaryExpr::Eq(b) => b.to_field(input),
            BinaryExpr::Neq(b) => b.to_field(input),
            BinaryExpr::Gt(b) => b.to_field(input),
            BinaryExpr::GtEq(b) => b.to_field(input),
            BinaryExpr::Lt(b) => b.to_field(input),
            BinaryExpr::LtEq(b) => b.to_field(input),
        }
    }
}

trait MathExprExt {
    fn add(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn subtract(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn mult(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn div(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn modulus(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
}

impl MathExprExt for Expr {
    fn add(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Math(MathExpr::Add(Add::new(self, rhs))))
    }

    fn subtract(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Math(MathExpr::Subtract(Subtract::new(self, rhs))))
    }

    fn mult(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Math(MathExpr::Multiply(Multiply::new(self, rhs))))
    }

    fn div(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Math(MathExpr::Divide(Divide::new(self, rhs))))
    }

    fn modulus(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Math(MathExpr::Modulus(Modulus::new(self, rhs))))
    }
}

trait BooleanBinaryExprExt {
    fn eq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn neq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn gt(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn gteq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn lt(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
    fn lteq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr>;
}

impl BooleanBinaryExprExt for Expr {
    fn eq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Binary(BinaryExpr::Eq(Eq::new(self, rhs))))
    }

    fn neq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Binary(BinaryExpr::Neq(Neq::new(self, rhs))))
    }

    fn gt(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Binary(BinaryExpr::Gt(Gt::new(self, rhs))))
    }

    fn gteq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Binary(BinaryExpr::GtEq(GtEq::new(self, rhs))))
    }

    fn lt(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Binary(BinaryExpr::Lt(Lt::new(self, rhs))))
    }

    fn lteq(self: Arc<Self>, rhs: Arc<Expr>) -> Arc<Expr> {
        Arc::new(Expr::Binary(BinaryExpr::LtEq(LtEq::new(self, rhs))))
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

#[cfg(test)]
mod test {
    use crate::logical_plan::expression_types::{col, lit_long, max, min, Alias, Min};
    use crate::logical_plan::expressions::{AliasExprExt, BooleanBinaryExprExt, MathExprExt};
    use std::sync::Arc;

    #[test]
    fn display_expressions_test() {
        let lit1 = Arc::new(lit_long(5));
        let lit2 = Arc::new(lit_long(10));

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
    fn chained_expression_test() {
        let col1 = Arc::new(col("COL_1"));
        let col2 = Arc::new(col("COL_2"));

        let col_result = col1.clone().eq(col2.clone());

        let max = max(col1);
        let col_result2 = max.div(min(col2).alias("min2"));

        assert_eq!("COL_1 = COL_2", col_result.to_string());
        assert_eq!("MAX(COL_1) / MIN(COL_2) AS min2", col_result2.to_string())
    }
}
