////////////////////////////////////////////////////////////////////////////
//// Binary Expressions
////////////////////////////////////////////////////////////////////////////

use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

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

pub trait BooleanBinaryExprExt {
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

pub struct BinaryExprBase {
    name: String,
    op: String,
    l: Arc<Expr>,
    r: Arc<Expr>,
}

impl BinaryExprBase {
    fn new(name: String, op: String, l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self { name, op, l, r }
    }
}

impl Display for BinaryExprBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

struct BooleanBinaryExpr {
    base: BinaryExprBase,
}

impl BooleanBinaryExpr {
    fn new(name: String, op: String, l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BinaryExprBase::new(name, op, l, r),
        }
    }
}

impl LogicalExpr for BooleanBinaryExpr {
    fn to_field(&self, _input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.base.name.clone(),
            data_type: ArrowType::BooleanType,
        }))
    }
}

impl Display for BooleanBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct And {
    base: BooleanBinaryExpr,
}

impl And {
    fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("and".to_string(), "AND".to_string(), l, r),
        }
    }
}

impl LogicalExpr for And {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for And {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Or {
    base: BooleanBinaryExpr,
}

impl Or {
    fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("or".to_string(), "OR".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Or {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Or {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Eq {
    base: BooleanBinaryExpr,
}

impl Eq {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("eq".to_string(), "=".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Eq {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Eq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Neq {
    base: BooleanBinaryExpr,
}

impl Neq {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("neq".to_string(), "!=".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Neq {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Neq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Gt {
    base: BooleanBinaryExpr,
}

impl Gt {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("gt".to_string(), ">".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Gt {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Gt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct GtEq {
    base: BooleanBinaryExpr,
}

impl GtEq {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("gteq".to_string(), ">=".to_string(), l, r),
        }
    }
}

impl LogicalExpr for GtEq {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for GtEq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Lt {
    base: BooleanBinaryExpr,
}

impl Lt {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("lt".to_string(), "<".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Lt {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Lt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct LtEq {
    base: BooleanBinaryExpr,
}

impl LtEq {
    pub fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("lteq".to_string(), "<=".to_string(), l, r),
        }
    }
}

impl LogicalExpr for LtEq {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for LtEq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}
