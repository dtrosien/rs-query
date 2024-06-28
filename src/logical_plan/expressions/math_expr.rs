use crate::datatypes::schema::Field;
use crate::logical_plan::expressions::binary_expr::{Base, BinaryExprBase};
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
////////////////////////////////////////////////////////////////////////////
//// Math Expressions
////////////////////////////////////////////////////////////////////////////

pub enum MathExpr {
    Add(Add),
    Subtract(Subtract),
    Multiply(Multiply),
    Divide(Divide),
    Modulus(Modulus),
}

impl Display for MathExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

impl Base for MathExpr {
    fn get_base(&self) -> &BinaryExprBase {
        match self {
            MathExpr::Add(m) => &m.base,
            MathExpr::Subtract(m) => &m.base,
            MathExpr::Multiply(m) => &m.base,
            MathExpr::Divide(m) => &m.base,
            MathExpr::Modulus(m) => &m.base,
        }
    }
}

pub trait MathExprExt {
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

impl LogicalExpr for BinaryExprBase {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.name.clone(),
            data_type: self.l.to_field(input)?.data_type.clone(),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Add {
    base: BinaryExprBase,
}

impl Add {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BinaryExprBase::new("add".to_string(), "+".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Add {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Add {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Subtract {
    base: BinaryExprBase,
}

impl Subtract {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BinaryExprBase::new("subtract".to_string(), "-".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Subtract {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Subtract {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Multiply {
    base: BinaryExprBase,
}

impl Multiply {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BinaryExprBase::new("mult".to_string(), "*".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Multiply {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Multiply {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Divide {
    pub base: BinaryExprBase,
}

impl Divide {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BinaryExprBase::new("div".to_string(), "/".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Divide {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Divide {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Modulus {
    base: BinaryExprBase,
}

impl Modulus {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: BinaryExprBase::new("mod".to_string(), "%".to_string(), l, r),
        }
    }
}

impl LogicalExpr for Modulus {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Modulus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}
