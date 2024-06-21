use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub enum AggrExpr {
    Max(Max),
    Min(Min),
    Sum(Sum),
    Avg(Avg),
    Count(Count),
    CountDistinct(CountDistinct),
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

pub struct AggregationExprBase {
    name: String,
    expr: Arc<Expr>,
}

impl AggregationExprBase {
    fn new(name: String, expr: Arc<Expr>) -> Self {
        Self { name, expr }
    }
}

impl LogicalExpr for AggregationExprBase {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.name.clone(),
            data_type: self.expr.to_field(input)?.data_type.clone(),
        }))
    }
}

impl Display for AggregationExprBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name, self.expr)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Max {
    base: AggregationExprBase,
}

/// Convenience method to create a max reference
pub fn max(expr: Arc<Expr>) -> Arc<Expr> {
    // todo how can i return here AggrExpr and still manage to concat it with Expr (see also Aggregate LogicalPlan)
    Arc::from(Expr::Aggr(AggrExpr::Max(Max::new(expr))))
}

impl Max {
    fn new(input: Arc<Expr>) -> Self {
        Self {
            base: AggregationExprBase::new("MAX".to_string(), input),
        }
    }
}

impl LogicalExpr for Max {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Max {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Min {
    base: AggregationExprBase,
}
pub fn min(expr: Arc<Expr>) -> Arc<Expr> {
    Arc::from(Expr::Aggr(AggrExpr::Min(Min::new(expr))))
}

impl Min {
    fn new(input: Arc<Expr>) -> Self {
        Self {
            base: AggregationExprBase::new("MIN".to_string(), input),
        }
    }
}

impl LogicalExpr for Min {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Min {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Sum {
    base: AggregationExprBase,
}

pub fn sum(expr: Arc<Expr>) -> Arc<Expr> {
    Arc::from(Expr::Aggr(AggrExpr::Sum(Sum::new(expr))))
}

impl Sum {
    fn new(input: Arc<Expr>) -> Self {
        Self {
            base: AggregationExprBase::new("SUM".to_string(), input),
        }
    }
}

impl LogicalExpr for Sum {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Sum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Avg {
    base: AggregationExprBase,
}

pub fn avg(expr: Arc<Expr>) -> Arc<Expr> {
    Arc::from(Expr::Aggr(AggrExpr::Avg(Avg::new(expr))))
}
impl Avg {
    fn new(input: Arc<Expr>) -> Self {
        Self {
            base: AggregationExprBase::new("AVG".to_string(), input),
        }
    }
}

impl LogicalExpr for Avg {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        self.base.to_field(input)
    }
}

impl Display for Avg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.base)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct Count {
    base: AggregationExprBase,
}

pub fn count(expr: Arc<Expr>) -> Arc<Expr> {
    Arc::from(Expr::Aggr(AggrExpr::Count(Count::new(expr))))
}

impl Count {
    fn new(input: Arc<Expr>) -> Self {
        Self {
            base: AggregationExprBase::new("COUNT".to_string(), input),
        }
    }
}

impl LogicalExpr for Count {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: "COUNT".to_string(),
            data_type: ArrowType::Int32Type,
        }))
    }
}

impl Display for Count {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "COUNT({})", self.base.expr)
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct CountDistinct {
    base: AggregationExprBase,
}

pub fn count_distinct(expr: Arc<Expr>) -> Arc<Expr> {
    Arc::from(Expr::Aggr(AggrExpr::CountDistinct(CountDistinct::new(
        expr,
    ))))
}

impl CountDistinct {
    fn new(input: Arc<Expr>) -> Self {
        Self {
            base: AggregationExprBase::new("COUNT DISTINCT".to_string(), input),
        }
    }
}

impl LogicalExpr for CountDistinct {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: "COUNT_DISTINCT".to_string(),
            data_type: ArrowType::Int32Type,
        }))
    }
}

impl Display for CountDistinct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "COUNT(DISTINCT {})", self.base.expr)
    }
}
