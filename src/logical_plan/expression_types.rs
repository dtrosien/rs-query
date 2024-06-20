use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use anyhow::anyhow;

use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::expressions::{AggrExpr, Expr, LiteralExpr};
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;

/// Logical expression representing a reference to a column by name.
pub struct Column {
    pub name: String,
}

/// Convenience method to create a Column reference
pub fn col(name: &str) -> Expr {
    Expr::Column(Column {
        name: name.to_string(),
    })
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

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a literal string value.
pub struct LiteralString {
    str: String,
}

/// Convenience method to create a LiteralString
pub fn lit_str(value: &str) -> Expr {
    Expr::Literal(LiteralExpr::LiteralString(LiteralString {
        str: value.to_string(),
    }))
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
pub fn lit_long(value: i64) -> Expr {
    Expr::Literal(LiteralExpr::LiteralLong(LiteralLong { i: value }))
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
pub fn lit_float(value: f32) -> Expr {
    Expr::Literal(LiteralExpr::LiteralFloat(LiteralFloat { i: value }))
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
pub fn lit_double(value: f64) -> Expr {
    Expr::Literal(LiteralExpr::LiteralDouble(LiteralDouble { i: value }))
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

////////////////////////////////////////////////////////////////////////////

/// Logical expression representing a cast of datatypes
pub struct CastExpr {
    expr: Arc<Expr>,
    data_type: ArrowType,
}

/// Convenience method to create a CastExpr
pub fn cast(expr: Arc<Expr>, data_type: ArrowType) -> Expr {
    Expr::Cast(CastExpr { expr, data_type })
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
//// Binary Expressions
////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
//// Math Expressions
////////////////////////////////////////////////////////////////////////////
/// marker trait to for math expressions (enhances logical expressions)

pub struct MathExprBase {
    name: String,
    op: String,
    l: Arc<Expr>,
    r: Arc<Expr>,
}

impl MathExprBase {
    fn new(name: String, op: String, l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self { name, op, l, r }
    }
}

impl LogicalExpr for MathExprBase {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>> {
        Ok(Arc::from(Field {
            name: self.name.clone(),
            data_type: self.l.to_field(input)?.data_type.clone(),
        }))
    }
}

impl Display for MathExprBase {
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

pub struct Add {
    base: MathExprBase,
}

impl Add {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: MathExprBase::new("add".to_string(), "+".to_string(), l, r),
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
    base: MathExprBase,
}

impl Subtract {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: MathExprBase::new("subtract".to_string(), "-".to_string(), l, r),
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
    base: MathExprBase,
}

impl Multiply {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: MathExprBase::new("mult".to_string(), "*".to_string(), l, r),
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
    pub base: MathExprBase,
}

impl Divide {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: MathExprBase::new("div".to_string(), "/".to_string(), l, r),
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
    base: MathExprBase,
}

impl Modulus {
    pub(crate) fn new(l: Arc<Expr>, r: Arc<Expr>) -> Self {
        Self {
            base: MathExprBase::new("mod".to_string(), "%".to_string(), l, r),
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

////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////

/// Aliased expression e.g. `expr.alias`.
pub struct Alias {
    expr: Arc<Expr>,
    alias: String,
}

impl Alias {
    pub(crate) fn new(expr: Arc<Expr>, alias: String) -> Self {
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

////////////////////////////////////////////////////////////////////////////
//// Aggregation Expressions
////////////////////////////////////////////////////////////////////////////

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
