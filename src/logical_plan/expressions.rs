use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::schema::Field;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use anyhow::anyhow;
use std::fmt;
use std::fmt::{Display, Formatter};
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

impl AggregationExpr for Column {}

impl MathExpr for Column {}

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

impl MathExpr for ColumnIndex {}
impl AggregationExpr for ColumnIndex {}

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
    i: i64,
}

/// Convenience method to create a LiteralLong
pub fn lit_long(value: i64) -> LiteralLong {
    LiteralLong { i: value }
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

impl MathExpr for LiteralLong {}
impl AggregationExpr for LiteralLong {}

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

impl MathExpr for LiteralFloat {}
impl AggregationExpr for LiteralFloat {}

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

impl MathExpr for LiteralDouble {}
impl AggregationExpr for LiteralDouble {}

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

// todo maybe use static dispatch (match case) instead of composition pattern to convert nested inheritance hierarchies
pub struct BinaryExprBase {
    name: String,
    op: String,
    l: Arc<dyn LogicalExpr>,
    r: Arc<dyn LogicalExpr>,
}

impl BinaryExprBase {
    fn new(name: String, op: String, l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
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
    expr: Arc<dyn LogicalExpr>,
}

impl UnaryExprBase {
    fn new(name: String, op: String, expr: Arc<dyn LogicalExpr>) -> Self {
        Self { name, op, expr }
    }
}

impl Display for UnaryExprBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.op, self.expr.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////

struct Not {
    base: UnaryExprBase,
}

impl Not {
    fn new(expr: Arc<dyn LogicalExpr>) -> Self {
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
    fn new(name: String, op: String, l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
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

struct And {
    base: BooleanBinaryExpr,
}

impl And {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("and".to_string(), "AND".to_string(), l, r),
        }
    }
}

impl MathExpr for And {}
impl AggregationExpr for And {}

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

struct Or {
    base: BooleanBinaryExpr,
}

impl Or {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("or".to_string(), "OR".to_string(), l, r),
        }
    }
}

impl MathExpr for Or {}
impl AggregationExpr for Or {}

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

struct Eq {
    base: BooleanBinaryExpr,
}

impl Eq {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("eq".to_string(), "=".to_string(), l, r),
        }
    }
}

impl MathExpr for Eq {}
impl AggregationExpr for Eq {}

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

struct Neq {
    base: BooleanBinaryExpr,
}

impl Neq {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("neq".to_string(), "!=".to_string(), l, r),
        }
    }
}

impl MathExpr for Neq {}
impl AggregationExpr for Neq {}

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

struct Gt {
    base: BooleanBinaryExpr,
}

impl Gt {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("gt".to_string(), ">".to_string(), l, r),
        }
    }
}

impl MathExpr for Gt {}
impl AggregationExpr for Gt {}

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

struct GtEq {
    base: BooleanBinaryExpr,
}

impl GtEq {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("gteq".to_string(), ">=".to_string(), l, r),
        }
    }
}

impl MathExpr for GtEq {}
impl AggregationExpr for GtEq {}

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

struct Lt {
    base: BooleanBinaryExpr,
}

impl Lt {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("lt".to_string(), "<".to_string(), l, r),
        }
    }
}

impl MathExpr for Lt {}
impl AggregationExpr for Lt {}

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

struct LtEq {
    base: BooleanBinaryExpr,
}

impl LtEq {
    fn new(l: Arc<dyn LogicalExpr>, r: Arc<dyn LogicalExpr>) -> Self {
        Self {
            base: BooleanBinaryExpr::new("lteq".to_string(), "<=".to_string(), l, r),
        }
    }
}

impl MathExpr for LtEq {}
impl AggregationExpr for LtEq {}

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

/// for quicker access
trait BooleanBinaryExprExt {
    fn eq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr>;
    fn neq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr>;
    fn gt(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr>;
    fn gteq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr>;
    fn lt(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr>;
    fn lteq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr>;
}

impl<T> BooleanBinaryExprExt for T
where
    T: LogicalExpr + 'static,
{
    fn eq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Eq::new(self, rhs))
    }

    fn neq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Neq::new(self, rhs))
    }

    fn gt(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Gt::new(self, rhs))
    }

    fn gteq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(GtEq::new(self, rhs))
    }

    fn lt(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Lt::new(self, rhs))
    }

    fn lteq(self: Arc<Self>, rhs: Arc<dyn LogicalExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(LtEq::new(self, rhs))
    }
}

////////////////////////////////////////////////////////////////////////////
//// Math Expressions
////////////////////////////////////////////////////////////////////////////
/// marker trait to for math expressions (enhances logical expressions)
trait MathExpr: LogicalExpr {}

struct MathExprBase {
    name: String,
    op: String,
    l: Arc<dyn MathExpr>,
    r: Arc<dyn MathExpr>,
}

impl MathExprBase {
    fn new(name: String, op: String, l: Arc<dyn MathExpr>, r: Arc<dyn MathExpr>) -> Self {
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

struct Add {
    base: MathExprBase,
}

impl Add {
    fn new(l: Arc<dyn MathExpr>, r: Arc<dyn MathExpr>) -> Self {
        Self {
            base: MathExprBase::new("add".to_string(), "+".to_string(), l, r),
        }
    }
}

impl MathExpr for Add {}
impl AggregationExpr for Add {}

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

struct Subtract {
    base: MathExprBase,
}

impl Subtract {
    fn new(l: Arc<dyn MathExpr>, r: Arc<dyn MathExpr>) -> Self {
        Self {
            base: MathExprBase::new("subtract".to_string(), "-".to_string(), l, r),
        }
    }
}

impl MathExpr for Subtract {}
impl AggregationExpr for Subtract {}

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

struct Multiply {
    base: MathExprBase,
}

impl Multiply {
    fn new(l: Arc<dyn MathExpr>, r: Arc<dyn MathExpr>) -> Self {
        Self {
            base: MathExprBase::new("mult".to_string(), "*".to_string(), l, r),
        }
    }
}

impl MathExpr for Multiply {}
impl AggregationExpr for Multiply {}

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

struct Divide {
    base: MathExprBase,
}

impl Divide {
    fn new(l: Arc<dyn MathExpr>, r: Arc<dyn MathExpr>) -> Self {
        Self {
            base: MathExprBase::new("div".to_string(), "/".to_string(), l, r),
        }
    }
}
impl MathExpr for Divide {}
impl AggregationExpr for Divide {}

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

struct Modulus {
    base: MathExprBase,
}

impl Modulus {
    fn new(l: Arc<dyn MathExpr>, r: Arc<dyn MathExpr>) -> Self {
        Self {
            base: MathExprBase::new("mod".to_string(), "%".to_string(), l, r),
        }
    }
}

impl MathExpr for Modulus {}
impl AggregationExpr for Modulus {}

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

/// for quicker access
trait MathExprExt {
    fn add(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr>;
    fn subtract(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr>;
    fn mult(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr>;
    fn div(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr>;
    fn modulus(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr>;
}

impl<T> MathExprExt for T
where
    T: MathExpr + 'static,
{
    fn add(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Add::new(self, rhs))
    }

    fn subtract(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Subtract::new(self, rhs))
    }

    fn mult(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Multiply::new(self, rhs))
    }

    fn div(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Divide::new(self, rhs))
    }

    fn modulus(self: Arc<Self>, rhs: Arc<dyn MathExpr>) -> Arc<dyn LogicalExpr> {
        Arc::new(Modulus::new(self, rhs))
    }
}

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

impl MathExpr for Alias {}
impl AggregationExpr for Alias {}

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

////////////////////////////////////////////////////////////////////////////
//// Aggregation Expressions
////////////////////////////////////////////////////////////////////////////

trait AggregationExpr: LogicalExpr {} // todo !!!!! maybe logicalExpr must implement Aggreagt and Math ... so other way arround, to get rid of the multiple impls and to be able to chain properly

struct AggregationExprBase {
    name: String,
    expr: Arc<dyn AggregationExpr>,
}

impl AggregationExprBase {
    fn new(name: String, expr: Arc<dyn AggregationExpr>) -> Self {
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

impl MathExpr for AggregationExprBase {}

impl Display for AggregationExprBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name, self.expr)
    }
}

////////////////////////////////////////////////////////////////////////////

struct Max {
    base: AggregationExprBase,
}

/// Convenience method to create a max reference
pub fn max(expr: Arc<dyn AggregationExpr>) -> Arc<dyn AggregationExpr> {
    // todo fix
    Arc::from(Max::new(expr))
}

impl Max {
    fn new(input: Arc<dyn AggregationExpr>) -> Self {
        Self {
            base: AggregationExprBase::new("MAX".to_string(), input),
        }
    }
}

impl AggregationExpr for Max {}
impl MathExpr for Max {}
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

struct Min {
    base: AggregationExprBase,
}

impl Min {
    fn new(input: Arc<dyn AggregationExpr>) -> Self {
        Self {
            base: AggregationExprBase::new("MIN".to_string(), input),
        }
    }
}

impl MathExpr for Min {}
impl AggregationExpr for Min {}

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

struct Sum {
    base: AggregationExprBase,
}

impl Sum {
    fn new(input: Arc<dyn AggregationExpr>) -> Self {
        Self {
            base: AggregationExprBase::new("SUM".to_string(), input),
        }
    }
}

impl MathExpr for Sum {}
impl AggregationExpr for Sum {}

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

struct Avg {
    base: AggregationExprBase,
}

impl Avg {
    fn new(input: Arc<dyn AggregationExpr>) -> Self {
        Self {
            base: AggregationExprBase::new("AVG".to_string(), input),
        }
    }
}

impl MathExpr for Avg {}
impl AggregationExpr for Avg {}

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

struct Count {
    base: AggregationExprBase,
}

impl Count {
    fn new(input: Arc<dyn AggregationExpr>) -> Self {
        Self {
            base: AggregationExprBase::new("COUNT".to_string(), input),
        }
    }
}

impl AggregationExpr for Count {}
impl MathExpr for Count {}
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

struct CountDistinct {
    base: AggregationExprBase,
}

impl CountDistinct {
    fn new(input: Arc<dyn AggregationExpr>) -> Self {
        Self {
            base: AggregationExprBase::new("COUNT DISTINCT".to_string(), input),
        }
    }
}
impl MathExpr for CountDistinct {}
impl AggregationExpr for CountDistinct {}

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

#[cfg(test)]
mod test {
    use crate::logical_plan::expressions::{
        col, lit_long, max, BooleanBinaryExprExt, MathExprExt, Max, Min,
    };
    use std::sync::Arc;

    #[test]
    fn display_math_expressions_test() {
        let lit1 = Arc::new(lit_long(5));
        let lit2 = Arc::new(lit_long(10));

        let add_result = lit1.clone().add(lit2.clone());
        let sub_result = lit1.clone().subtract(lit2.clone());
        let div_result = lit1.clone().div(lit2.clone());
        let eq_result = lit1.clone().eq(lit2.clone());
        let gt_result = lit1.clone().gt(lit2.clone());
        let gteq_result = lit1.clone().gteq(lit2.clone());
        let neq_result = lit1.clone().neq(lit2.clone());
        let lt_result = lit1.clone().lt(lit2.clone());
        let lteq_result = lit1.clone().lteq(lit2.clone());
        let mult_result = lit1.clone().mult(lit2.clone());
        let mod_result = lit1.modulus(lit2);

        assert_eq!("5 + 10", add_result.to_string());
        assert_eq!("5 - 10", sub_result.to_string());
        assert_eq!("5 / 10", div_result.to_string());
        assert_eq!("5 = 10", eq_result.to_string());
        assert_eq!("5 > 10", gt_result.to_string());
        assert_eq!("5 >= 10", gteq_result.to_string());
        assert_eq!("5 != 10", neq_result.to_string());
        assert_eq!("5 < 10", lt_result.to_string());
        assert_eq!("5 <= 10", lteq_result.to_string());
        assert_eq!("5 * 10", mult_result.to_string());
        assert_eq!("5 % 10", mod_result.to_string());
    }

    #[test]
    fn col_expression_test() {
        let col1 = Arc::new(col("COL_1"));
        let col2 = Arc::new(col("COL_2"));

        let col_result = col1.clone().eq(col2.clone());

        let max = Arc::from(Max::new(col1));
        let col_result2 = max.div(Arc::from(Min::new(col2)));

        assert_eq!("COL_1 = COL_2", col_result.to_string());
        assert_eq!("MAX(COL_1) / MIN(COL_2)", col_result2.to_string())
    }
}
