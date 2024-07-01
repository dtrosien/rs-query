pub mod aggregate_expression;
pub mod binary_expression;
pub mod boolean_expression;
pub mod cast_expression;
pub mod column_expression;
pub mod math_expression;
pub mod sum_expression;

use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::literal_value_vector::LiteralValueVector;
use crate::datatypes::record_batch::RecordBatch;
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

pub trait Expression: ToString {
    /// Evaluate the expression against an input record batch and produce a column of data as output
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector>;
}

pub trait Accumulator {
    fn accumulate(&mut self, value: Option<Arc<dyn Any>>);
    fn final_value(&self) -> Option<Arc<dyn Any>>;
}

pub struct LiteralLongExpression {
    pub value: i64,
}

impl Display for LiteralLongExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Expression for LiteralLongExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        Arc::new(LiteralValueVector {
            arrow_type: ArrowType::Int64Type,
            value: Some(Arc::new(self.value)),
            size: input.row_count(),
        })
    }
}

pub struct LiteralFloatExpression {
    pub value: f32,
}

impl Display for LiteralFloatExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Expression for LiteralFloatExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        Arc::new(LiteralValueVector {
            arrow_type: ArrowType::FloatType,
            value: Some(Arc::new(self.value)),
            size: input.row_count(),
        })
    }
}

pub struct LiteralDoubleExpression {
    pub value: f64,
}

impl Display for LiteralDoubleExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Expression for LiteralDoubleExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        Arc::new(LiteralValueVector {
            arrow_type: ArrowType::DoubleType,
            value: Some(Arc::new(self.value)),
            size: input.row_count(),
        })
    }
}
pub struct LiteralStringExpression {
    pub value: String,
}

impl Display for LiteralStringExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Expression for LiteralStringExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        Arc::new(LiteralValueVector {
            arrow_type: ArrowType::StringType,
            value: Some(Arc::new(self.value.clone())),
            size: input.row_count(),
        })
    }
}
