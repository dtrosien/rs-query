use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::physical_plan::expressions::Expression;
use std::fmt::Display;
use std::sync::Arc;

/// Reference column in a batch by index
#[derive(Debug)]
pub struct ColumnExpression {
    i: usize,
}

impl Display for ColumnExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.i)
    }
}

impl Expression for ColumnExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        input.field(self.i)
    }
}
