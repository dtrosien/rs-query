use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::physical_plan::expressions::Expression;
use std::sync::Arc;

pub trait BinaryExpression: Expression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        todo!()
    }
}
