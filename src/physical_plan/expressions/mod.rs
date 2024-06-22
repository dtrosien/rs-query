use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use std::sync::Arc;

pub trait Expression {
    /// Evaluate the expression against an input record batch and produce a column of data as output
    fn evaluate(input: Arc<RecordBatch>) -> Arc<dyn ColumnVector>;
}
