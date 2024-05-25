use arrow::datatypes::DataType;
use std::any::Any;
use std::sync::Arc;

pub trait ColumnVector {
    fn get_type(&self) -> DataType;
    fn get_value(&self, i: usize) -> Option<Arc<dyn Any>>;
    fn size(&self) -> usize;
}
