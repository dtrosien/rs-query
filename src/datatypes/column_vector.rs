use std::any::Any;

pub trait ColumnVector {
    fn get_type(&self) -> arrow::datatypes::DataType;
    fn get_value(&self, i: u64) -> Option<&Box<dyn Any>>;
    fn size(&self) -> u64;
}
