use crate::datatypes::column_vector::ColumnVector;
use std::any::Any;
use std::sync::Arc;

struct LiteralValueVector {
    arrow_type: arrow::datatypes::DataType,
    value: Option<Arc<dyn Any>>,
    size: usize,
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> arrow::datatypes::DataType {
        self.arrow_type.clone()
    }

    fn get_value(&self, i: usize) -> Option<Arc<dyn Any>> {
        match i > self.size {
            true => None,
            false => self.value.clone(),
        }
    }

    fn size(&self) -> usize {
        return self.size;
    }
}
