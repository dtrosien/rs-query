use crate::datatypes::column_vector::ColumnVector;
use std::any::Any;

struct LiteralValueVector {
    arrow_type: arrow::datatypes::DataType,
    value: Option<Box<dyn Any>>,
    size: u64,
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> arrow::datatypes::DataType {
        self.arrow_type.clone()
    }

    fn get_value(&self, i: u64) -> Option<&Box<dyn Any>> {
        match i > self.size {
            true => None,
            false => self.value.as_ref(),
        }
    }

    fn size(&self) -> u64 {
        return self.size;
    }
}
