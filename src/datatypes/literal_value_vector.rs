use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::column_vector::ColumnVector;
use std::any::Any;
use std::sync::Arc;

pub struct LiteralValueVector {
    pub arrow_type: ArrowType,
    pub value: Option<Arc<dyn Any>>,
    pub size: usize,
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> ArrowType {
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
