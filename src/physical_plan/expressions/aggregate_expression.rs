use crate::datatypes::arrow_types::ArrowType;
use crate::physical_plan::expressions::{Accumulator, Expression};
use std::sync::{Arc, Mutex};

pub trait AggregateExpression: ToString {
    fn input_expression(&self) -> Arc<dyn Expression>;
    fn create_accumulator(&self, arrow_type: ArrowType) -> Arc<Mutex<dyn Accumulator>>;
}
