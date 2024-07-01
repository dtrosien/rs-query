use crate::datatypes::arrow_types::ArrowType;
use crate::physical_plan::expressions::aggregate_expression::AggregateExpression;
use crate::physical_plan::expressions::{Accumulator, Expression};
use std::any::Any;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

pub struct SumExpression {
    pub expr: Arc<dyn Expression>,
}

impl Display for SumExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUM({})", self.expr.to_string())
    }
}

impl AggregateExpression for SumExpression {
    fn input_expression(&self) -> Arc<dyn Expression> {
        self.expr.clone()
    }

    fn create_accumulator(&self, arrow_type: ArrowType) -> Arc<Mutex<dyn Accumulator>> {
        let acc = SumAccumulator::new(arrow_type);
        Arc::new(Mutex::new(acc))
    }
}

pub struct SumAccumulator {
    value: Option<Arc<dyn Any>>,
    arrow_type: ArrowType,
}

impl SumAccumulator {
    fn new(arrow_type: ArrowType) -> Self {
        SumAccumulator {
            value: None,
            arrow_type,
        }
    }
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: Option<Arc<dyn Any>>) {
        if let Some(current_value) = self.value.clone() {
            match self.arrow_type {
                ArrowType::Int8Type => {
                    let c_val = current_value.downcast_ref::<i8>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<i8>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::Int16Type => {
                    let c_val = current_value.downcast_ref::<i16>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<i16>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::Int32Type => {
                    let c_val = current_value.downcast_ref::<i32>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<i32>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::Int64Type => {
                    let c_val = current_value.downcast_ref::<i64>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<i64>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::UInt8Type => {
                    let c_val = current_value.downcast_ref::<u8>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<u8>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::UInt16Type => {
                    let c_val = current_value.downcast_ref::<u16>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<u16>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::UInt32Type => {
                    let c_val = current_value.downcast_ref::<u32>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<u32>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::UInt64Type => {
                    let c_val = current_value.downcast_ref::<u64>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<u64>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::FloatType => {
                    let c_val = current_value.downcast_ref::<f32>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<f32>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                ArrowType::DoubleType => {
                    let c_val = current_value.downcast_ref::<f64>();
                    let val = value.and_then(|any_val| any_val.downcast_ref::<f64>().cloned());
                    c_val.zip(val).map(|(c_val, val)| {
                        self.value = Some(Arc::new(c_val + val) as Arc<dyn Any>)
                    });
                }
                _ => panic!(
                    "Unsupported data type in math expression: {:?}",
                    self.arrow_type
                ),
            };
        } else {
            self.value = value;
        }
    }

    fn final_value(&self) -> Option<Arc<dyn Any>> {
        self.value.clone()
    }
}
