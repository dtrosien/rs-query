use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::physical_plan::expressions::Expression;
use std::any::Any;

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

pub struct CastExpression {
    pub expr: Arc<dyn Expression>,
    pub data_type: ArrowType,
}

impl Display for CastExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr.to_string(), self.data_type)
    }
}

impl Expression for CastExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        let value = self.expr.evaluate(input);
        let array =
            ArrowArrayFactory::create(self.data_type.clone().to_datatype(), input.row_count());
        let mut vector = ArrowVectorBuilder::new(array);

        match self.data_type {
            ArrowType::Int8Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_i8(v.deref())));
                }
            }
            ArrowType::Int16Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_i16(v.deref())));
                }
            }
            ArrowType::Int32Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_i32(v.deref())));
                }
            }
            ArrowType::Int64Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);

                    vector.append(v.and_then(|v| convert_to_i64(v.deref())));
                }
            }
            ArrowType::UInt8Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_u8(v.deref())));
                }
            }
            ArrowType::UInt16Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_u16(v.deref())));
                }
            }
            ArrowType::UInt32Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_u32(v.deref())));
                }
            }
            ArrowType::UInt64Type => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_u64(v.deref())));
                }
            }
            ArrowType::FloatType => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_f32(v.deref())));
                }
            }
            ArrowType::DoubleType => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    vector.append(v.and_then(|v| convert_to_f64(v.deref())));
                }
            }
            ArrowType::StringType => {
                for i in 0..value.size() {
                    let v = value.get_value(i);
                    let str_value = v.and_then(|v| {
                        if let Some(s) = v.downcast_ref::<String>() {
                            Some(Arc::new(s.clone()) as Arc<dyn Any>)
                        } else if let Some(bytes) = v.downcast_ref::<Vec<u8>>() {
                            String::from_utf8(bytes.clone())
                                .ok()
                                .map(|s| Arc::new(s) as Arc<dyn Any>)
                        } else {
                            None
                        }
                    });
                    vector.append(str_value);
                }
            }
            _ => panic!("Cast to {:?} is not supported", self.data_type),
        }
        vector.build()
    }
}

// todo remove tries for unfitting types (functions where generated)
fn convert_to_i8(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<i8>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        i8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else {
        None
    }
}

fn convert_to_i16(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<i16>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as i16) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        i16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        i16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        i16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        Some(Arc::new(num as i16) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        i16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        i16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else {
        None
    }
}

fn convert_to_i32(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<i32>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as i32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num as i32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        i32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        i32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        Some(Arc::new(num as i32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        Some(Arc::new(num as i32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        i32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else {
        None
    }
}

fn convert_to_i64(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<i64>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as i64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num as i64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        Some(Arc::new(num as i64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        i64::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        Some(Arc::new(num as i64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        Some(Arc::new(num as i64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        Some(Arc::new(num as i64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f32>() {
        if num >= 0.0 {
            Some(Arc::new(num as i64) as Arc<dyn Any>)
        } else {
            None
        }
    } else if let Some(&num) = v.downcast_ref::<f64>() {
        if num >= 0.0 {
            Some(Arc::new(num as i64) as Arc<dyn Any>)
        } else {
            None
        }
    } else {
        None
    }
}

fn convert_to_u8(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<u8>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        u8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        u8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        u8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        if num >= 0 {
            Some(Arc::new(num as u8) as Arc<dyn Any>)
        } else {
            None
        }
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        u8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        u8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        u8::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else {
        None
    }
}

fn convert_to_u16(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<u16>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as u16) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        u16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        u16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        if num >= 0 {
            Some(Arc::new(num as u16) as Arc<dyn Any>)
        } else {
            None
        }
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        u16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        u16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        u16::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else {
        None
    }
}

fn convert_to_u32(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<u32>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as u32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num as u32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        u32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        if num >= 0 {
            Some(Arc::new(num as u32) as Arc<dyn Any>)
        } else {
            None
        }
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        u32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        u32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        u32::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f32>() {
        if num >= 0.0 {
            Some(Arc::new(num as u32) as Arc<dyn Any>)
        } else {
            None
        }
    } else {
        None
    }
}

fn convert_to_u64(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<u64>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as u64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num as u64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        Some(Arc::new(num as u64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        if num >= 0 {
            Some(Arc::new(num as u64) as Arc<dyn Any>)
        } else {
            None
        }
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        u64::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        u64::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        u64::try_from(num).ok().map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f32>() {
        if num >= 0.0 {
            Some(Arc::new(num as u64) as Arc<dyn Any>)
        } else {
            None
        }
    } else if let Some(&num) = v.downcast_ref::<f64>() {
        if num >= 0.0 {
            Some(Arc::new(num as u64) as Arc<dyn Any>)
        } else {
            None
        }
    } else {
        None
    }
}

fn convert_to_f32(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<f32>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f32>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f64>() {
        Some(Arc::new(num as f32) as Arc<dyn Any>)
    } else {
        None
    }
}

fn convert_to_f64(v: &dyn Any) -> Option<Arc<dyn Any>> {
    if let Some(str_value) = v.downcast_ref::<String>() {
        str_value
            .parse::<f64>()
            .ok()
            .map(|i| Arc::new(i) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u8>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u16>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u32>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<u64>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i8>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i16>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i32>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<i64>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f32>() {
        Some(Arc::new(num as f64) as Arc<dyn Any>)
    } else if let Some(&num) = v.downcast_ref::<f64>() {
        Some(Arc::new(num) as Arc<dyn Any>)
    } else {
        None
    }
}
