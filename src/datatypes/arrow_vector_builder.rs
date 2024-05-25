use crate::datatypes::arrow_field_vector::ArrowFieldVector;
use crate::datatypes::column_vector::ColumnVector;
use arrow::array::*;
use std::any::Any;
use std::sync::{Arc, Mutex};

pub struct ArrowVectorBuilder {
    field_vector: Box<dyn ArrayBuilder>,
}

impl ArrowVectorBuilder {
    pub fn new(field_vector: Box<dyn ArrayBuilder>) -> Self {
        Self { field_vector }
    }

    fn append(&mut self, value: Option<Box<dyn Any>>) {
        if let Some(string_builder) = self
            .field_vector
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<String>() {
                    string_builder.append_value(value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    string_builder.append_value(value);
                } else {
                    string_builder.append_null();
                }
            } else {
                string_builder.append_null();
            }
        } else if let Some(int8_builder) =
            self.field_vector.as_any_mut().downcast_mut::<Int8Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i8>() {
                    int8_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int8_builder.append_value(value.parse().unwrap());
                } else {
                    int8_builder.append_null();
                }
            } else {
                int8_builder.append_null();
            }
        } else if let Some(int16_builder) = self
            .field_vector
            .as_any_mut()
            .downcast_mut::<Int16Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i16>() {
                    int16_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int16_builder.append_value(value.parse().unwrap());
                } else {
                    int16_builder.append_null();
                }
            } else {
                int16_builder.append_null();
            }
        } else if let Some(int32_builder) = self
            .field_vector
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i32>() {
                    int32_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int32_builder.append_value(value.parse().unwrap());
                } else {
                    int32_builder.append_null();
                }
            } else {
                int32_builder.append_null();
            }
        } else if let Some(int64_builder) = self
            .field_vector
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i64>() {
                    int64_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int64_builder.append_value(value.parse().unwrap());
                } else {
                    int64_builder.append_null();
                }
            } else {
                int64_builder.append_null();
            }
        } else if let Some(float32_builder) = self
            .field_vector
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<f32>() {
                    float32_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    float32_builder.append_value(value.parse().unwrap());
                } else {
                    float32_builder.append_null();
                }
            } else {
                float32_builder.append_null();
            }
        } else if let Some(float64_builder) = self
            .field_vector
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<f64>() {
                    float64_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    float64_builder.append_value(value.parse().unwrap());
                } else {
                    float64_builder.append_null();
                }
            } else {
                float64_builder.append_null();
            }
        }
    }

    pub fn build(mut self) -> Arc<Mutex<dyn ColumnVector>> {
        Arc::new(Mutex::new(ArrowFieldVector(Arc::new(Mutex::new(
            self.field_vector.finish(),
        )))))
    }
}
