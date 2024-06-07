use crate::datatypes::arrow_field_vector::ArrowFieldVector;
use crate::datatypes::column_vector::ColumnVector;
use arrow::array::*;
use std::any::Any;
use std::sync::{Arc, Mutex};

/// uses Builder instead of array in comparison to kquery, since this is more convenient in arrow for rust
pub struct ArrowVectorBuilder {
    arrow_array_builder: Box<dyn ArrayBuilder>,
}

impl ArrowVectorBuilder {
    pub fn new(array_builder: Box<dyn ArrayBuilder>) -> Self {
        Self {
            arrow_array_builder: array_builder,
        }
    }

    pub fn append(&mut self, value: Option<Box<dyn Any>>) {
        if let Some(string_builder) = self
            .arrow_array_builder
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
        } else if let Some(int8_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<Int8Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i8>() {
                    int8_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int8_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    int8_builder.append_value(value.parse().unwrap());
                } else {
                    int8_builder.append_null();
                }
            } else {
                int8_builder.append_null();
            }
        } else if let Some(int16_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<Int16Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i16>() {
                    int16_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int16_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    int16_builder.append_value(value.parse().unwrap());
                } else {
                    int16_builder.append_null();
                }
            } else {
                int16_builder.append_null();
            }
        } else if let Some(int32_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i32>() {
                    int32_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int32_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    int32_builder.append_value(value.parse().unwrap());
                } else {
                    int32_builder.append_null();
                }
            } else {
                int32_builder.append_null();
            }
        } else if let Some(int64_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<i64>() {
                    int64_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    int64_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    int64_builder.append_value(value.parse().unwrap());
                } else {
                    int64_builder.append_null();
                }
            } else {
                int64_builder.append_null();
            }
        } else if let Some(uint8_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<UInt8Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<u8>() {
                    uint8_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    uint8_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    uint8_builder.append_value(value.parse().unwrap());
                } else {
                    uint8_builder.append_null();
                }
            } else {
                uint8_builder.append_null();
            }
        } else if let Some(uint16_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<UInt16Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<u16>() {
                    uint16_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    uint16_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    uint16_builder.append_value(value.parse().unwrap());
                } else {
                    uint16_builder.append_null();
                }
            } else {
                uint16_builder.append_null();
            }
        } else if let Some(uint32_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<UInt32Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<u32>() {
                    uint32_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    uint32_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    uint32_builder.append_value(value.parse().unwrap());
                } else {
                    uint32_builder.append_null();
                }
            } else {
                uint32_builder.append_null();
            }
        } else if let Some(uint64_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<u64>() {
                    uint64_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    uint64_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    uint64_builder.append_value(value.parse().unwrap());
                } else {
                    uint64_builder.append_null();
                }
            } else {
                uint64_builder.append_null();
            }
        } else if let Some(float32_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<f32>() {
                    float32_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    float32_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    float32_builder.append_value(value.parse().unwrap());
                } else {
                    float32_builder.append_null();
                }
            } else {
                float32_builder.append_null();
            }
        } else if let Some(float64_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<f64>() {
                    float64_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    float64_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    float64_builder.append_value(value.parse().unwrap());
                } else {
                    float64_builder.append_null();
                }
            } else {
                float64_builder.append_null();
            }
        } else if let Some(bool_builder) = self
            .arrow_array_builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
        {
            if let Some(value) = value {
                if let Some(value) = value.downcast_ref::<bool>() {
                    bool_builder.append_value(*value);
                } else if let Some(value) = value.downcast_ref::<&str>() {
                    bool_builder.append_value(value.parse().unwrap());
                } else if let Some(value) = value.downcast_ref::<String>() {
                    bool_builder.append_value(value.parse().unwrap());
                } else {
                    bool_builder;
                }
            } else {
                bool_builder.append_null();
            }
        } else {
            panic!("unsupported type")
        }
    }

    // todo check if not better to just return ArrowFieldVector even if its handled different in kquery
    pub fn build(mut self) -> Arc<dyn ColumnVector> {
        Arc::new(ArrowFieldVector(Arc::new(Mutex::new(
            self.arrow_array_builder.finish(),
        ))))
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
    use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
    use arrow::datatypes::DataType;

    #[test]
    fn test_builder() {
        let field_vector_builder = ArrowArrayFactory::create(DataType::Int64, 5);
        let mut builder = ArrowVectorBuilder::new(field_vector_builder);

        builder.append(Some(Box::new(12)));
        builder.append(Some(Box::new(122)));
        builder.append(Some(Box::new("22")));

        let column_vector = builder.build();
        let binding = column_vector.get_value(2).unwrap();
        let third_value = *binding.downcast_ref::<i64>().unwrap();

        assert_eq!(third_value, 22);
    }
    // todo test all builder types
}
