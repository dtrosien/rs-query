use crate::datatypes::column_vector::ColumnVector;
use arrow::array::{
    Array, ArrayAccessor, ArrayBuilder, BooleanArray, BooleanBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, Int8Array, Int8Builder, StringArray, StringBuilder, UInt16Array, UInt16Builder,
    UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::DataType;
use std::any::Any;
use std::sync::{Arc, Mutex};

/// aka FieldVectorFactory in kquery
pub struct ArrowArrayFactory;

impl ArrowArrayFactory {
    pub fn create(arrow_type: DataType, initial_capacity: usize) -> Box<dyn ArrayBuilder> {
        match arrow_type {
            DataType::Boolean => Box::new(BooleanBuilder::with_capacity(initial_capacity)),
            DataType::Int8 => Box::new(Int8Builder::with_capacity(initial_capacity)),
            DataType::Int16 => Box::new(Int16Builder::with_capacity(initial_capacity)),
            DataType::Int32 => Box::new(Int32Builder::with_capacity(initial_capacity)),
            DataType::Int64 => Box::new(Int64Builder::with_capacity(initial_capacity)),
            DataType::UInt8 => Box::new(UInt8Builder::with_capacity(initial_capacity)),
            DataType::UInt16 => Box::new(UInt16Builder::with_capacity(initial_capacity)),
            DataType::UInt32 => Box::new(UInt32Builder::with_capacity(initial_capacity)),
            DataType::UInt64 => Box::new(UInt64Builder::with_capacity(initial_capacity)),
            DataType::Float32 => Box::new(Float32Builder::with_capacity(initial_capacity)),
            DataType::Float64 => Box::new(Float64Builder::with_capacity(initial_capacity)),
            DataType::Utf8 => Box::new(StringBuilder::with_capacity(
                initial_capacity,
                initial_capacity,
            )),
            _ => panic!("Unsupported data type"),
        }
    }
}

// todo test if impl instead of dyn can be used here
pub struct ArrowFieldVector(pub Arc<Mutex<dyn Array>>);

impl ColumnVector for ArrowFieldVector {
    fn get_type(&self) -> DataType {
        self.0.lock().unwrap().data_type().clone()
    }

    fn get_value(&self, i: usize) -> Option<Arc<dyn Any>> {
        if i >= self.0.lock().unwrap().len() {
            return None;
        }

        let guard = self.0.lock().unwrap();
        let value: Arc<dyn Any> = match guard.data_type() {
            DataType::Boolean => {
                let array = guard.as_any().downcast_ref::<BooleanArray>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Int8 => {
                let array = guard.as_any().downcast_ref::<Int8Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Int16 => {
                let array = guard.as_any().downcast_ref::<Int16Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Int32 => {
                let array = guard.as_any().downcast_ref::<Int32Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Int64 => {
                let array = guard.as_any().downcast_ref::<Int64Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::UInt8 => {
                let array = guard.as_any().downcast_ref::<UInt8Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::UInt16 => {
                let array = guard.as_any().downcast_ref::<UInt16Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::UInt32 => {
                let array = guard.as_any().downcast_ref::<UInt32Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::UInt64 => {
                let array = guard.as_any().downcast_ref::<UInt64Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Float32 => {
                let array = guard.as_any().downcast_ref::<Float32Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Float64 => {
                let array = guard.as_any().downcast_ref::<Float64Array>().unwrap();
                Arc::new(array.value(i))
            }
            DataType::Utf8 => {
                let array = guard.as_any().downcast_ref::<StringArray>().unwrap();
                Arc::new(array.value(i).to_string())
            }
            _ => panic!("Unsupported data type"),
        };

        Some(value)
    }

    fn size(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::arrow_field_vector::{ArrowArrayFactory, ArrowFieldVector};
    use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
    use crate::datatypes::column_vector::ColumnVector;
    use arrow::datatypes::DataType;
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_size() {
        let column_vector =
            create_test_i64_column_vector(vec![Box::new(12), Box::new(12), Box::new("12")]);

        let size = column_vector.lock().unwrap().size();

        assert_eq!(size, 3);
    }

    #[test]
    fn test_get_value() {
        let column_vector = create_test_i64_column_vector(vec![Box::new(12), Box::new("77")]);

        let binding = column_vector.lock().unwrap().get_value(1).unwrap();
        let third_value = *binding.downcast_ref::<i64>().unwrap();

        assert_eq!(third_value, 77);
    }

    #[test]
    fn test_get_type() {
        let column_vector = create_test_i64_column_vector(vec![Box::new(12)]);

        let data_type = column_vector.lock().unwrap().get_type();

        assert!(&data_type.equals_datatype(&DataType::Int64))
    }

    fn create_test_i64_column_vector(values: Vec<Box<dyn Any>>) -> Arc<Mutex<dyn ColumnVector>> {
        let field_vector_builder = ArrowArrayFactory::create(DataType::Int64, values.len());
        let mut builder = ArrowVectorBuilder::new(field_vector_builder);

        for v in values {
            builder.append(Some(v));
        }
        builder.build()
    }
}
