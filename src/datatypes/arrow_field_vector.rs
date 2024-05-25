use crate::datatypes::arrow_types::ArrowTypes;
use crate::datatypes::column_vector::ColumnVector;
use arrow::array::{
    Array, ArrayAccessor, ArrayBuilder, BooleanArray, BooleanBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, Int8Array, Int8Builder, StringArray, StringBuilder, UInt16Array, UInt16Builder,
    UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::DataType;
use std::any::Any;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

pub struct FieldVectorFactory;

impl FieldVectorFactory {
    fn create(arrow_type: DataType, initial_capacity: usize) -> Arc<dyn Array> {
        match arrow_type {
            DataType::Boolean => Arc::new(BooleanBuilder::with_capacity(initial_capacity).finish()),
            DataType::Int8 => Arc::new(Int8Builder::with_capacity(initial_capacity).finish()),
            DataType::Int16 => Arc::new(Int16Builder::with_capacity(initial_capacity).finish()),
            DataType::Int32 => Arc::new(Int32Builder::with_capacity(initial_capacity).finish()),
            DataType::Int64 => Arc::new(Int64Builder::with_capacity(initial_capacity).finish()),
            DataType::UInt8 => Arc::new(UInt8Builder::with_capacity(initial_capacity).finish()),
            DataType::UInt16 => Arc::new(UInt16Builder::with_capacity(initial_capacity).finish()),
            DataType::UInt32 => Arc::new(UInt32Builder::with_capacity(initial_capacity).finish()),
            DataType::UInt64 => Arc::new(UInt64Builder::with_capacity(initial_capacity).finish()),
            DataType::Float32 => Arc::new(Float32Builder::with_capacity(initial_capacity).finish()),
            DataType::Float64 => Arc::new(Float64Builder::with_capacity(initial_capacity).finish()),
            DataType::Utf8 => {
                Arc::new(StringBuilder::with_capacity(initial_capacity, initial_capacity).finish())
            }
            _ => panic!("Unsupported data type"),
        }
    }
}

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
