use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::schema::Schema;
use anyhow::anyhow;
use arrow::array::Array;
use arrow::datatypes::DataType;
use std::fmt::Display;
use std::sync::Arc;

pub struct RecordBatch {
    pub(crate) schema: Arc<Schema>,
    pub(crate) fields: Vec<Arc<dyn ColumnVector>>,
}

impl RecordBatch {
    pub fn row_count(&self) -> usize {
        self.fields.first().unwrap().size() as usize
    }

    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    pub fn field(&self, i: usize) -> Arc<dyn ColumnVector> {
        self.fields[i].clone()
    }

    pub fn to_csv(&self) -> Option<String> {
        let mut csv = String::new();
        let mut headers = Vec::new();
        for i in 0..self.column_count() {
            headers.push(self.schema.fields[i].name.clone());
        }
        csv.push_str(&headers.join(","));
        csv.push_str("\n");
        for i in 0..self.row_count() {
            let mut row = Vec::new();
            for j in 0..self.column_count() {
                let data_type = self.schema.fields.get(j)?.data_type.clone();
                let any_value = self.field(j).get_value(i)?;
                match data_type {
                    ArrowType::BooleanType => {
                        row.push(any_value.downcast_ref::<bool>()?.to_string().to_owned());
                    }
                    ArrowType::Int8Type => {
                        row.push(any_value.downcast_ref::<i8>()?.to_string().to_owned());
                    }
                    ArrowType::Int16Type => {
                        row.push(any_value.downcast_ref::<i16>()?.to_string().to_owned());
                    }
                    ArrowType::Int32Type => {
                        row.push(any_value.downcast_ref::<i32>()?.to_string().to_owned());
                    }
                    ArrowType::Int64Type => {
                        row.push(any_value.downcast_ref::<i64>()?.to_string().to_owned());
                    }
                    ArrowType::UInt8Type => {
                        row.push(any_value.downcast_ref::<u8>()?.to_string().to_owned());
                    }
                    ArrowType::UInt16Type => {
                        row.push(any_value.downcast_ref::<u16>()?.to_string().to_owned());
                    }
                    ArrowType::UInt32Type => {
                        row.push(any_value.downcast_ref::<u32>()?.to_string().to_owned());
                    }
                    ArrowType::UInt64Type => {
                        row.push(any_value.downcast_ref::<u64>()?.to_string().to_owned());
                    }
                    ArrowType::FloatType => {
                        row.push(any_value.downcast_ref::<f32>()?.to_string().to_owned());
                    }
                    ArrowType::DoubleType => {
                        row.push(any_value.downcast_ref::<f64>()?.to_string().to_owned());
                    }
                    ArrowType::StringType => {
                        row.push(any_value.downcast_ref::<String>()?.to_owned());
                    }
                    _ => {
                        anyhow!("type not supported: {:?}", data_type);
                    }
                }
            }
            csv.push_str(&row.join(","));
            csv.push_str("\n");
        }
        Some(csv)
    }
}

impl Display for RecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_csv().unwrap_or_default())
    }
}
