use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::schema::Schema;
use anyhow::anyhow;
use arrow::array::Array;
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

pub struct RecordBatch {
    pub schema: Arc<Schema>,
    pub fields: Vec<Arc<dyn ColumnVector>>,
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

    /// prints without headers for testing purposes
    pub fn to_csv(&self) -> Option<String> {
        let mut csv = String::new();
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

    pub fn value_to_string(any_value: Arc<dyn Any>, data_type: &ArrowType) -> Option<String> {
        match data_type {
            ArrowType::BooleanType => any_value.downcast_ref::<bool>().map(|v| v.to_string()),
            ArrowType::Int8Type => any_value.downcast_ref::<i8>().map(|v| v.to_string()),
            ArrowType::Int16Type => any_value.downcast_ref::<i16>().map(|v| v.to_string()),
            ArrowType::Int32Type => any_value.downcast_ref::<i32>().map(|v| v.to_string()),
            ArrowType::Int64Type => any_value.downcast_ref::<i64>().map(|v| v.to_string()),
            ArrowType::UInt8Type => any_value.downcast_ref::<u8>().map(|v| v.to_string()),
            ArrowType::UInt16Type => any_value.downcast_ref::<u16>().map(|v| v.to_string()),
            ArrowType::UInt32Type => any_value.downcast_ref::<u32>().map(|v| v.to_string()),
            ArrowType::UInt64Type => any_value.downcast_ref::<u64>().map(|v| v.to_string()),
            ArrowType::FloatType => any_value.downcast_ref::<f32>().map(|v| v.to_string()),
            ArrowType::DoubleType => any_value.downcast_ref::<f64>().map(|v| v.to_string()),
            ArrowType::StringType => any_value.downcast_ref::<String>().map(|v| v.clone()),
            _ => None,
        }
    }

    /// prints formated with headers
    pub fn show(&self) -> Option<String> {
        let mut csv = String::new();
        let mut headers = Vec::new();
        let mut max_lengths = Vec::new();

        // Collect header names and calculate max lengths
        for i in 0..self.column_count() {
            let header = self.schema.fields[i].name.clone();
            max_lengths.push(header.len());
            headers.push(header);
        }

        // Determine maximum length for each column by inspecting the data
        for i in 0..self.row_count() {
            for j in 0..self.column_count() {
                let data_type = self.schema.fields.get(j)?.data_type.clone();
                let any_value = self.field(j).get_value(i)?;
                if let Some(value_str) = Self::value_to_string(any_value, &data_type) {
                    if value_str.len() > max_lengths[j] {
                        max_lengths[j] = value_str.len();
                    }
                }
            }
        }

        // Create header row
        let header_row: Vec<String> = headers
            .iter()
            .enumerate()
            .map(|(i, header)| format!("{:width$}", header, width = max_lengths[i]))
            .collect();
        csv.push_str(&format!("|{}|\n", header_row.join("|")));

        // Create header underline row
        let header_underline: Vec<String> =
            max_lengths.iter().map(|&len| "-".repeat(len)).collect();
        csv.push_str(&format!("|{}|\n", header_underline.join("|")));

        // Create data rows
        for i in 0..self.row_count() {
            let mut row = Vec::new();
            for j in 0..self.column_count() {
                let data_type = self.schema.fields.get(j)?.data_type.clone();
                let any_value = self.field(j).get_value(i)?;
                if let Some(value_str) = Self::value_to_string(any_value, &data_type) {
                    row.push(format!("{:width$}", value_str, width = max_lengths[j]));
                } else {
                    row.push(format!("{:width$}", "", width = max_lengths[j]));
                }
            }
            csv.push_str(&format!("|{}|\n", row.join("|")));
        }

        Some(csv)
    }
}

impl Display for RecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.show().unwrap_or_default())
    }
}
