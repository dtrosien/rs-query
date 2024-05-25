use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::schema::Schema;
use arrow::array::Array;
use std::fmt::Display;
use std::sync::Arc;

struct RecordBatch {
    schema: Schema,
    fields: Vec<Arc<dyn ColumnVector>>,
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

    // pub fn to_csv(&self) -> String {
    //     let mut csv = String::new();
    //     let mut headers = Vec::new();
    //     for i in 0..self.column_count() {
    //         headers.push(self.schema.fields[i].name.clone());
    //     }
    //     csv.push_str(&headers.join(","));
    //     csv.push_str("\n");
    //     for i in 0..self.row_count() {
    //         let mut row = Vec::new();
    //         for j in 0..self.column_count() {
    //             let value = self.field(j).get_value(i as u64).unwrap();
    //             row.push(value);
    //         }
    //         csv.push_str(&row.join(","));
    //         csv.push_str("\n");
    //     }
    //     csv
    // }
}

// impl Display for RecordBatch {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}", self.to_csv())
//     }
// }
