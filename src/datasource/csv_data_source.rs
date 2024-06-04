use crate::datasource::DataSource;
use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use arrow::array::ArrayBuilder;
use csv::{ReaderBuilder, StringRecord};
use std::any::Any;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub struct CsvDataSource {
    pub file_name: String,
    pub schema: Arc<Schema>,
    has_headers: bool,
    batch_size: usize,
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn scan(&self, projection: Vec<String>) -> impl Iterator<Item = RecordBatch> {
        let file = File::open(&self.file_name).unwrap();
        let reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(file);
        let r = CsvReader {
            schema: Arc::new(Schema { fields: vec![] }), // todo correct schema
            batch_size: self.batch_size,
            reader,
        };
        r.into_iter()
    }
}

struct CsvReader {
    schema: Arc<Schema>,
    batch_size: usize,
    reader: csv::Reader<File>,
}

impl IntoIterator for CsvReader {
    type Item = RecordBatch;
    type IntoIter = CsvReaderIterator;

    fn into_iter(self) -> Self::IntoIter {
        CsvReaderIterator {
            schema: self.schema,
            batch_size: self.batch_size,
            reader: self.reader.into_records(),
        }
    }
}

struct CsvReaderIterator {
    schema: Arc<Schema>,
    batch_size: usize,
    reader: csv::StringRecordsIntoIter<File>,
}

impl Iterator for CsvReaderIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut rows = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.reader.next() {
                Some(Ok(record)) => rows.push(record),
                _ => break,
            }
        }
        if rows.is_empty() {
            None
        } else {
            Some(self.create_batch(rows))
        }
    }
}

impl CsvReaderIterator {
    fn create_batch(&self, rows: Vec<StringRecord>) -> RecordBatch {
        let initial_capacity = rows.len();

        let mut fields: Vec<ArrowVectorBuilder> = self
            .schema
            .fields
            .iter()
            .map(|x| ArrowArrayFactory::create(x.data_type.clone(), initial_capacity))
            .map(|s| ArrowVectorBuilder::new(s))
            .collect();

        rows.iter().for_each(|row| {
            for (index, mut field) in fields.iter_mut().enumerate() {
                let value = row
                    .get(index)
                    .map(|str_value| Box::new(str_value.to_string()) as Box<dyn Any>);
                field.append(value)
            }
        });

        let fields: Vec<Arc<dyn ColumnVector>> =
            fields.into_iter().map(|field| field.build()).collect();

        RecordBatch {
            schema: self.schema.clone(),
            fields,
        }
    }
}

// todo tests
