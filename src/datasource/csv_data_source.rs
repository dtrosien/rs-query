use crate::datasource::DataSource;
use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::{Field, Schema};
use arrow::array::ArrayBuilder;
use arrow::datatypes::DataType;
use csv::{Reader, ReaderBuilder, StringRecord, Terminator, Trim};
use std::any::Any;
use std::fs::File;
use std::sync::Arc;
use tracing::info;

pub struct CsvDataSource {
    pub file_name: String,
    pub schema: Arc<Schema>,
    has_headers: bool,
    batch_size: usize,
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(&self, projection: Vec<&str>) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        info!("scan() projection={}", projection.concat());

        let file = Self::open_file(&self.file_name);
        let reader = Self::get_default_reader(self.has_headers, file);

        let read_schema = if projection.is_empty() {
            self.schema.clone()
        } else {
            Arc::from(self.schema.select(projection).expect("TODO: panic message"))
        };

        let r = CsvReader {
            full_schema: self.schema.clone(),
            read_schema,
            batch_size: self.batch_size,
            reader,
        };
        Box::new(r.into_iter())
    }
}

impl CsvDataSource {
    pub fn new(
        file_name: &str,
        schema: Option<Arc<Schema>>,
        has_headers: bool,
        batch_size: usize,
    ) -> Self {
        let schema = schema
            .unwrap_or_else(|| Arc::from(Self::infer_schema(has_headers, file_name).unwrap()));
        CsvDataSource {
            file_name: file_name.to_owned(),
            schema,
            has_headers,
            batch_size,
        }
    }

    fn get_default_reader(has_headers: bool, file: File) -> Reader<File> {
        ReaderBuilder::new()
            .has_headers(has_headers)
            .terminator(Terminator::CRLF)
            .delimiter(b',')
            .trim(Trim::All)
            .from_reader(file)
    }

    fn open_file(file_name: &str) -> File {
        File::open(file_name).expect(format!("file not found: {}", file_name).as_str())
    }

    fn infer_schema(has_headers: bool, file_name: &str) -> anyhow::Result<Schema> {
        let file = Self::open_file(file_name);
        let mut reader = Self::get_default_reader(has_headers, file);

        let headers = reader.headers()?;
        if has_headers {
            let fields = headers
                .iter()
                .map(|column_name| {
                    Arc::new(Field {
                        name: column_name.to_string(),
                        data_type: ArrowType::StringType,
                    })
                })
                .collect();
            Ok(Schema { fields })
        } else {
            let fields = headers
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    Arc::new(Field {
                        name: format!("field_{i}"),
                        data_type: ArrowType::StringType,
                    })
                })
                .collect();
            Ok(Schema { fields })
        }
    }
}

struct CsvReader {
    full_schema: Arc<Schema>,
    read_schema: Arc<Schema>,
    batch_size: usize,
    reader: Reader<File>,
}

impl IntoIterator for CsvReader {
    type Item = RecordBatch;
    type IntoIter = CsvReaderIterator;

    fn into_iter(self) -> Self::IntoIter {
        CsvReaderIterator {
            file_schema: self.full_schema,
            read_schema: self.read_schema,
            batch_size: self.batch_size,
            reader: self.reader.into_records(),
        }
    }
}

struct CsvReaderIterator {
    file_schema: Arc<Schema>,
    read_schema: Arc<Schema>,
    batch_size: usize,
    reader: csv::StringRecordsIntoIter<File>,
}

impl Iterator for CsvReaderIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut rows = Vec::with_capacity(self.batch_size);

        rows.extend(
            self.reader
                .by_ref()
                .take(self.batch_size)
                .filter_map(Result::ok),
        );

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

        let mut field_builders: Vec<(usize, ArrowVectorBuilder)> = self
            .read_schema
            .fields
            .iter()
            .map(|x| {
                let pos = self // todo move position finding outside of iterator to just do it once
                    .file_schema
                    .fields
                    .iter()
                    .position(|s| s.name == x.name)
                    .unwrap();
                let array = ArrowArrayFactory::create(x.data_type.to_datatype(), initial_capacity);
                let builder = ArrowVectorBuilder::new(array);
                (pos, builder)
            })
            .collect();

        rows.iter().for_each(|row| {
            for (index, field) in field_builders.iter_mut() {
                let value = row
                    .get(*index)
                    .map(|str_value| Box::new(str_value.to_string()) as Box<dyn Any>);
                field.append(value)
            }
        });

        let fields: Vec<Arc<dyn ColumnVector>> = field_builders
            .into_iter()
            .map(|(_, field)| field.build())
            .collect();

        RecordBatch {
            schema: self.read_schema.clone(),
            fields,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::datasource::{DataSource, Source};
    use crate::datatypes::arrow_types::ArrowType;
    use crate::datatypes::record_batch::RecordBatch;
    use crate::datatypes::schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_read_csv_with_header() {
        let ds = Source::from_csv("testdata/employee.csv", None, true, 1024);
        let result = ds.scan(vec![]).next().unwrap();

        assert_eq!(ds.schema().fields.len(), 6);
        assert_eq!(ds.schema().fields.get(3).unwrap().name, "state");
        assert_eq!(result.row_count(), 4);
        assert_eq!(result.fields.len(), 6);
    }

    #[test]
    fn test_read_csv_without_header() {
        let ds = Source::from_csv("testdata/employee_no_header.csv", None, false, 1024);
        let result = ds.scan(vec![]).next().unwrap();

        assert_eq!(ds.schema().fields.len(), 6);
        assert_eq!(ds.schema().fields.get(3).unwrap().name, "field_3");
        assert_eq!(result.row_count(), 4);
        assert_eq!(result.fields.len(), 6);
    }

    #[test]
    fn test_read_csv_projection_with_header() {
        let ds = Source::from_csv("testdata/employee.csv", None, true, 1024);
        let projection = vec!["id", "state", "salary"];
        let result = ds.scan(projection).next().unwrap();

        // schema must not be touched by projection
        assert_eq!(ds.schema().fields.len(), 6);
        assert_eq!(ds.schema().fields.get(3).unwrap().name, "state");
        assert_eq!(result.row_count(), 4);
        assert_eq!(result.fields.len(), 3);
        // actual read schema after projection
        assert_eq!(result.schema.fields.len(), 3);
        assert_eq!(result.schema.fields.get(1).unwrap().name, "state");
    }

    #[test]
    fn test_read_csv_projection_without_header() {
        let ds = Source::from_csv("testdata/employee_no_header.csv", None, false, 1024);
        let projection = vec!["field_0", "field_3", "field_5"];
        let result = ds.scan(projection).next().unwrap();

        // schema must not be touched by projection
        assert_eq!(ds.schema().fields.len(), 6);
        assert_eq!(ds.schema().fields.get(3).unwrap().name, "field_3");
        assert_eq!(result.row_count(), 4);
        assert_eq!(result.fields.len(), 3);
        // actual read schema after projection
        assert_eq!(result.schema.fields.len(), 3);
        assert_eq!(result.schema.fields.get(1).unwrap().name, "field_3");
    }

    #[test]
    fn test_read_csv_with_provided_schema() {
        let field0 = Field {
            name: "id".to_string(),
            data_type: ArrowType::UInt16Type,
        };
        let field1 = Field {
            name: "first_name".to_string(),
            data_type: ArrowType::StringType,
        };
        let field2 = Field {
            name: "last_name".to_string(),
            data_type: ArrowType::StringType,
        };
        let field3 = Field {
            name: "state".to_string(),
            data_type: ArrowType::StringType,
        };
        let field4 = Field {
            name: "job_title".to_string(),
            data_type: ArrowType::StringType,
        };
        let field5 = Field {
            name: "salary".to_string(),
            data_type: ArrowType::Int64Type,
        };
        let fields = vec![field0, field1, field2, field3, field4, field5]
            .into_iter()
            .map(|f| Arc::new(f))
            .collect();
        let schema = Schema { fields };
        let ds = Source::from_csv("testdata/employee.csv", Some(Arc::from(schema)), true, 1024);
        let result = ds.scan(vec![]).next().unwrap();

        assert_eq!(ds.schema().fields.len(), 6);
        assert_eq!(ds.schema().fields.get(3).unwrap().name, "state");
        assert_eq!(result.row_count(), 4);
        assert_eq!(result.fields.len(), 6);
        assert_eq!(
            result.schema.fields.get(5).unwrap().data_type,
            ArrowType::Int64Type
        );
        let binding1 = result.fields.get(0).unwrap().get_value(1).unwrap();
        let uint_val = binding1.downcast_ref::<u16>().unwrap();
        let binding2 = result.fields.get(5).unwrap().get_value(0).unwrap();
        let int_val = binding2.downcast_ref::<i64>().unwrap();

        assert_eq!(*uint_val, 2);
        assert_eq!(*int_val, 12000);
        //println!("{result}")
    }

    #[test]
    fn test_read_csv_with_small_batch_size() {
        let ds = Source::from_csv("testdata/employee.csv", None, true, 1);

        let batches: Vec<RecordBatch> = ds.scan(vec![]).collect();

        assert_eq!(batches.len(), 4);
        assert_eq!(batches.first().unwrap().row_count(), 1)
    }
}
