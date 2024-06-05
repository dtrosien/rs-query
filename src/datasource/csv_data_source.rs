use crate::datasource::DataSource;
use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::{Field, Schema};
use arrow::array::ArrayBuilder;
use arrow::datatypes::DataType;
use csv::{Reader, ReaderBuilder, StringRecord, Terminator};
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

    fn scan(&self, projection: Vec<&str>) -> impl Iterator<Item = RecordBatch> {
        info!("scan() projection={}", projection.concat());

        let file = Self::open_file(&self.file_name);
        let reader = Self::get_default_reader(self.has_headers, file);

        let read_schema = if projection.is_empty() {
            self.schema.clone()
        } else {
            Arc::from(self.schema.select(projection).expect("TODO: panic message"))
        };

        // todo TRANSLATE
        //    val settings = defaultSettings()
        //     if (projection.isNotEmpty()) {
        //       settings.selectFields(*projection.toTypedArray())
        //     }
        //     settings.isHeaderExtractionEnabled = hasHeaders
        //     if (!hasHeaders) {
        //       settings.setHeaders(*readSchema.fields.map { it.name }.toTypedArray())
        //     }

        let r = CsvReader {
            file_schema: self.schema.clone(),
            read_schema,
            batch_size: self.batch_size,
            reader,
        };
        r.into_iter()
    }
}

impl CsvDataSource {
    pub fn new(
        file_name: String,
        schema: Option<Arc<Schema>>,
        has_headers: bool,
        batch_size: usize,
    ) -> Self {
        let schema = schema
            .unwrap_or_else(|| Arc::from(Self::infer_schema(has_headers, &file_name).unwrap()));
        CsvDataSource {
            file_name,
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
            .from_reader(file)
    }

    fn open_file(file_name: &String) -> File {
        File::open(file_name).expect(format!("file not found: {}", file_name).as_str())
    }

    fn infer_schema(has_headers: bool, file_name: &String) -> anyhow::Result<Schema> {
        let file = Self::open_file(file_name);
        let mut reader = Self::get_default_reader(has_headers, file);

        let headers = reader.headers()?;
        if has_headers {
            let fields = headers
                .iter()
                .map(|column_name| Field {
                    name: column_name.to_string(),
                    data_type: DataType::Utf8,
                })
                .collect();
            Ok(Schema { fields })
        } else {
            let fields = headers
                .iter()
                .enumerate()
                .map(|(i, _)| Field {
                    name: format!("field_{i}"),
                    data_type: DataType::Utf8,
                })
                .collect();
            Ok(Schema { fields })
        }
    }
}

struct CsvReader {
    file_schema: Arc<Schema>,
    read_schema: Arc<Schema>,
    batch_size: usize,
    reader: Reader<File>,
}

impl IntoIterator for CsvReader {
    type Item = RecordBatch;
    type IntoIter = CsvReaderIterator;

    fn into_iter(self) -> Self::IntoIter {
        CsvReaderIterator {
            file_schema: self.file_schema,
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
            .read_schema
            .fields
            .iter()
            .map(|x| ArrowArrayFactory::create(x.data_type.clone(), initial_capacity))
            .map(|s| ArrowVectorBuilder::new(s))
            .collect();

        rows.iter().for_each(|row| {
            for (index, field) in fields.iter_mut().enumerate() {
                let value = row
                    .get(index)
                    .map(|str_value| Box::new(str_value.to_string()) as Box<dyn Any>);
                field.append(value)
            }
        });

        let fields: Vec<Arc<dyn ColumnVector>> =
            fields.into_iter().map(|field| field.build()).collect();

        RecordBatch {
            schema: self.read_schema.clone(),
            fields,
        }
    }
}

// todo tests
