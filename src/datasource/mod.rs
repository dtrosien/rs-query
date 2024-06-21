mod csv_data_source;
pub mod in_memory_data_source;

use crate::datasource::csv_data_source::CsvDataSource;
use crate::datasource::in_memory_data_source::InMemoryDataSource;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use std::sync::Arc;

// todo maybe use scan(&self, projection: Vec<&str>) -> Box<dyn Iterator<Item = RecordBatch>> and get rid of enum here (I dont think Arc<Source> is necessary in logical_plan ...)

pub trait DataSource {
    /// Return the schema for the underlying data source
    fn schema(&self) -> Arc<Schema>;

    /// Scan the data source, selecting the specified columns
    fn scan(&self, projection: Vec<&str>) -> impl Iterator<Item = RecordBatch>;
}

pub enum Source {
    CSV(CsvDataSource),
    InMemory(InMemoryDataSource),
}

impl Source {
    pub fn schema(&self) -> Arc<Schema> {
        match self {
            Source::CSV(s) => s.schema.clone(),
            Source::InMemory(s) => s.schema.clone(),
        }
    }

    pub fn scan<'a>(
        &'a self,
        projection: Vec<&'a str>,
    ) -> Box<dyn Iterator<Item = RecordBatch> + 'a> {
        match self {
            Source::CSV(s) => Box::new(s.scan(projection)),
            Source::InMemory(s) => Box::new(s.scan(projection)),
        }
    }

    // Associated function to create a CSV source
    pub fn from_csv(
        file_name: &str,
        schema: Option<Arc<Schema>>,
        has_headers: bool,
        batch_size: usize,
    ) -> Arc<Self> {
        let ds = CsvDataSource::new(file_name, schema, has_headers, batch_size);
        Arc::from(Source::CSV(ds))
    }

    // Associated function to create an InMemory source
    pub fn from_in_memory(schema: Arc<Schema>, data: Vec<RecordBatch>) -> Arc<Self> {
        let ds = InMemoryDataSource::new(schema, data);
        Arc::from(Source::InMemory(ds))
    }
}
