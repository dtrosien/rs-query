use crate::datatypes::schema::Schema;
use arrow::ipc::RecordBatch;
use std::sync::Arc;

trait DataSource<'a> {
    /// Return the schema for the underlying data source
    fn schema() -> Schema;

    /// Scan the data source, selecting the specified columns
    fn scan(projection: Vec<String>) -> Vec<RecordBatch<'a>>;
}
