mod csv_data_source;
pub mod in_memory_data_source;

use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use std::sync::Arc;

trait DataSource {
    /// Return the schema for the underlying data source
    fn schema(&self) -> Arc<Schema>;

    /// Scan the data source, selecting the specified columns
    fn scan(&self, projection: Vec<&str>) -> impl Iterator<Item = RecordBatch>;
}
