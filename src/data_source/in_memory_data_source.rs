use crate::data_source::DataSource;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use std::sync::Arc;

pub struct InMemoryDataSource {
    pub schema: Arc<Schema>,
    pub data: Vec<RecordBatch>,
}

impl InMemoryDataSource {
    pub fn new(schema: Arc<Schema>, data: Vec<RecordBatch>) -> Self {
        InMemoryDataSource { schema, data }
    }
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(&self, projection: Vec<&str>) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let projection_indices: Vec<usize> = projection
            .iter()
            .map(|name| {
                self.schema
                    .fields
                    .iter()
                    .position(|n| n.name.eq(name))
                    .unwrap()
            })
            .collect();

        Box::new(self.data.iter().map(move |batch| RecordBatch {
            schema: self.schema.clone(),
            fields: projection_indices.iter().map(|i| batch.field(*i)).collect(),
        }))
    }
}
