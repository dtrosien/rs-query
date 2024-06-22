use crate::datasource::{DataSource, Source};
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::logical_plan::data_frame::DataFrame;
use crate::physical_plan::PhysicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct ScanExec {
    ds: Arc<Source>,
    projection: Vec<String>,
}

impl Display for ScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScanExec: schema={}, projection={:?}",
            self.ds.schema(),
            self.projection
        )
    }
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> Arc<Schema> {
        Arc::new(
            self.ds
                .schema()
                .select(self.projection.iter().map(String::as_str).collect())
                .unwrap(),
        )
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        self.ds
            .scan(self.projection.iter().map(String::as_str).collect()) // todo fix mismatch of String and str and remove conversions
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![]
    }
}
