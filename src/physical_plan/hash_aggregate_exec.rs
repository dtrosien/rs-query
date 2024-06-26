use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::physical_plan::PhysicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct HashAggregateExec {}

impl Display for HashAggregateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", todo!())
    }
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        todo!()
    }
}
