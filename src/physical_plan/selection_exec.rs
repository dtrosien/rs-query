use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::physical_plan::expressions::Expression;
use crate::physical_plan::PhysicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct SelectionExec {
    input: Arc<dyn PhysicalPlan>,
    expr: Arc<dyn Expression>,
}

impl Display for SelectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SelectionExec: {}", self.expr.to_string())
    }
}

impl PhysicalPlan for SelectionExec {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}
