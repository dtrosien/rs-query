use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::physical_plan::expressions::Expression;
use crate::physical_plan::PhysicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct ProjectionExec {
    input: Arc<dyn PhysicalPlan>,
    schema: Arc<Schema>,
    expr: Vec<Arc<dyn Expression>>,
}

impl Display for ProjectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expr_str = self // todo display can be implemented for  Vec<Arc<dyn Expression>> and then used universal
            .expr
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "ProjectionExec: {}", expr_str)
    }
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        Box::new(self.input.execute().map(|batch| {
            let columns: Vec<Arc<dyn ColumnVector>> =
                self.expr.iter().map(|e| e.evaluate(&batch)).collect();
            RecordBatch {
                schema: self.schema.clone(),
                fields: columns,
            }
        }))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}
