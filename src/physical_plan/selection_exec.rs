use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
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
        let input = self.input.execute();
        Box::new(input.map(move |batch| {
            let result = self.expr.evaluate(&batch);

            let schema = batch.schema.clone();
            let column_count = batch.schema.fields.len();

            let filtered_fields: Vec<Arc<dyn ColumnVector>> = (0..column_count)
                .map(|i| filter(batch.field(i), result.clone()))
                .collect();

            RecordBatch {
                schema,
                fields: filtered_fields,
            }
        }))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}
fn filter(v: Arc<dyn ColumnVector>, selection: Arc<dyn ColumnVector>) -> Arc<dyn ColumnVector> {
    let array = ArrowArrayFactory::create(v.get_type().to_datatype().clone(), selection.size());
    let mut filtered_vector = ArrowVectorBuilder::new(array);

    for i in 0..selection.size() {
        if selection
            .get_value(i)
            .and_then(|v| v.downcast_ref::<bool>().cloned())
            .unwrap_or(false)
        {
            filtered_vector.append(v.get_value(i));
        }
    }

    filtered_vector.build()
}
