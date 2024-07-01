use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::physical_plan::expressions::aggregate_expression::AggregateExpression;
use crate::physical_plan::expressions::{Accumulator, Expression};
use crate::physical_plan::PhysicalPlan;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

pub struct HashAggregateExec {
    pub input: Arc<dyn PhysicalPlan>,
    pub group_expr: Vec<Arc<dyn Expression>>,
    pub aggregate_expr: Vec<Arc<dyn AggregateExpression>>,
    pub schema: Arc<Schema>,
}

impl Display for HashAggregateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HashAggregateExec: groupExpr={}, aggrExpr={}",
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            self.aggregate_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        // todo use impl Hash or Eq instead of just string
        let mut map: HashMap<Vec<Option<String>>, Vec<Arc<Mutex<dyn Accumulator>>>> =
            HashMap::new();

        self.input.execute().for_each(|batch| {
            let group_keys: Vec<Arc<dyn ColumnVector>> =
                self.group_expr.iter().map(|e| e.evaluate(&batch)).collect();

            let aggr_input_values: Vec<Arc<dyn ColumnVector>> = self
                .aggregate_expr
                .iter()
                .map(|e| e.input_expression().evaluate(&batch))
                .collect();

            for row_index in 0..batch.row_count() {
                let row_key: Vec<Option<String>> = group_keys
                    .iter()
                    .map(|k| {
                        if let (Some(v), t) = (k.get_value(row_index), k.get_type()) {
                            RecordBatch::value_to_string(v, &t) // todo use impl Hash or Eq instead of just string
                        } else {
                            None
                        }
                    })
                    .collect();

                let accumulators = map.entry(row_key).or_insert_with(|| {
                    self.aggregate_expr
                        .iter()
                        .map(|a| a.create_accumulator(ArrowType::Int64Type)) // todo !!! get arrowtype from field !!!!
                        .collect()
                });

                accumulators.iter().enumerate().for_each(|(index, acc)| {
                    let value = aggr_input_values
                        .get(index)
                        .and_then(|v| v.get_value(row_index));
                    acc.lock().unwrap().accumulate(value);
                })
            }
        });

        let mut builders = Vec::new();

        self.schema.fields.iter().for_each(|f| {
            let array = ArrowArrayFactory::create(f.data_type.clone().to_datatype(), map.len());
            let vector = ArrowVectorBuilder::new(array);
            builders.push(vector);
        });

        map.iter().for_each(|entry| {
            let grouping_key = entry.0;
            let accumulators = entry.1;

            self.group_expr.iter().enumerate().for_each(|(index, _)| {
                if let Some(vec) = builders.get_mut(index) {
                    let val = grouping_key
                        .get(index)
                        .cloned()
                        .flatten()
                        .map(|s| Arc::new(s) as Arc<dyn Any>);
                    vec.append(val);
                }
            });

            self.aggregate_expr
                .iter()
                .enumerate()
                .for_each(|(index, expr)| {
                    if let Some(vec) = builders.get_mut(self.group_expr.len() + index) {
                        let val = accumulators
                            .get(index)
                            .cloned()
                            .map(|a| a.lock().unwrap().final_value())
                            .flatten();
                        vec.append(val);
                    }
                });
        });

        let fields: Vec<Arc<dyn ColumnVector>> =
            builders.into_iter().map(|vec| vec.build()).collect();

        let output_batch = RecordBatch {
            schema: self.schema.clone(),
            fields,
        };

        Box::new(vec![output_batch].into_iter())
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}
