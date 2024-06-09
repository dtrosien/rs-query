use crate::datatypes::schema::Field;
use crate::logical_plan::LogicalPlan;
use std::sync::Arc;

pub trait LogicalExpr: ToString {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>>;
}
