use crate::datatypes::schema::Field;
use crate::logical_plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub trait LogicalExpr: Display {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> anyhow::Result<Arc<Field>>;
}
