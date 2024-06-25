use crate::datatypes::schema::Schema;
use crate::logical_plan::LogicalPlan;
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

/// Logical plan representing a limit
pub struct Limit {
    input: Arc<dyn LogicalPlan>,
    limit: usize,
}

impl Limit {
    pub fn new(input: Arc<dyn LogicalPlan>, limit: usize) -> Self {
        Limit { input, limit }
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit: {}", self.limit)
    }
}

impl LogicalPlan for Limit {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
