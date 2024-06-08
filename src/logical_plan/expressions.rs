use crate::datatypes::schema::Field;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

struct Column {
    pub name: String,
}

pub fn col(name: &str) -> Column {
    Column {
        name: name.to_string(),
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl LogicalExpr for Column {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        Arc::new(
            // todo remove here when Arc<Field> is used in Schema
            input
                .schema()
                .fields
                .iter()
                .find(|f| self.name == f.name)
                .expect(format!("No column named {}", self.name).as_str())
                .clone(),
        )
    }
}
