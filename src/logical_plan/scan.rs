use crate::datasource::{DataSource, Source};
use crate::datatypes::schema::Schema;
use crate::logical_plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

struct Scan {
    path: String,
    datasource: Arc<Source>,
    projection: Vec<String>,
    schema: Arc<Schema>,
}

impl Scan {
    pub fn new(path: String, datasource: Arc<Source>, projection: Vec<String>) -> Self {
        let schema = Self::derive_schema(datasource.clone(), projection.clone());
        Scan {
            path,
            datasource,
            projection,
            schema,
        }
    }

    fn derive_schema(datasource: Arc<Source>, projection: Vec<String>) -> Arc<Schema> {
        let schema = datasource.schema();
        if projection.is_empty() {
            return schema;
        } else {
            Arc::from(
                schema
                    .select(projection.iter().map(AsRef::as_ref).collect())
                    .expect("TODO: panic message"),
            )
        }
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.projection.is_empty() {
            write!(f, "Scan: {}; projection=None", self.path)
        } else {
            write!(f, "Scan: {}; projection={:?}", self.path, self.projection)
        }
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![]
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_scan() {
        todo!()
    }
}
