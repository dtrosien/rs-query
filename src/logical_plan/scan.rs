use crate::datasource::{DataSource, Source};
use crate::datatypes::schema::Schema;
use crate::logical_plan::LogicalPlan;
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

/// Represents a scan of a data source
pub struct Scan {
    path: String,
    pub datasource: Arc<Source>,
    pub projection: Vec<String>,
    schema: Arc<Schema>,
}

impl Scan {
    pub fn new(path: String, datasource: Arc<Source>, projection: Vec<String>) -> Arc<Self> {
        let schema = Self::derive_schema(datasource.clone(), projection.clone());
        Arc::new(Scan {
            path,
            datasource,
            projection,
            schema,
        })
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod test {
    use crate::datasource::Source;
    use crate::logical_plan::format;
    use crate::logical_plan::scan::Scan;

    #[test]
    fn test_logical_scan() {
        let csv = Source::from_csv("testdata/employee.csv", None, true, 1024);
        let scan = Scan::new("employee".to_string(), csv, vec![]);
        let plan_string = format(scan, 0);
        assert_eq!("Scan: employee; projection=None\n", plan_string);
        //println!("{plan_string}")
    }
}
