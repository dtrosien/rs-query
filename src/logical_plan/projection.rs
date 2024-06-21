use crate::datatypes::schema::Schema;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct Projection {
    input: Arc<dyn LogicalPlan>,
    expr: Vec<Arc<Expr>>,
}

impl Projection {
    pub fn new(input: Arc<dyn LogicalPlan>, expr: Vec<Arc<Expr>>) -> Self {
        Projection { input, expr }
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields = self
            .expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Projection: {}", fields)
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Arc<Schema> {
        let fields = self
            .expr
            .iter()
            // filter_map because to_field returns a result
            .filter_map(|e| e.to_field(self.input.clone()).ok())
            .collect();
        Arc::from(Schema { fields })
    }
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }
}

#[cfg(test)]
mod test {
    use crate::datasource::Source;
    use crate::logical_plan::expressions::col;
    use crate::logical_plan::format;
    use crate::logical_plan::projection::Projection;
    use crate::logical_plan::scan::Scan;
    use std::sync::Arc;

    #[test]
    fn test_logical_projection() {
        let csv = Source::from_csv("testdata/employee.csv", None, true, 1024);
        let scan = Arc::from(Scan::new("employee".to_string(), csv, vec![]));

        let projection = Arc::from(Projection::new(scan, vec![col("id")]));
        let plan_string = format(projection, 0);
        // println!("{plan_string}");
        assert_eq!(
            "Projection: id\n\tScan: employee; projection=None\n",
            plan_string
        );
    }
}
