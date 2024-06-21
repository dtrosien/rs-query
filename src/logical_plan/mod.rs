mod aggregate;
pub mod expressions;
mod limit;
pub mod logical_expr;
mod projection;
pub mod scan;
mod selection;

use crate::datatypes::schema::Schema;
use std::sync::Arc;

trait LogicalPlan: ToString {
    fn schema(&self) -> Arc<Schema>;
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;

    fn pretty(self: Arc<Self>) -> String
    where
        Self: Sized + 'static,
    {
        format(self.clone(), 0)
    }
}

fn format(plan: Arc<dyn LogicalPlan>, indent: usize) -> String {
    let mut b = String::new();
    for _ in 0..indent {
        b.push_str("\t");
    }
    b.push_str(&plan.to_string());
    b.push_str("\n");
    for child in plan.children() {
        b.push_str(&format(child, indent + 1));
    }
    b
}

#[cfg(test)]
mod test {
    use crate::datasource::Source;
    use crate::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
    use crate::logical_plan::expressions::col;
    use crate::logical_plan::expressions::literal_expr::lit_str;
    use crate::logical_plan::format;
    use crate::logical_plan::projection::Projection;
    use crate::logical_plan::scan::Scan;
    use crate::logical_plan::selection::Selection;
    use std::sync::Arc;

    #[test]
    fn test_build_logical_plan() {
        // create a plan to represent the data source
        let csv = Arc::from(Source::from_csv("testdata/employee.csv", None, true, 1024));
        // create a plan to represent the scan of the data source (FROM)
        let scan = Arc::from(Scan::new("employee".to_string(), csv, vec![]));
        // create a plan to represent the selection (WHERE)
        let filter_expr = Arc::from(col("state")).eq(Arc::from(lit_str("CO")));
        let selection = Arc::from(Selection::new(scan, filter_expr));
        // create a plan to represent the projection (SELECT)
        let plan = Arc::from(Projection::new(
            selection,
            vec![
                Arc::from(col("id")),
                Arc::from(col("first_name")),
                Arc::from(col("last_name")),
            ],
        ));

        let plan_string = format(plan, 0);
        //println!("{plan_string}")
        assert_eq!(
            "Projection: id, first_name, last_name\n\tSelection: state = CO\n\t\tScan: employee; projection=None\n",
            plan_string
        );
    }

    #[test]
    fn test_build_nested_logical_plan() {
        let plan = Arc::from(Projection::new(
            Arc::from(Selection::new(
                Arc::from(Scan::new(
                    "employee".to_string(),
                    Arc::from(Source::from_csv("testdata/employee.csv", None, true, 1024)),
                    vec![],
                )),
                Arc::from(col("state")).eq(Arc::from(lit_str("CO"))),
            )),
            vec![
                Arc::from(col("id")),
                Arc::from(col("first_name")),
                Arc::from(col("last_name")),
            ],
        ));

        let plan_string = format(plan, 0);
        //println!("{plan_string}")
        assert_eq!(
            "Projection: id, first_name, last_name\n\tSelection: state = CO\n\t\tScan: employee; projection=None\n",
            plan_string
        );
    }

    #[test]
    fn test_build_logical_plan_with_aggregation() {
        // create a plan to represent the data source
        let csv = Arc::from(Source::from_csv("testdata/employee.csv", None, true, 1024));
        // create a plan to represent the scan of the data source (FROM)
        let scan = Arc::from(Scan::new("employee".to_string(), csv, vec![]));
        // create a plan to represent the selection (WHERE)
        let filter_expr = Arc::from(col("state")).eq(Arc::from(lit_str("CO")));
        let selection = Arc::from(Selection::new(scan, filter_expr));
        // create a plan to represent the projection (SELECT)
        let plan = Arc::from(Projection::new(
            selection,
            vec![
                Arc::from(col("id")),
                Arc::from(col("first_name")),
                Arc::from(col("last_name")),
            ],
        ));

        todo!();
        // todo finish test and streamline arced structs

        let plan_string = format(plan, 0);
        //println!("{plan_string}")
        assert_eq!(
            "Projection: id, first_name, last_name\n\tSelection: state = CO\n\t\tScan: employee; projection=None\n",
            plan_string
        );
    }
}
