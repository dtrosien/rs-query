pub mod aggregate;
pub mod data_frame;
pub mod expressions;
pub mod limit;
pub mod logical_expr;
pub mod projection;
pub mod scan;
pub mod selection;

use crate::datatypes::schema::Schema;
use std::sync::Arc;

trait LogicalPlan: ToString {
    fn schema(&self) -> Arc<Schema>;
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;
}

/// trait to pretty print LogicalPlan
/// outside logical plan because otherwise it does not work with ?Sized
pub trait LogicalPlanPrinter {
    fn pretty(&self) -> String;
}
/// pretty prints LogicalPlan objects
impl LogicalPlanPrinter for Arc<dyn LogicalPlan> {
    fn pretty(&self) -> String {
        format(self.clone(), 0)
    }
}

/// Format a logical plan in human-readable form
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

    #[test]
    fn test_build_logical_plan() {
        // create a plan to represent the data source
        let csv = Source::from_csv("testdata/employee.csv", None, true, 1024);
        // create a plan to represent the scan of the data source (FROM)
        let scan = Scan::new("employee".to_string(), csv, vec![]);
        // create a plan to represent the selection (WHERE)
        let filter_expr = col("state").eq(lit_str("CO"));
        let selection = Selection::new(scan, filter_expr);
        // create a plan to represent the projection (SELECT)
        let plan = Projection::new(
            selection,
            vec![col("id"), col("first_name"), col("last_name")],
        );

        let plan_string = format(plan, 0);
        //println!("{plan_string}")
        assert_eq!(
            "Projection: id, first_name, last_name\n\tSelection: state = CO\n\t\tScan: employee; projection=None\n",
            plan_string
        );
    }

    #[test]
    fn test_build_nested_logical_plan() {
        let plan = Projection::new(
            Selection::new(
                Scan::new(
                    "employee".to_string(),
                    Source::from_csv("testdata/employee.csv", None, true, 1024),
                    vec![],
                ),
                col("state").eq(lit_str("CO")),
            ),
            vec![col("id"), col("first_name"), col("last_name")],
        );

        let plan_string = format(plan, 0);
        //println!("{plan_string}")
        assert_eq!(
            "Projection: id, first_name, last_name\n\tSelection: state = CO\n\t\tScan: employee; projection=None\n",
            plan_string
        );
    }
}
