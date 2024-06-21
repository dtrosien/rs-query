use crate::datatypes::schema::Schema;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct Selection {
    input: Arc<dyn LogicalPlan>,
    expr: Arc<Expr>,
}

impl Selection {
    pub fn new(input: Arc<dyn LogicalPlan>, expr: Arc<Expr>) -> Arc<Self> {
        Arc::new(Selection { input, expr })
    }
}

impl Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Selection: {}", self.expr)
    }
}

impl LogicalPlan for Selection {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }
}

// think about whether to use macro or new()-> Arc<Self>
#[macro_export]
macro_rules! selection {
    ($input:expr, $expr:expr) => {
        Selection::new($input, $expr)
    };
}

#[cfg(test)]
mod test {
    use crate::datasource::Source;
    use crate::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
    use crate::logical_plan::expressions::col;
    use crate::logical_plan::expressions::literal_expr::lit_str;
    use crate::logical_plan::format;
    use crate::logical_plan::scan::Scan;
    use crate::logical_plan::selection::Selection;

    #[test]
    fn test_logical_selection() {
        let csv = Source::from_csv("testdata/employee.csv", None, true, 1024);
        let scan = Scan::new("employee".to_string(), csv, vec![]);

        let filter_expr = col("state").eq(lit_str("CO"));
        // let selection = selection!(scan, filter_expr);
        let selection = Selection::new(scan, filter_expr);
        let plan_string = format(selection, 0);
        assert_eq!(
            "Selection: state = CO\n\tScan: employee; projection=None\n",
            plan_string
        );
        //println!("{plan_string}")
    }
}
