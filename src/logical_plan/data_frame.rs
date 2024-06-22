use crate::datasource::Source;
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::logical_plan::aggregate::Aggregate;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::projection::Projection;
use crate::logical_plan::scan::Scan;
use crate::logical_plan::selection::Selection;
use crate::logical_plan::LogicalPlan;
use std::sync::Arc;

pub trait DataFrame {
    /// Apply a projection
    fn project(self: Arc<Self>, expr: Vec<Arc<Expr>>) -> Arc<dyn DataFrame>;

    /// Apply a filter
    fn filter(self: Arc<Self>, expr: Arc<Expr>) -> Arc<dyn DataFrame>;

    /// Aggregate
    fn aggregate(
        self: Arc<Self>,
        group_expr: Vec<Arc<Expr>>,
        aggregate_expr: Vec<Arc<Expr>>,
    ) -> Arc<dyn DataFrame>;

    /// Returns the schema of the data that will be produced by this DataFrame.
    fn schema(self: Arc<Self>) -> Arc<Schema>;

    /// Get the logical plan
    fn logical_plan(self: Arc<Self>) -> Arc<dyn LogicalPlan>;
}

struct DataFrameImpl {
    plan: Arc<dyn LogicalPlan>,
}

impl DataFrame for DataFrameImpl {
    fn project(self: Arc<Self>, expr: Vec<Arc<Expr>>) -> Arc<dyn DataFrame> {
        Arc::new(DataFrameImpl {
            plan: Projection::new(self.plan.clone(), expr),
        })
    }

    fn filter(self: Arc<Self>, expr: Arc<Expr>) -> Arc<dyn DataFrame> {
        Arc::new(DataFrameImpl {
            plan: Selection::new(self.plan.clone(), expr),
        })
    }

    fn aggregate(
        self: Arc<Self>,
        group_expr: Vec<Arc<Expr>>,
        aggregate_expr: Vec<Arc<Expr>>,
    ) -> Arc<dyn DataFrame> {
        Arc::new(DataFrameImpl {
            plan: Aggregate::new(self.plan.clone(), group_expr, aggregate_expr),
        })
    }

    fn schema(self: Arc<Self>) -> Arc<Schema> {
        self.plan.schema()
    }

    fn logical_plan(self: Arc<Self>) -> Arc<dyn LogicalPlan> {
        self.plan.clone()
    }
}

/// Convenience Dataframe builder for supported DataSources
pub struct DF {}

impl DF {
    pub fn from_csv(
        file_name: &str,
        schema: Option<Arc<Schema>>,
        has_headers: bool,
        batch_size: usize,
    ) -> Arc<dyn DataFrame> {
        Arc::new(DataFrameImpl {
            plan: Scan::new(
                file_name.to_string(),
                Source::from_csv(file_name, schema, has_headers, batch_size),
                vec![],
            ),
        })
    }

    pub fn from_in_memory(schema: Arc<Schema>, data: Vec<RecordBatch>) -> Arc<dyn DataFrame> {
        Arc::new(DataFrameImpl {
            plan: Scan::new(
                "in_memory".to_string(),
                Source::from_in_memory(schema, data),
                vec![],
            ),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::logical_plan::data_frame::{DataFrame, DF};
    use crate::logical_plan::expressions::aggr_expr::{count, max, min};
    use crate::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
    use crate::logical_plan::expressions::literal_expr::{lit_float, lit_long, lit_str};
    use crate::logical_plan::expressions::math_expr::MathExprExt;
    use crate::logical_plan::expressions::{alias, col};
    use crate::logical_plan::PlanPrinter;
    use std::sync::Arc;

    #[test]
    fn test_build_data_frame() {
        let df = test_csv()
            .filter(col("state").eq(lit_str("CO")))
            .project(vec![col("id"), col("first_name"), col("last_name")]);

        //println!("{df.logical_plan().pretty()}");
        assert_eq!(
            "Projection: id, first_name, last_name\n\
        \tSelection: state = CO\n\
        \t\tScan: testdata/employee.csv; projection=None\n",
            df.logical_plan().pretty()
        );
    }

    #[test]
    fn test_multiply_alias_data_frame() {
        let df = test_csv()
            .filter(col("state").eq(lit_str("CO")))
            .project(vec![
                col("id"),
                col("first_name"),
                col("last_name"),
                col("salary"),
                alias(col("salary").mult(lit_float(0.1)), "bonus"),
            ])
            .filter(col("bonus").gt(lit_long(1000)));

        assert_eq!(
            "Selection: bonus > 1000\n\
     \tProjection: id, first_name, last_name, salary, salary * 0.1 AS bonus\n\
     \t\tSelection: state = CO\n\
     \t\t\tScan: testdata/employee.csv; projection=None\n",
            df.logical_plan().pretty()
        );
    }

    #[test]
    fn test_aggregate_data_frame() {
        let df = test_csv().aggregate(
            vec![col("state")],
            vec![min(col("salary")), max(col("salary")), count(col("salary"))],
        );

        let logical_plan_string = df.logical_plan().pretty();
        //println!("{logical_plan_string}");

        assert_eq!(
            "Aggregate: group_expr=state, aggregate_expr=MIN(salary), MAX(salary), COUNT(salary)\n\
            \tScan: testdata/employee.csv; projection=None\n",
            logical_plan_string
        );
    }

    fn test_csv() -> Arc<dyn DataFrame> {
        DF::from_csv("testdata/employee.csv", None, true, 1024)
    }
}
