use crate::datatypes::schema::Schema;
use crate::logical_plan::aggregate::Aggregate;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::projection::Projection;
use crate::logical_plan::selection::Selection;
use crate::logical_plan::LogicalPlan;
use std::sync::Arc;

pub trait DataFrame {
    /// Apply a projection
    fn project(&self, expr: Vec<Arc<Expr>>) -> impl DataFrame; // todo test if using impl also for expressions is maybe better (after this DataFrame implementation proofed to be correct in physical plan and query planner)

    /// Apply a filter
    fn filter(&self, expr: Arc<Expr>) -> impl DataFrame; // todo 2 check if arc<dyn > is better because then as_arc or something is possible

    /// Aggregate
    fn aggregate(
        &self,
        group_expr: Vec<Arc<Expr>>,
        aggregate_expr: Vec<Arc<Expr>>,
    ) -> impl DataFrame;

    /// Returns the schema of the data that will be produced by this DataFrame.
    fn schema(&self) -> Arc<Schema>;

    /// Get the logical plan
    fn logical_plan(&self) -> Arc<dyn LogicalPlan>;

    // fn as_arc(&self) -> Arc<dyn DataFrame>; // all other impl must also be arc<dyn>
}

struct DataFrameImpl {
    plan: Arc<dyn LogicalPlan>,
}

impl DataFrame for DataFrameImpl {
    fn project(&self, expr: Vec<Arc<Expr>>) -> impl DataFrame {
        DataFrameImpl {
            plan: Projection::new(self.plan.clone(), expr),
        }
    }

    fn filter(&self, expr: Arc<Expr>) -> impl DataFrame {
        DataFrameImpl {
            plan: Selection::new(self.plan.clone(), expr),
        }
    }

    fn aggregate(
        &self,
        group_expr: Vec<Arc<Expr>>,
        aggregate_expr: Vec<Arc<Expr>>,
    ) -> impl DataFrame {
        DataFrameImpl {
            plan: Aggregate::new(self.plan.clone(), group_expr, aggregate_expr),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        self.plan.schema()
    }

    fn logical_plan(&self) -> Arc<dyn LogicalPlan> {
        self.plan.clone()
    }

    // fn as_arc(&self) -> Arc<dyn DataFrame> {
    //     Arc::new(self)
    // }
}

#[cfg(test)]
mod test {
    use crate::datasource::Source;
    use crate::logical_plan::data_frame::{DataFrame, DataFrameImpl};
    use crate::logical_plan::expressions::aggr_expr::{count, max, min};
    use crate::logical_plan::expressions::binary_expr::BooleanBinaryExprExt;
    use crate::logical_plan::expressions::literal_expr::{lit_float, lit_long, lit_str};
    use crate::logical_plan::expressions::math_expr::MathExprExt;
    use crate::logical_plan::expressions::{alias, col};
    use crate::logical_plan::scan::Scan;
    use crate::logical_plan::PlanPrinter;

    #[test]
    fn test_build_data_frame() {
        let df = csv()
            .filter(col("state").eq(lit_str("CO")))
            .project(vec![col("id"), col("first_name"), col("last_name")])
            .logical_plan();

        //println!("{df.pretty()}");
        assert_eq!(
            "Projection: id, first_name, last_name\n\
        \tSelection: state = CO\n\
        \t\tScan: employee; projection=None\n",
            df.pretty()
        );
    }

    #[test]
    fn test_multiply_alias_data_frame() {
        let df = csv()
            .filter(col("state").eq(lit_str("CO")))
            .project(vec![
                col("id"),
                col("first_name"),
                col("last_name"),
                col("salary"),
                alias(col("salary").mult(lit_float(0.1)), "bonus"),
            ])
            .filter(col("bonus").gt(lit_long(1000)))
            .logical_plan();

        assert_eq!(
            "Selection: bonus > 1000\n\
     \tProjection: id, first_name, last_name, salary, salary * 0.1 AS bonus\n\
     \t\tSelection: state = CO\n\
     \t\t\tScan: employee; projection=None\n",
            df.pretty()
        );
    }

    #[test]
    fn test_aggregate_data_frame() {
        let df = csv()
            .aggregate(
                vec![col("state")],
                vec![min(col("salary")), max(col("salary")), count(col("salary"))],
            )
            .logical_plan();

        assert_eq!(
            "Aggregate: group_expr=state, aggregate_expr=MIN(salary), MAX(salary), COUNT(salary)\n\
            \tScan: employee; projection=None\n",
            df.pretty()
        );
    }

    fn csv() -> impl DataFrame {
        let csv = Source::from_csv("testdata/employee.csv", None, true, 1024);
        DataFrameImpl {
            plan: Scan::new("employee".to_string(), csv, vec![]),
        }
    }
}
