use crate::datatypes::schema::{Field, Schema};
use crate::logical_plan::expressions::aggr_expr::AggrExpr;
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

pub struct Aggregate {
    input: Arc<dyn LogicalPlan>,
    group_expr: Vec<Arc<Expr>>,
    aggregate_expr: Vec<Arc<Expr>>, // todo change back to AggrExpr when solution for chaining expressions is found (see also aggr_expr.rs)
}

impl Aggregate {
    pub fn new(
        input: Arc<dyn LogicalPlan>,
        group_expr: Vec<Arc<Expr>>,
        aggregate_expr: Vec<Arc<Expr>>,
    ) -> Self {
        Aggregate {
            input,
            group_expr,
            aggregate_expr,
        }
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Aggregate: group_expr={}, aggregate_expr={}",
            self.group_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.aggregate_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Arc<Schema> {
        let group_fields: Vec<Arc<Field>> = self
            .group_expr
            .iter()
            // filter_map because to_field returns a result
            .filter_map(|e| e.to_field(self.input.clone()).ok())
            .collect();
        let aggr_fields: Vec<Arc<Field>> = self
            .aggregate_expr
            .iter()
            .filter_map(|e| e.to_field(self.input.clone()).ok())
            .collect();

        let fields = group_fields
            .into_iter()
            .chain(aggr_fields.into_iter())
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
    use crate::datatypes::arrow_types::ArrowType;
    use crate::logical_plan::aggregate::Aggregate;
    use crate::logical_plan::expressions::aggr_expr::max;
    use crate::logical_plan::expressions::{cast, col};
    use crate::logical_plan::format;
    use crate::logical_plan::scan::Scan;
    use std::sync::Arc;

    #[test]
    fn test_logical_selection() {
        let csv = Arc::from(Source::from_csv("testdata/employee.csv", None, true, 1024));
        let scan = Arc::from(Scan::new("employee".to_string(), csv, vec![]));

        let group_expr = vec![Arc::from(col("state"))];
        let aggr_expr = vec![max(Arc::from(cast(
            Arc::from(col("salary")),
            ArrowType::Int32Type,
        )))];
        let aggregate = Arc::from(Aggregate::new(scan, group_expr, aggr_expr));

        let plan_string = format(aggregate, 0);
        assert_eq!(
            "Aggregate: group_expr=state, aggregate_expr=MAX(Cast(salary AS Int32Type))\n\tScan: employee; projection=None\n",
            plan_string
        );
        //println!("{plan_string}")
    }
}
