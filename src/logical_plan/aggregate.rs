use crate::datatypes::schema::{Field, Schema};
use crate::logical_plan::expressions::Expr;
use crate::logical_plan::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use arrow::array::Array;
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

pub struct Aggregate {
    pub input: Arc<dyn LogicalPlan>,
    pub group_expr: Vec<Arc<Expr>>,
    pub aggregate_expr: Vec<Arc<Expr>>, // todo use exlicit AggrExpr (or impl trait) if a better solution for expr chaining is found
}

impl Aggregate {
    pub fn new(
        input: Arc<dyn LogicalPlan>,
        group_expr: Vec<Arc<Expr>>,
        aggregate_expr: Vec<Arc<Expr>>,
    ) -> Arc<Self> {
        let filtered_aggr_expr: Vec<Arc<Expr>> = aggregate_expr // todo remove filter when a better solution for to exlicit AggrExpr is found
            .into_iter()
            .filter(|a| matches!(**a, Expr::Aggr(_)))
            .collect();
        Arc::new(Aggregate {
            input,
            group_expr,
            aggregate_expr: filtered_aggr_expr,
        })
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
        let fields: Vec<Arc<Field>> = self
            .group_expr
            .iter()
            .chain(self.aggregate_expr.iter())
            .filter_map(|e| e.to_field(self.input.clone()).ok())
            .collect();

        Arc::from(Schema { fields })
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod test {
    use crate::data_source::Source;
    use crate::datatypes::arrow_types::ArrowType;
    use crate::logical_plan::aggregate::Aggregate;
    use crate::logical_plan::expressions::aggr_expr::max;
    use crate::logical_plan::expressions::{cast, col};
    use crate::logical_plan::format;
    use crate::logical_plan::scan::Scan;

    #[test]
    fn test_logical_selection() {
        let csv = Source::from_csv("testdata/employee.csv", None, true, 1024);
        let scan = Scan::new("employee".to_string(), csv, vec![]);

        let group_expr = vec![col("state")];
        let aggr_expr = vec![
            max(cast(col("salary"), ArrowType::Int32Type)),
            col("must_not_be_in_plan"),
        ];
        let aggregate = Aggregate::new(scan, group_expr, aggr_expr);

        let plan_string = format(aggregate, 0);
        //println!("{plan_string}");
        assert_eq!(
            "Aggregate: group_expr=state, aggregate_expr=MAX(CAST(salary AS Int32Type))\n\tScan: employee; projection=None\n",
            plan_string
        );
    }
}
