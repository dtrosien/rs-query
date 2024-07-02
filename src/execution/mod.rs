use crate::data_source::{DataSource, Source};
use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use crate::logical_plan::data_frame::{DataFrame, DataFrameImpl};
use crate::logical_plan::scan::Scan;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::Optimizer;
use crate::query_planner::QueryPlanner;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

/// Execution Context
pub struct ExecutionContext {
    pub settings: HashMap<String, String>,
    tables: HashMap<String, Arc<dyn DataFrame>>,
    batch_size: usize,
}

impl ExecutionContext {
    pub fn new(settings: HashMap<String, String>) -> Self {
        let batch_size = settings
            .get("csv.batch_size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        ExecutionContext {
            settings,
            tables: Default::default(),
            batch_size,
        }
    }

    /// Create a DataFrame for the given SQL Select
    pub fn sql(sql: &str) -> Arc<dyn DataFrame> {
        todo!()
    }

    /// Get a DataFrame representing the specified CSV file
    pub fn csv(&self, file_name: impl Into<String>, has_headers: bool) -> Arc<dyn DataFrame> {
        let file_name = file_name.into();
        Arc::new(DataFrameImpl {
            plan: Scan::new(
                file_name.clone(),
                Source::from_csv(file_name, None, has_headers, self.batch_size),
                vec![],
            ),
        })
    }

    /// Get a DataFrame representing a specified RecordBatch
    pub fn in_memory(&self, schema: Arc<Schema>, data: Vec<RecordBatch>) -> Arc<dyn DataFrame> {
        Arc::new(DataFrameImpl {
            plan: Scan::new(
                "in_memory".to_string(),
                Source::from_in_memory(schema, data),
                vec![],
            ),
        })
    }

    /// Register a DataFrame with the context
    pub fn register(&mut self, table_name: impl Into<String>, df: Arc<dyn DataFrame>) {
        self.tables.insert(table_name.into(), df);
    }

    pub fn register_data_source(
        &mut self,
        table_name: impl Into<String>,
        data_source: Arc<Source>,
    ) {
        let table_name = table_name.into();
        let df = Arc::new(DataFrameImpl {
            plan: Scan::new(table_name.clone(), data_source, vec![]),
        });
        self.register(table_name.as_str(), df)
    }

    pub fn register_csv(
        &mut self,
        table_name: impl Into<String>,
        file_name: impl Into<String>,
        has_headers: bool,
    ) {
        self.register(table_name, self.csv(file_name, has_headers))
    }

    /// Execute the logical plan represented by a DataFrame
    pub fn execute(
        &self,
        df: Arc<dyn DataFrame>,
        optimize: bool,
    ) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let plan = if optimize {
            Optimizer::optimize(df.logical_plan())
        } else {
            df.logical_plan()
        };
        self.execute_logical_plan(plan.deref())
    }

    /// Execute the provided logical plan
    pub fn execute_logical_plan(
        &self,
        plan: &dyn LogicalPlan,
    ) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let physical_plan = QueryPlanner::create_physical_plan(plan);
        let batches: Vec<RecordBatch> = physical_plan.execute().collect(); // todo think about better solution
        Box::new(batches.into_iter())
    }
}
