use crate::logical_plan::data_frame::DataFrame;
use sqlparser::ast::TableFactor;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SqlPlanner;

impl SqlPlanner {
    pub fn create_data_frame(
        select: sqlparser::ast::Select,
        tables: HashMap<String, Arc<dyn DataFrame>>,
    ) {
        if let Some(table_with_joins) = select.from.first() {
            match &table_with_joins.relation {
                TableFactor::Table { name, .. } => {
                    let table = tables.get(&name.to_string()).unwrap();
                }
                _ => panic!("not supported"),
            }
        }
    }
}
