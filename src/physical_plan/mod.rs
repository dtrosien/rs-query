use crate::datatypes::record_batch::RecordBatch;
use crate::datatypes::schema::Schema;
use std::sync::Arc;

pub mod expressions;
mod projection_exec;
pub mod scan_exec;
pub mod selection_exec;

/// A physical plan represents an executable piece of code that will produce data.
pub trait PhysicalPlan: ToString {
    /// Returns the schema of this PhysicalPlan
    fn schema(&self) -> Arc<Schema>;

    /// Execute a physical plan and produce a series of record batches.
    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_>;

    /// Returns the children (inputs) of this physical plan.
    /// This method is used to enable use of the visitor pattern to walk a query tree.
    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>>;
}

/// Trait to pretty print PhysicalPlan
/// outside physical plan because otherwise it does not work with ?Sized
pub trait PhysicalPlanPrinter {
    fn pretty(&self) -> String;
}

/// Pretty prints PhysicalPlan objects
impl PhysicalPlanPrinter for Arc<dyn PhysicalPlan> {
    fn pretty(&self) -> String {
        format(self.clone(), 0)
    }
}

/// Format a physical plan in human-readable form
fn format(plan: Arc<dyn PhysicalPlan>, indent: usize) -> String {
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
