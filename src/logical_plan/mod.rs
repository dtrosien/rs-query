pub mod expressions;
pub mod logical_expr;

use crate::datatypes::schema::Schema;
use std::sync::Arc;

trait LogicalPlan: ToString {
    fn schema(&self) -> Arc<Schema>;
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;

    fn pretty(&self) -> String {
        format(self.clone_arc(), 0)
    }

    /// Example
    ///   fn clone_arc(&self) -> Arc<dyn LogicalPlan> {
    ///         Arc::new(Self {
    ///             schema: Arc::clone(&self.schema),
    ///             children: self.children.clone(),
    ///         })
    ///     }
    fn clone_arc(&self) -> Arc<dyn LogicalPlan>;
}

// // Trait to enable cloning of Arc<dyn LogicalPlan>
// trait CloneArcLogicalPlan {
//     fn clone_arc(&self) -> Arc<dyn LogicalPlan>;
// }
//
// // Implement CloneArcLogicalPlan for all types that implement LogicalPlan + Clone
// impl<T> CloneArcLogicalPlan for T
// where
//     T: 'static + LogicalPlan + Clone,
// {
//     fn clone_arc(&self) -> Arc<dyn LogicalPlan> {
//         Arc::new(self.clone())
//     }
// }

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
