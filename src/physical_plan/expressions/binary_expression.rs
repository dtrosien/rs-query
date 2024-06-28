use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::physical_plan::expressions::Expression;
use std::sync::Arc;

/// This base class provides a common framework for evaluating binary expressions.
/// Binary expressions require the evaluation of both left and right input expressions.
/// Once these inputs are evaluated, the specific binary operator can be applied to the resulting values.
/// This base class simplifies the implementation of individual binary operators by centralizing common logic.
///
/// The general steps for evaluating a binary expression are:
/// 1. Evaluate the left input expression to obtain its value.
/// 2. Evaluate the right input expression to obtain its value.
/// 3. Ensure the resulting values are compatible for the binary operation (e.g., matching sizes and types).
/// 4. Apply the specific binary operator to the evaluated values.
///
/// This approach allows subclasses to focus on implementing the specific binary operation,
/// promoting code reuse and reducing duplication.
///
pub trait BinaryExpression: Expression {
    fn l_expr(&self) -> Arc<dyn Expression>;
    fn r_expr(&self) -> Arc<dyn Expression>;

    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        let ll = self.l_expr().evaluate(input);
        let rr = self.r_expr().evaluate(input);
        assert_eq!(ll.size(), rr.size(), "different vector length");
        if ll.get_type() != rr.get_type() {
            panic!(
                "Binary expression operands do not have the same type: {:?} != {:?}",
                ll.get_type(),
                rr.get_type()
            )
        };
        self.evaluate_binary(ll, rr)
    }
    fn evaluate_binary(
        &self,
        l: Arc<dyn ColumnVector>,
        r: Arc<dyn ColumnVector>,
    ) -> Arc<dyn ColumnVector>;
}
