use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::physical_plan::expressions::binary_expression::BinaryExpression;
use crate::physical_plan::expressions::Expression;
use std::any::Any;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

pub trait MathExpression: BinaryExpression {
    fn evaluate_binary(
        &self,
        l: Arc<dyn ColumnVector>,
        r: Arc<dyn ColumnVector>,
    ) -> Arc<dyn ColumnVector> {
        let array = ArrowArrayFactory::create(l.get_type().to_datatype(), l.size());
        let mut vector = ArrowVectorBuilder::new(array);

        for i in 0..l.size() {
            let value = if let (Some(l_v), Some(r_v)) = (l.get_value(i), r.get_value(i)) {
                self.evaluate_math_op(l_v.deref(), r_v.deref(), l.get_type())
            } else {
                None
            };

            vector.append(value);
        }
        vector.build()
    }

    fn evaluate_math_op(
        &self,
        l: &dyn Any,
        r: &dyn Any,
        arrow_type: ArrowType,
    ) -> Option<Arc<dyn Any>>;
}

pub struct AddExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl BinaryExpression for AddExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_binary(
        &self,
        l: Arc<dyn ColumnVector>,
        r: Arc<dyn ColumnVector>,
    ) -> Arc<dyn ColumnVector> {
        MathExpression::evaluate_binary(self, l, r)
    }
}

impl Expression for AddExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BinaryExpression::evaluate(self, input)
    }
}

impl Display for AddExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+{}", self.l.to_string(), self.r.to_string())
    }
}

impl MathExpression for AddExpression {
    fn evaluate_math_op(
        &self,
        l: &dyn Any,
        r: &dyn Any,
        arrow_type: ArrowType,
    ) -> Option<Arc<dyn Any>> {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().and_then(|l| {
                r.downcast_ref::<i8>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::Int16Type => l.downcast_ref::<i16>().and_then(|l| {
                r.downcast_ref::<i16>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::Int32Type => l.downcast_ref::<i32>().and_then(|l| {
                r.downcast_ref::<i32>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::Int64Type => l.downcast_ref::<i64>().and_then(|l| {
                r.downcast_ref::<i64>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().and_then(|l| {
                r.downcast_ref::<u8>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().and_then(|l| {
                r.downcast_ref::<u16>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().and_then(|l| {
                r.downcast_ref::<u32>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().and_then(|l| {
                r.downcast_ref::<u64>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::FloatType => l.downcast_ref::<f32>().and_then(|l| {
                r.downcast_ref::<f32>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            ArrowType::DoubleType => l.downcast_ref::<f64>().and_then(|l| {
                r.downcast_ref::<f64>()
                    .map(|r| Arc::new(l + r) as Arc<dyn Any>)
            }),
            _ => panic!("Unsupported data type in math expression: {:?}", arrow_type),
        }
    }
}

////////////////////////////////////////////////////////////////////////////
