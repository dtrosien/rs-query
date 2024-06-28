use crate::datatypes::arrow_field_vector::ArrowArrayFactory;
use crate::datatypes::arrow_types::ArrowType;
use crate::datatypes::arrow_vector_builder::ArrowVectorBuilder;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::record_batch::RecordBatch;
use crate::physical_plan::expressions::Expression;
use std::any::Any;
use std::fmt::{Display, Pointer};
use std::ops::Deref;
use std::sync::Arc;

pub trait BooleanExpression: Expression {
    fn l_expr(&self) -> Arc<dyn Expression>;
    fn r_expr(&self) -> Arc<dyn Expression>;

    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        let ll = self.l_expr().evaluate(input);
        let rr = self.r_expr().evaluate(input);
        assert_eq!(ll.size(), rr.size(), "different vector length");
        if ll.get_type() != rr.get_type() {
            panic!(
                "Cannot compare values of different type: {:?} != {:?}",
                ll.get_type(),
                rr.get_type()
            )
        };
        self.compare(ll, rr)
    }

    fn compare(&self, l: Arc<dyn ColumnVector>, r: Arc<dyn ColumnVector>) -> Arc<dyn ColumnVector> {
        let vec_size = l.size();
        let array = ArrowArrayFactory::create(ArrowType::BooleanType.to_datatype(), vec_size);
        let mut boolean_vector = ArrowVectorBuilder::new(array);
        for i in 0..vec_size {
            let value = self.evaluate_bool(
                l.get_value(i).unwrap().deref(),
                r.get_value(i).unwrap().deref(),
                l.get_type(),
            );
            boolean_vector.append(Some(Arc::new(value)))
        }
        boolean_vector.build()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool;
}

pub struct AndExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for AndExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AND {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for AndExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for AndExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        to_bool(l.deref()) && to_bool(r.deref())
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct OrExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for OrExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} OR {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for OrExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for OrExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        to_bool(l.deref()) || to_bool(r.deref())
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct EqExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for EqExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for EqExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for EqExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().eq(&r.downcast_ref::<i8>()),
            ArrowType::Int16Type => l.downcast_ref::<i16>().eq(&r.downcast_ref::<i16>()),
            ArrowType::Int32Type => l.downcast_ref::<i32>().eq(&r.downcast_ref::<i32>()),
            ArrowType::Int64Type => l.downcast_ref::<i64>().eq(&r.downcast_ref::<i64>()),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().eq(&r.downcast_ref::<u8>()),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().eq(&r.downcast_ref::<u16>()),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().eq(&r.downcast_ref::<u32>()),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().eq(&r.downcast_ref::<u64>()),
            ArrowType::FloatType => l.downcast_ref::<f32>().eq(&r.downcast_ref::<f32>()),
            ArrowType::DoubleType => l.downcast_ref::<f64>().eq(&r.downcast_ref::<f64>()),
            ArrowType::StringType => l.downcast_ref::<String>().eq(&r.downcast_ref::<String>()),
            _ => panic!(
                "Unsupported data type in comparison expression: {:?}",
                arrow_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct NeqExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for NeqExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} != {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for NeqExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for NeqExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().ne(&r.downcast_ref::<i8>()),
            ArrowType::Int16Type => l.downcast_ref::<i16>().ne(&r.downcast_ref::<i16>()),
            ArrowType::Int32Type => l.downcast_ref::<i32>().ne(&r.downcast_ref::<i32>()),
            ArrowType::Int64Type => l.downcast_ref::<i64>().ne(&r.downcast_ref::<i64>()),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().ne(&r.downcast_ref::<u8>()),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().ne(&r.downcast_ref::<u16>()),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().ne(&r.downcast_ref::<u32>()),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().ne(&r.downcast_ref::<u64>()),
            ArrowType::FloatType => l.downcast_ref::<f32>().ne(&r.downcast_ref::<f32>()),
            ArrowType::DoubleType => l.downcast_ref::<f64>().ne(&r.downcast_ref::<f64>()),
            ArrowType::StringType => l.downcast_ref::<String>().ne(&r.downcast_ref::<String>()),
            _ => panic!(
                "Unsupported data type in comparison expression: {:?}",
                arrow_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct LtExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for LtExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} < {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for LtExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for LtExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().lt(&r.downcast_ref::<i8>()),
            ArrowType::Int16Type => l.downcast_ref::<i16>().lt(&r.downcast_ref::<i16>()),
            ArrowType::Int32Type => l.downcast_ref::<i32>().lt(&r.downcast_ref::<i32>()),
            ArrowType::Int64Type => l.downcast_ref::<i64>().lt(&r.downcast_ref::<i64>()),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().lt(&r.downcast_ref::<u8>()),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().lt(&r.downcast_ref::<u16>()),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().lt(&r.downcast_ref::<u32>()),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().lt(&r.downcast_ref::<u64>()),
            ArrowType::FloatType => l.downcast_ref::<f32>().lt(&r.downcast_ref::<f32>()),
            ArrowType::DoubleType => l.downcast_ref::<f64>().lt(&r.downcast_ref::<f64>()),
            ArrowType::StringType => l.downcast_ref::<String>().lt(&r.downcast_ref::<String>()),
            _ => panic!(
                "Unsupported data type in comparison expression: {:?}",
                arrow_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct LtEqExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for LtEqExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} < {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for LtEqExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for LtEqExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().le(&r.downcast_ref::<i8>()),
            ArrowType::Int16Type => l.downcast_ref::<i16>().le(&r.downcast_ref::<i16>()),
            ArrowType::Int32Type => l.downcast_ref::<i32>().le(&r.downcast_ref::<i32>()),
            ArrowType::Int64Type => l.downcast_ref::<i64>().le(&r.downcast_ref::<i64>()),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().le(&r.downcast_ref::<u8>()),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().le(&r.downcast_ref::<u16>()),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().le(&r.downcast_ref::<u32>()),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().le(&r.downcast_ref::<u64>()),
            ArrowType::FloatType => l.downcast_ref::<f32>().le(&r.downcast_ref::<f32>()),
            ArrowType::DoubleType => l.downcast_ref::<f64>().le(&r.downcast_ref::<f64>()),
            ArrowType::StringType => l.downcast_ref::<String>().le(&r.downcast_ref::<String>()),
            _ => panic!(
                "Unsupported data type in comparison expression: {:?}",
                arrow_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct GtExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for GtExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} < {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for GtExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for GtExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().gt(&r.downcast_ref::<i8>()),
            ArrowType::Int16Type => l.downcast_ref::<i16>().gt(&r.downcast_ref::<i16>()),
            ArrowType::Int32Type => l.downcast_ref::<i32>().gt(&r.downcast_ref::<i32>()),
            ArrowType::Int64Type => l.downcast_ref::<i64>().gt(&r.downcast_ref::<i64>()),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().gt(&r.downcast_ref::<u8>()),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().gt(&r.downcast_ref::<u16>()),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().gt(&r.downcast_ref::<u32>()),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().gt(&r.downcast_ref::<u64>()),
            ArrowType::FloatType => l.downcast_ref::<f32>().gt(&r.downcast_ref::<f32>()),
            ArrowType::DoubleType => l.downcast_ref::<f64>().gt(&r.downcast_ref::<f64>()),
            ArrowType::StringType => l.downcast_ref::<String>().gt(&r.downcast_ref::<String>()),
            _ => panic!(
                "Unsupported data type in comparison expression: {:?}",
                arrow_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub struct GtEqExpression {
    pub l: Arc<dyn Expression>,
    pub r: Arc<dyn Expression>,
}

impl Display for GtEqExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} < {}", self.l.to_string(), self.r.to_string())
    }
}

impl Expression for GtEqExpression {
    fn evaluate(&self, input: &RecordBatch) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl BooleanExpression for GtEqExpression {
    fn l_expr(&self) -> Arc<dyn Expression> {
        self.l.clone()
    }

    fn r_expr(&self) -> Arc<dyn Expression> {
        self.r.clone()
    }

    fn evaluate_bool(&self, l: &dyn Any, r: &dyn Any, arrow_type: ArrowType) -> bool {
        match arrow_type {
            ArrowType::Int8Type => l.downcast_ref::<i8>().ge(&r.downcast_ref::<i8>()),
            ArrowType::Int16Type => l.downcast_ref::<i16>().ge(&r.downcast_ref::<i16>()),
            ArrowType::Int32Type => l.downcast_ref::<i32>().ge(&r.downcast_ref::<i32>()),
            ArrowType::Int64Type => l.downcast_ref::<i64>().ge(&r.downcast_ref::<i64>()),
            ArrowType::UInt8Type => l.downcast_ref::<u8>().ge(&r.downcast_ref::<u8>()),
            ArrowType::UInt16Type => l.downcast_ref::<u16>().ge(&r.downcast_ref::<u16>()),
            ArrowType::UInt32Type => l.downcast_ref::<u32>().ge(&r.downcast_ref::<u32>()),
            ArrowType::UInt64Type => l.downcast_ref::<u64>().ge(&r.downcast_ref::<u64>()),
            ArrowType::FloatType => l.downcast_ref::<f32>().ge(&r.downcast_ref::<f32>()),
            ArrowType::DoubleType => l.downcast_ref::<f64>().ge(&r.downcast_ref::<f64>()),
            ArrowType::StringType => l.downcast_ref::<String>().ge(&r.downcast_ref::<String>()),
            _ => panic!(
                "Unsupported data type in comparison expression: {:?}",
                arrow_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

fn to_bool(v: &dyn Any) -> bool {
    match v.downcast_ref::<bool>() {
        Some(b) => *b,
        None => panic!("Not a bool"),
    }
}
