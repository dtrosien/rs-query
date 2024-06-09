use arrow::datatypes::DataType;

#[derive(Clone, Debug, PartialEq)]
pub enum ArrowType {
    BooleanType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    FloatType,
    DoubleType,
    StringType,
}

impl ArrowType {
    pub fn to_datatype(&self) -> DataType {
        match self {
            ArrowType::BooleanType => DataType::Boolean,
            ArrowType::Int8Type => DataType::Int8,
            ArrowType::Int16Type => DataType::Int16,
            ArrowType::Int32Type => DataType::Int32,
            ArrowType::Int64Type => DataType::Int64,
            ArrowType::UInt8Type => DataType::UInt8,
            ArrowType::UInt16Type => DataType::UInt16,
            ArrowType::UInt32Type => DataType::UInt32,
            ArrowType::UInt64Type => DataType::UInt64,
            ArrowType::FloatType => DataType::Float32,
            ArrowType::DoubleType => DataType::Float64,
            ArrowType::StringType => DataType::Utf8,
        }
    }

    pub fn from_datatype(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => ArrowType::BooleanType,
            DataType::Int8 => ArrowType::Int8Type,
            DataType::Int16 => ArrowType::Int16Type,
            DataType::Int32 => ArrowType::Int32Type,
            DataType::Int64 => ArrowType::Int64Type,
            DataType::UInt8 => ArrowType::UInt8Type,
            DataType::UInt16 => ArrowType::UInt16Type,
            DataType::UInt32 => ArrowType::UInt32Type,
            DataType::UInt64 => ArrowType::UInt64Type,
            DataType::Float32 => ArrowType::FloatType,
            DataType::Float64 => ArrowType::DoubleType,
            DataType::Utf8 => ArrowType::StringType,
            _ => panic!("Unsupported data type"),
        }
    }
}
