use arrow::datatypes::DataType;

pub enum ArrowTypes {
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

impl ArrowTypes {
    pub fn to_datatype(&self) -> DataType {
        match self {
            ArrowTypes::BooleanType => DataType::Boolean,
            ArrowTypes::Int8Type => DataType::Int8,
            ArrowTypes::Int16Type => DataType::Int16,
            ArrowTypes::Int32Type => DataType::Int32,
            ArrowTypes::Int64Type => DataType::Int64,
            ArrowTypes::UInt8Type => DataType::UInt8,
            ArrowTypes::UInt16Type => DataType::UInt16,
            ArrowTypes::UInt32Type => DataType::UInt32,
            ArrowTypes::UInt64Type => DataType::UInt64,
            ArrowTypes::FloatType => DataType::Float32,
            ArrowTypes::DoubleType => DataType::Float64,
            ArrowTypes::StringType => DataType::Utf8,
        }
    }

    pub fn from_datatype(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => ArrowTypes::BooleanType,
            DataType::Int8 => ArrowTypes::Int8Type,
            DataType::Int16 => ArrowTypes::Int16Type,
            DataType::Int32 => ArrowTypes::Int32Type,
            DataType::Int64 => ArrowTypes::Int64Type,
            DataType::UInt8 => ArrowTypes::UInt8Type,
            DataType::UInt16 => ArrowTypes::UInt16Type,
            DataType::UInt32 => ArrowTypes::UInt32Type,
            DataType::UInt64 => ArrowTypes::UInt64Type,
            DataType::Float32 => ArrowTypes::FloatType,
            DataType::Float64 => ArrowTypes::DoubleType,
            DataType::Utf8 => ArrowTypes::StringType,
            _ => panic!("Unsupported data type"),
        }
    }
}
