pub enum ArrowTypes {
    BooleanType(arrow::datatypes::BooleanType),
    Int8Type(arrow::datatypes::Int8Type),
    Int16Type(arrow::datatypes::Int16Type),
    Int32Type(arrow::datatypes::Int32Type),
    Int64Type(arrow::datatypes::Int64Type),
    UInt8Type(arrow::datatypes::UInt8Type),
    UInt16Type(arrow::datatypes::UInt16Type),
    UInt32Type(arrow::datatypes::UInt32Type),
    UInt64Type(arrow::datatypes::UInt64Type),
    FloatType(arrow::datatypes::Float32Type),
    DoubleType(arrow::datatypes::Float64Type),
    StringType(arrow::datatypes::Utf8Type),
}
