use crate::datatypes::arrow_types::ArrowType;
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<Arc<Field>>,
}

impl Schema {
    // Convert from Arrow's Schema
    pub fn from_arrow(arrow_schema: &ArrowSchema) -> Self {
        let fields: Vec<Arc<Field>> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                Arc::new(Field {
                    name: f.name().clone(),
                    data_type: ArrowType::from_datatype(f.data_type()),
                })
            })
            .collect();
        Schema { fields }
    }

    // Convert to Arrow's Schema
    pub fn to_arrow(&self) -> ArrowSchema {
        let fields: Vec<_> = self.fields.iter().map(|f| f.to_arrow()).collect();
        ArrowSchema::new(fields)
    }

    // Project schema by field indices
    pub fn project(&self, indices: Vec<usize>) -> Schema {
        let fields = indices
            .into_iter()
            .map(|i| self.fields[i].clone())
            .collect();
        Schema { fields }
    }

    // Select schema by field names
    pub fn select(&self, names: Vec<&str>) -> Result<Schema, &'static str> {
        let mut fields = Vec::new();
        let name_set: HashMap<_, _> = self.fields.iter().map(|f| (&f.name as &str, f)).collect();
        for &name in names.iter() {
            match name_set.get(name) {
                Some(field) => fields.push((*field).clone()),
                None => return Err("Field name not found"),
            }
        }
        Ok(Schema { fields })
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {:?}", field.name, field.data_type)?;
        }
        write!(f, "]")
    }
}

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub data_type: ArrowType,
}

impl Field {
    // Convert to Arrow's Field
    fn to_arrow(&self) -> ArrowField {
        ArrowField::new(&self.name, self.data_type.to_datatype(), true)
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::arrow_types::ArrowType;
    use crate::datatypes::schema::{Field, Schema};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    #[test]
    fn test_schema_conversions() {
        let field1 = Arc::new(Field {
            name: "test1".to_string(),
            data_type: ArrowType::StringType,
        });
        let field2 = Arc::new(Field {
            name: "test2".to_string(),
            data_type: ArrowType::Int64Type,
        });
        let schema = Schema {
            fields: vec![field1, field2],
        };

        let arrow_schema = schema.to_arrow();
        let first_arrow_field = arrow_schema.fields().first().unwrap();
        let num_fields = arrow_schema.fields().len();

        assert_eq!(first_arrow_field.name(), "test1");
        assert_eq!(first_arrow_field.data_type(), &DataType::Utf8);
        assert_eq!(num_fields, 2)
    }

    #[test]
    fn test_display_schema() {
        let field1 = Arc::new(Field {
            name: "test1".to_string(),
            data_type: ArrowType::StringType,
        });
        let field2 = Arc::new(Field {
            name: "test2".to_string(),
            data_type: ArrowType::Int64Type,
        });
        let schema = Schema {
            fields: vec![field1, field2],
        };

        assert_eq!("[test1: StringType, test2: Int64Type]", format!("{schema}"));
    }
}
