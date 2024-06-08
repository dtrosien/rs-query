use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<Field>, // todo use Arc<Field>
}

impl Schema {
    // Convert from Arrow's Schema
    pub fn from_arrow(arrow_schema: &ArrowSchema) -> Self {
        let fields: Vec<Field> = arrow_schema
            .fields()
            .iter()
            .map(|f| Field {
                name: f.name().clone(),
                data_type: f.data_type().clone(),
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

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType, // todo use own arrow_type (like done in kquery)
}

impl Field {
    // Convert to Arrow's Field
    fn to_arrow(&self) -> ArrowField {
        ArrowField::new(&self.name, self.data_type.clone(), true)
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::schema::{Field, Schema};
    use arrow::datatypes::DataType;

    #[test]
    fn test_schema_conversions() {
        let field1 = Field {
            name: "test1".to_string(),
            data_type: DataType::Utf8,
        };
        let field2 = Field {
            name: "test2".to_string(),
            data_type: DataType::Int64,
        };
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
}
