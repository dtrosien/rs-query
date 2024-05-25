use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use std::collections::HashMap;

pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    // Convert from Arrow's Schema
    fn from_arrow(arrow_schema: &ArrowSchema) -> Self {
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
    fn to_arrow(&self) -> ArrowSchema {
        let fields: Vec<_> = self.fields.iter().map(|f| f.to_arrow()).collect();
        ArrowSchema::new(fields)
    }

    // Project schema by field indices
    fn project(&self, indices: Vec<usize>) -> Schema {
        let fields = indices
            .into_iter()
            .map(|i| self.fields[i].clone())
            .collect();
        Schema { fields }
    }

    // Select schema by field names
    fn select(&self, names: Vec<&str>) -> Result<Schema, &'static str> {
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
struct Field {
    pub name: String,
    data_type: DataType,
}

impl Field {
    // Convert to Arrow's Field
    fn to_arrow(&self) -> ArrowField {
        ArrowField::new(&self.name, self.data_type.clone(), true)
    }
}
