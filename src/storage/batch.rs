use crate::schema::error::SchemaError;
use crate::schema::Schema;
use crate::storage::row::Row;

pub struct Batch {
    rows: Vec<Row>,
}

impl Batch {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    pub(crate) fn check_type_compatability(&self, schema: &Schema) -> Result<(), SchemaError> {
        for row in &self.rows {
            schema.check_type_compatability(row.column_values())?
        }
        Ok(())
    }

    pub(crate) fn into_rows(self) -> Vec<Row> {
        self.rows
    }
}

impl From<Vec<Row>> for Batch {
    fn from(rows: Vec<Row>) -> Self {
        Batch::new(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::error::SchemaError;
    use crate::schema::Schema;
    use crate::storage::row::Row;
    use crate::types::column_type::ColumnType;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn batch_with_incompatible_column_count() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let batch = Batch::new(vec![Row::filled(vec![ColumnValue::Int(10)])]);
        let result = batch.check_type_compatability(&schema);

        assert!(matches!(
            result,
            Err(SchemaError::ColumnCountMismatch {expected, actual}) if expected == 2 && actual == 1
        ))
    }

    #[test]
    fn batch_with_incompatible_column_values() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let batch = Batch::new(vec![Row::filled(vec![ColumnValue::Text(
            "relop".to_string(),
        )])]);
        let result = batch.check_type_compatability(&schema);

        assert!(matches!(
            result,
            Err(SchemaError::ColumnTypeMismatch {column, expected, actual}) if column == "id" && expected == ColumnType::Int && actual == ColumnType::Text
        ))
    }
}
