use crate::schema::error::SchemaError;
use crate::schema::primary_key::PrimaryKey;
use crate::schema::Schema;
use crate::storage::error::BatchError;
use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
use crate::storage::row::Row;
use std::collections::HashSet;

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

    pub(crate) fn unique_primary_key_values(
        &self,
        schema: &Schema,
    ) -> Result<Vec<PrimaryKeyColumnValues>, BatchError> {
        let mut seen = HashSet::new();

        if let Some(primary_key) = schema.primary_key() {
            for row in &self.rows {
                let primary_key_column_values =
                    PrimaryKeyColumnValues::new(row, primary_key, schema);

                if !seen.insert(primary_key_column_values) {
                    return Err(BatchError::DuplicatePrimaryKey);
                }
            }

            let mut values = Vec::with_capacity(self.rows.len());
            for row in &self.rows {
                values.push(PrimaryKeyColumnValues::new(row, primary_key, schema));
            }
            return Ok(values);
        }
        Ok(vec![])
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

    #[test]
    fn unique_primary_key_values() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_primary_key(PrimaryKey::single("id"))
            .unwrap();

        let batch = Batch::new(vec![
            Row::filled(vec![ColumnValue::Int(1)]),
            Row::filled(vec![ColumnValue::Int(2)]),
        ]);

        let all_primary_key_column_values = batch.unique_primary_key_values(&schema).unwrap();
        assert_eq!(2, all_primary_key_column_values.len());

        let primary_key_column_values = all_primary_key_column_values.first().unwrap();
        assert_eq!(&[ColumnValue::Int(1)], primary_key_column_values.values());

        let primary_key_column_values = all_primary_key_column_values.last().unwrap();
        assert_eq!(&[ColumnValue::Int(2)], primary_key_column_values.values());
    }

    #[test]
    fn duplicate_primary_key_values() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_primary_key(PrimaryKey::single("id"))
            .unwrap();

        let batch = Batch::new(vec![
            Row::filled(vec![ColumnValue::Int(1)]),
            Row::filled(vec![ColumnValue::Int(1)]),
        ]);

        assert!(batch.unique_primary_key_values(&schema).is_err());
    }
}
