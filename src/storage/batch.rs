use crate::schema::error::SchemaError;
use crate::schema::Schema;
use crate::storage::error::BatchError;
use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
use crate::storage::row::Row;
use std::collections::HashSet;

/// Represents a collection of rows to be processed together.
///
/// Batches are used for bulk insertion operations to improve performance
/// and allow for atomic validation of multiple rows.
pub struct Batch {
    rows: Vec<Row>,
}

impl Batch {
    /// Creates a new `Batch` from a vector of rows.
    ///
    /// # Arguments
    ///
    /// * `rows` - A `Vec<Row>` containing the rows to include in the batch.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::storage::batch::Batch;
    /// use relop::storage::row::Row;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let rows = vec![
    ///     Row::filled(vec![ColumnValue::int(1)]),
    ///     Row::filled(vec![ColumnValue::int(2)]),
    /// ];
    /// let batch = Batch::new(rows);
    /// ```
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    /// Checks if the rows in the batch are compatible with the table schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema to validate against.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all rows are compatible.
    /// * `Err(SchemaError)` - If a row has a column count mismatch or type mismatch.
    pub(crate) fn check_type_compatability(&self, schema: &Schema) -> Result<(), SchemaError> {
        for row in &self.rows {
            schema.check_type_compatability(row.column_values())?
        }
        Ok(())
    }

    /// Identifies unique primary key values within the batch.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema containing the primary key definition.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<PrimaryKeyColumnValues>)` - A vector of unique primary key values.
    /// * `Err(BatchError)` - If the batch contains duplicate primary keys.
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

    /// Consumes the `Batch` and returns the contained rows.
    pub(crate) fn into_rows(self) -> Vec<Row> {
        self.rows
    }
}

impl From<Vec<Row>> for Batch {
    /// Converts a `Vec<Row>` into a `Batch`.
    ///
    /// # Arguments
    ///
    /// * `rows` - The vector of rows to be converted.
    fn from(rows: Vec<Row>) -> Self {
        Batch::new(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::error::SchemaError;
    use crate::schema::primary_key::PrimaryKey;
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

        let batch = Batch::new(vec![Row::filled(vec![ColumnValue::int(10)])]);
        let result = batch.check_type_compatability(&schema);

        assert!(matches!(
            result,
            Err(SchemaError::ColumnCountMismatch {expected, actual}) if expected == 2 && actual == 1
        ))
    }

    #[test]
    fn batch_with_incompatible_column_values() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let batch = Batch::new(vec![Row::filled(vec![ColumnValue::text("relop")])]);
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
            Row::filled(vec![ColumnValue::int(1)]),
            Row::filled(vec![ColumnValue::int(2)]),
        ]);

        let all_primary_key_column_values = batch.unique_primary_key_values(&schema).unwrap();
        assert_eq!(2, all_primary_key_column_values.len());

        let primary_key_column_values = all_primary_key_column_values.first().unwrap();
        assert_eq!(&[ColumnValue::int(1)], primary_key_column_values.values());

        let primary_key_column_values = all_primary_key_column_values.last().unwrap();
        assert_eq!(&[ColumnValue::int(2)], primary_key_column_values.values());
    }

    #[test]
    fn duplicate_primary_key_values() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_primary_key(PrimaryKey::single("id"))
            .unwrap();

        let batch = Batch::new(vec![
            Row::filled(vec![ColumnValue::int(1)]),
            Row::filled(vec![ColumnValue::int(1)]),
        ]);

        assert!(batch.unique_primary_key_values(&schema).is_err());
    }
}
