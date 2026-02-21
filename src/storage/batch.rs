use crate::schema::error::SchemaError;
use crate::schema::Schema;
use crate::storage::row::Row;

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
    use crate::rows;
    use crate::schema;
    use crate::schema::error::SchemaError;
    use crate::types::column_type::ColumnType;

    #[test]
    fn batch_with_incompatible_column_count() {
        let schema = schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap();
        let batch = Batch::new(rows![[10]]);
        let result = batch.check_type_compatability(&schema);

        assert!(matches!(
            result,
            Err(SchemaError::ColumnCountMismatch {expected, actual}) if expected == 2 && actual == 1
        ))
    }

    #[test]
    fn batch_with_incompatible_column_values() {
        let schema = schema!["id" => ColumnType::Int].unwrap();

        let batch = Batch::new(rows![["relop"]]);
        let result = batch.check_type_compatability(&schema);

        assert!(matches!(
            result,
            Err(SchemaError::ColumnTypeMismatch {column, expected, actual}) if column == "id" && expected == ColumnType::Int && actual == ColumnType::Text
        ))
    }
}
