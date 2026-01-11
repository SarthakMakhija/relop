use crate::catalog::table::Table;
use crate::storage::row::Row;
use crate::types::column_value::ColumnValue;

/// A read-only view over a single row, bound to a table's schema.
///
/// `RowView` provides **name-based access** to column values without exposing
/// internal storage details such as column positions or row layout.
///
/// It pairs:
/// - a concrete [`Row`] containing the actual values, and
/// - a reference to the corresponding [`Table`] used to resolve column names.
///
/// This abstraction is primarily used by query execution results (e.g. `SELECT *`)
/// to allow clients to retrieve column values by name instead of index.
///
/// # Notes
///
/// - Column lookups are resolved via the table schema at runtime.
/// - No cloning of column values occurs; returned values are borrowed.
/// - `RowView` is intentionally read-only.
pub struct RowView<'a> {
    row: Row,
    table: &'a Table,
}

impl<'a> RowView<'a> {
    /// Creates a new `RowView` for the given row and table.
    ///
    /// # Arguments
    ///
    /// * `row` - The row containing column values.
    /// * `table` - The table whose schema defines the column layout.
    pub(crate) fn new(row: Row, table: &'a Table) -> Self {
        Self { row, table }
    }

    /// Retrieves the value of a column by name.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to retrieve.
    ///
    /// # Returns
    ///
    /// * `Some(&ColumnValue)` if the column exists.
    /// * `None` if the column name is not part of the table schema.
    ///
    /// # Notes
    ///
    /// - Column name resolution is case-sensitive.
    /// - This method performs a schema lookup on each call.
    pub fn column(&self, column_name: &str) -> Option<&ColumnValue> {
        let column_position = self.table.schema().column_position(column_name)?;
        self.row.column_value_at(column_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn column() {
        let table = Table::new(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        let row = Row::filled(vec![ColumnValue::Int(200)]);

        let view = RowView::new(row, &table);
        assert_eq!(&ColumnValue::Int(200), view.column("id").unwrap());
    }

    #[test]
    fn attempt_to_get_non_existing_column() {
        let table = Table::new(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        let row = Row::filled(vec![ColumnValue::Int(200)]);

        let view = RowView::new(row, &table);
        assert!(view.column("name").is_none());
    }
}
