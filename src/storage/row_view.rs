use crate::schema::Schema;
use crate::storage::row::Row;
use crate::types::column_value::ColumnValue;

/// A read-only view over a single row, bound to a table's schema.
///
/// `RowView` provides **name-based access** to column values without exposing
/// internal storage details such as column positions or row layout.
///
/// It pairs:
/// - a concrete [`Row`] containing the actual values, and
/// - a reference to the corresponding [`Schema`] used to resolve column names.
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
    schema: &'a Schema,
    visible_positions: &'a [usize],
}

impl<'a> RowView<'a> {
    /// Creates a new `RowView` for the given row and table.
    ///
    /// # Arguments
    ///
    /// * `row` - The row containing column values.
    /// * `schema` - The schema which defines the column layout.
    pub(crate) fn new(row: Row, schema: &'a Schema, visible_positions: &'a [usize]) -> Self {
        Self {
            row,
            schema,
            visible_positions,
        }
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
        let column_position = self.schema.column_position(column_name)?;
        if self.visible_positions.contains(&column_position) {
            return self.row.column_value_at(column_position);
        }
        None
    }
    /// Projects the row view to a new set of visible positions.
    pub(crate) fn project(self, visible_positions: &'a [usize]) -> Self {
        Self {
            row: self.row,
            schema: self.schema,
            visible_positions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
        let row = Row::filled(vec![ColumnValue::Int(200)]);

        let visible_positions = vec![0];
        let view = RowView::new(row, &schema, &visible_positions);
        assert_eq!(&ColumnValue::Int(200), view.column("id").unwrap());
    }

    #[test]
    fn attempt_to_get_non_existing_column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
        let row = Row::filled(vec![ColumnValue::Int(200)]);

        let visible_positions = vec![0];
        let view = RowView::new(row, &schema, &visible_positions);
        assert!(view.column("name").is_none());
    }

    #[test]
    fn attempt_to_get_a_column_not_in_visible_position() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();
        let row = Row::filled(vec![
            ColumnValue::Int(200),
            ColumnValue::Text("relop".to_string()),
        ]);

        let visible_positions = vec![1];
        let view = RowView::new(row, &schema, &visible_positions);
        assert!(view.column("id").is_none());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            view.column("name").unwrap()
        );
    }
    #[test]
    fn project_row_view() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let row = Row::filled(vec![
            ColumnValue::Int(200),
            ColumnValue::Text("relop".to_string()),
        ]);

        let visible_positions = vec![0, 1];
        let view = RowView::new(row, &schema, &visible_positions);
        assert_eq!(&ColumnValue::Int(200), view.column("id").unwrap());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            view.column("name").unwrap()
        );

        let projection = vec![1];
        let projected_view = view.project(&projection);
        assert!(projected_view.column("id").is_none());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            projected_view.column("name").unwrap()
        );
    }
}
