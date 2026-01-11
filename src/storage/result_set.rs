use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::storage::row::Row;
use crate::types::column_value::ColumnValue;
use std::sync::Arc;

/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` wraps a `TableScan` and the associated `Table` metadata, allowing
/// iteration over rows and safe retrieval of column values by name.
pub struct ResultSet {
    table_scan: TableScan,
    table: Arc<Table>,
}

impl ResultSet {
    /// Creates a new `ResultSet`.
    ///
    /// # Arguments
    ///
    /// * `table_scan` - The iterator over the table's rows.
    /// * `table` - The table metadata, used for resolving column names to positions.
    pub(crate) fn new(table_scan: TableScan, table: Arc<Table>) -> Self {
        Self { table_scan, table }
    }

    /// Returns an iterator over the rows in the result set.
    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        self.table_scan.iter()
    }

    /// Retrieves the value of a specific column from a row.
    ///
    /// # Arguments
    ///
    /// * `row` - The row to retrieve the value from.
    /// * `column_name` - The name of the column to retrieve.
    ///
    /// # Returns
    ///
    /// * `Some(&ColumnValue)` - The value of the column if it exists.
    /// * `None` - If the column name is invalid.
    pub fn column<'a>(&self, row: &'a Row, column_name: &str) -> Option<&'a ColumnValue> {
        let column_position = self.table.schema().column_position(column_name)?;
        row.column_value_at(column_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schema::Schema;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;

    #[test]
    fn result_set() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![
            ColumnValue::Int(1),
            ColumnValue::Text("relop".to_string()),
        ]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ResultSet::new(table_scan, Arc::new(table));

        let rows: Vec<_> = result_set.iter().collect();
        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();
        assert_eq!(&ColumnValue::Int(1), result_set.column(row, "id").unwrap());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            result_set.column(row, "name").unwrap()
        );
    }

    #[test]
    fn attempt_to_get_non_existent_column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(1)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ResultSet::new(table_scan, Arc::new(table));

        let rows: Vec<_> = result_set.iter().collect();
        let row = rows.first().unwrap();

        assert!(result_set.column(row, "name").is_none());
    }
}
