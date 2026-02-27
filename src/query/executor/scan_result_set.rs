use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::schema::Schema;
use crate::storage::row_filter::{NoFilter, RowFilter};
use crate::storage::row_view::RowView;
use std::sync::Arc;

/// A `ResultSet` implementation that scans an entire table.
///
/// `ScanResultsSet` holds a reference to the table data via `TableScan` (the owner)
/// and produces iterators that view all rows in the table.
pub struct ScanResultsSet<F: RowFilter = NoFilter> {
    table_scan: TableScan<F>,
    visible_positions: Arc<Vec<usize>>,
    prefixed_schema: Schema,
}

impl<F: RowFilter> ScanResultsSet<F> {
    /// Creates a new `ScanResultsSet` for the given table.
    ///
    /// # Arguments
    ///
    /// * `table_scan` - The owner of the table data.
    /// * `table` - The metadata of the table (schema, etc.).
    /// * `alias` - The optional alias for the table.
    pub(crate) fn new(table_scan: TableScan<F>, table: Arc<Table>, alias: Option<String>) -> Self {
        let base_schema = table.schema_ref();
        let column_positions = (0..base_schema.column_count()).collect();
        let prefix = alias.unwrap_or_else(|| table.name().to_string());
        let prefixed_schema = base_schema.with_prefix(&prefix);

        Self {
            table_scan,
            visible_positions: Arc::new(column_positions),
            prefixed_schema,
        }
    }
}

impl<F: RowFilter + 'static> ResultSet for ScanResultsSet<F> {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        // We call .iter() on TableScan, which returns a TableIterator.
        // We map that iterator to RowView.
        Ok(Box::new(self.table_scan.iter().map(move |row| {
            Ok(RowView::new(
                row,
                &self.prefixed_schema,
                &self.visible_positions,
            ))
        })))
    }

    fn schema(&self) -> &Schema {
        &self.prefixed_schema
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::table::Table;
    use crate::catalog::table_scan::TableScan;
    use crate::query::executor::scan_result_set::ScanResultsSet;
    use std::sync::Arc;

    use super::*;
    use crate::storage::row::Row;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, row, schema};

    #[test]
    fn scan_result_set() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table), None);

        let mut iterator = result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn scan_result_set_with_a_filter() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);
        table_store.insert(row![2, "query"]);

        struct MatchingRelopFilter;
        impl RowFilter for MatchingRelopFilter {
            fn matches(&self, row: &Row) -> bool {
                row.column_value_at(1).unwrap().text_value().unwrap() == "relop"
            }
        }

        let table_scan = TableScan::with_filter(Arc::new(table_store), MatchingRelopFilter);
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table), None);

        let mut iterator = result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn attempt_to_get_result_set_with_non_existent_column() {
        let table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let table_store = TableStore::new();
        table_store.insert(row![1]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table), None);

        let mut iterator = result_set.iterator().unwrap();
        assert_next_row!(iterator.as_mut(), !"name");
    }

    #[test]
    fn schema() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table), None);

        assert_eq!(
            result_set.schema().column_names(),
            vec!["employees.id", "employees.name"]
        );
    }

    #[test]
    fn schema_with_alias() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table), Some("e".to_string()));

        assert_eq!(result_set.schema().column_names(), vec!["e.id", "e.name"]);
    }
}
