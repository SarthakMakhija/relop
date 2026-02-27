use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::query::parser::ordering_key::OrderingKey;
use crate::schema::Schema;
use crate::storage::row_view::{RowView, RowViewComparator};

/// A `ResultSet` implementation that orders rows based on specified criteria.
///
/// `OrderingResultSet` wraps another `ResultSet`, consumes all its rows, sorts them
/// in memory using the provided `ordering_keys`, and yields them in sorted order.
///
/// # Note
///
/// This implementation performs an **in-memory sort**, meaning it buffers all rows
/// from the inner result set before yielding the first row.
pub struct OrderingResultSet {
    inner: Box<dyn ResultSet>,
    ordering_keys: Vec<OrderingKey>,
}

impl OrderingResultSet {
    /// Creates a new `OrderingResultSet`.
    ///
    /// # Arguments
    ///
    /// * `inner` - The source `ResultSet` to sort.
    /// * `ordering_keys` - Examples of keys defining the sort order.
    pub fn new(inner: Box<dyn ResultSet>, ordering_keys: Vec<OrderingKey>) -> Self {
        Self {
            inner,
            ordering_keys,
        }
    }
}

impl ResultSet for OrderingResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        let comparator = RowViewComparator::new(self.schema(), &self.ordering_keys)?;
        let iterator = self.inner.iterator()?;

        let mut rows: Vec<RowView> = Vec::new();
        for result in iterator {
            match result {
                Ok(row_view) => rows.push(row_view),
                Err(err) => return Err(err),
            }
        }
        rows.sort_by(|left, right| comparator.compare(left, right));
        Ok(Box::new(rows.into_iter().map(Ok)))
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use crate::query::executor::limit_result_set::LimitResultSet;

    use crate::catalog::table::Table;
    use crate::catalog::table_scan::TableScan;
    use crate::query::executor::scan_result_set::ScanResultsSet;
    use std::sync::Arc;

    use super::*;
    use crate::query::executor::test_utils::ErrorResultSet;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::{asc, assert_next_row, assert_no_more_rows, desc, rows, schema};

    #[test]
    fn ordering_result_set_single_column_ascending() {
        let table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let table_store = TableStore::new();
        table_store.insert_all(rows![[2], [1]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let ordering_keys = vec![asc!("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1);
        assert_next_row!(iterator.as_mut(), "id" => 2);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn ordering_result_set_single_column_descending() {
        let table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1], [2]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let ordering_keys = vec![desc!("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 2);
        assert_next_row!(iterator.as_mut(), "id" => 1);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn ordering_result_set_multiple_columns_ascending() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, 20], [1, 10]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let ordering_keys = vec![asc!("id"), asc!("rank")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1, "rank" => 10);
        assert_next_row!(iterator.as_mut(), "id" => 1, "rank" => 20);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn ordering_result_set_with_limit() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[3, 30], [1, 10], [2, 20]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let ordering_keys = vec![asc!("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);

        let limit_result_set = LimitResultSet::new(Box::new(ordering_result_set), 2);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1);
        assert_next_row!(iterator.as_mut(), "id" => 2);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn ordering_result_set_with_unknown_column_fails() {
        let table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let table_store = TableStore::new();
        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let ordering_keys = vec![asc!("unknown")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let result = ordering_result_set.iterator();

        assert!(matches!(
            result,
            Err(ExecutionError::UnknownColumn(column)) if column == "unknown"
        ));
    }

    #[test]
    fn ordering_result_set_with_error_during_buffering() {
        let schema = Arc::new(schema!["id" => ColumnType::Int].unwrap());
        let result_set = Box::new(ErrorResultSet {
            schema: schema.clone(),
        });

        let ordering_keys = vec![asc!("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let result = ordering_result_set.iterator();

        assert!(matches!(
            result,
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }
}
