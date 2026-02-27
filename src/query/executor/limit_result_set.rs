use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::schema::Schema;

/// A `ResultSet` implementation that limits the number of rows returned.
///
/// `LimitResultSet` wraps another `ResultSet` and ensures that at most `limit` rows
/// are yielded during iteration.
pub struct LimitResultSet {
    inner: Box<dyn ResultSet>,
    limit: usize,
}

impl LimitResultSet {
    /// Creates a new `LimitResultSet`.
    ///
    /// # Arguments
    ///
    /// * `inner` - The source `ResultSet` to limit.
    /// * `limit` - The maximum number of rows to return.
    pub(crate) fn new(inner: Box<dyn ResultSet>, limit: usize) -> Self {
        Self { inner, limit }
    }
}

impl ResultSet for LimitResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        let inner_iterator = self.inner.iterator()?;
        Ok(Box::new(inner_iterator.take(self.limit)))
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use crate::query::executor::project_result_set::ProjectResultSet;

    use crate::catalog::table::Table;
    use crate::catalog::table_scan::TableScan;
    use crate::query::executor::scan_result_set::ScanResultsSet;
    use std::sync::Arc;

    use super::*;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, rows, schema};

    #[test]
    fn limit_result_set() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let limit_result_set = LimitResultSet::new(result_set, 1);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn limit_result_set_given_limit_higher_than_the_available_rows() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let limit_result_set = LimitResultSet::new(result_set, 4);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_next_row!(iterator.as_mut(), "id" => 2, "name" => "query");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn limit_result_set_with_projection() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));
        let projected_result_set = ProjectResultSet::new(result_set, &["id"]).unwrap();

        let limit_result_set = LimitResultSet::new(Box::new(projected_result_set), 1);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1, ! "name");
        assert_no_more_rows!(iterator.as_mut());
    }
}
