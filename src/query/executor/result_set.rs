use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::query::executor::error::ExecutionError;
use crate::query::parser::ordering_key::OrderingKey;
use crate::query::plan::predicate::Predicate;
use crate::schema::Schema;
use crate::storage::row_view::{RowView, RowViewComparator};
use std::sync::Arc;

/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` wraps a `TableIterator` and the associated `Table` metadata, allowing
/// iteration over rows and safe retrieval of column values by name.
/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` acts as a factory for iterators. It owns the underlying data source (like `TableScan`),
/// enabling multiple iterations or consistent views.
///
/// # Design Decisions
///
/// `ResultSet` is designed as a factory for iterators rather than an iterator itself.
///
/// This separation decouples the ownership of the query result data from the specific state of iteration.
///
/// Consequently, this design:
/// - **Avoids Self-Referential Structs**: It prevents issues where a struct would need to hold both the data owner (`TableScan`) and the iterator that borrows from it.
/// - **Enables Thread Safety**: `ResultSet` remains immutable and can be safely shared across threads.
/// - **Allows Multiple Passes**: Consumers can create multiple independent iterators over the same result set.
pub trait ResultSet {
    // Return a boxed iterator that yields Result<RowView, ...>
    // The iterator is bound by the lifetime of &self
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError>;

    fn schema(&self) -> &Schema;
}

/// Represents the result for an individual RowView.
pub type RowViewResult<'a> = Result<RowView<'a>, ExecutionError>;

/// A `ResultSet` implementation that scans an entire table.
///
/// `ScanResultsSet` holds a reference to the table data via `TableScan` (the owner)
/// and produces iterators that view all rows in the table.
pub struct ScanResultsSet {
    table_scan: TableScan,
    table: Arc<Table>,
    visible_positions: Arc<Vec<usize>>,
}

impl ScanResultsSet {
    /// Creates a new `ScanResultsSet` for the given table.
    ///
    /// # Arguments
    ///
    /// * `table_scan` - The owner of the table data.
    /// * `table` - The metadata of the table (schema, etc.).
    pub(crate) fn new(table_scan: TableScan, table: Arc<Table>) -> Self {
        let column_positions = (0..table.schema_ref().column_count()).collect();
        Self {
            table_scan,
            table,
            visible_positions: Arc::new(column_positions),
        }
    }
}

impl ResultSet for ScanResultsSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        // We call .iter() on TableScan, which returns a TableIterator (the iterator).
        // We map that iterator to RowView.
        Ok(Box::new(self.table_scan.iter().map(move |row| {
            Ok(RowView::new(
                row,
                self.table.schema_ref(),
                &self.visible_positions,
            ))
        })))
    }

    fn schema(&self) -> &Schema {
        self.table.schema_ref()
    }
}

/// A `ResultSet` implementation that applies a projection (column selection)
/// to an underlying `ResultSet`.
///
/// `ProjectResultSet` wraps another `ResultSet` and filters the columns visible
/// in the produced `RowView`s.
pub struct ProjectResultSet {
    inner: Box<dyn ResultSet>,
    visible_positions: Vec<usize>,
}

impl ProjectResultSet {
    /// Creates a new `ProjectResultSet`.
    ///
    /// # Arguments
    ///
    /// * `inner` - The source `ResultSet` to project from.
    /// * `columns` - The list of column names to include in the projection.
    ///
    /// # Returns
    ///
    /// * `Ok(ProjectResultSet)` if all specified columns exist in the source schema.
    /// * `Err(ExecutionError::UnknownColumn)` if any column is not found.
    pub(crate) fn new<T: AsRef<str>>(
        inner: Box<dyn ResultSet>,
        columns: &[T],
    ) -> Result<ProjectResultSet, ExecutionError> {
        let schema = inner.schema();

        let positions = columns
            .iter()
            .map(|column_name| {
                schema
                    .column_position(column_name.as_ref())
                    .ok_or_else(|| ExecutionError::UnknownColumn(column_name.as_ref().to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ProjectResultSet {
            inner,
            visible_positions: positions,
        })
    }
}

/// A `ResultSet` implementation that filters rows based on a predicate.
///
/// `FilterResultSet` wraps another `ResultSet` and only yields rows that satisfy
/// the given `Predicate`.
pub struct FilterResultSet {
    inner: Box<dyn ResultSet>,
    predicate: Predicate,
}

impl FilterResultSet {
    /// Creates a new `FilterResultSet`.
    ///
    /// # Arguments
    ///
    /// * `inner` - The source `ResultSet` to filter.
    /// * `predicate` - The predicate to apply to each row.
    pub(crate) fn new(inner: Box<dyn ResultSet>, predicate: Predicate) -> Self {
        Self { inner, predicate }
    }
}

impl ResultSet for FilterResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        let inner_iterator = self.inner.iterator()?;
        let result = inner_iterator.filter_map(move |row_view_result| match row_view_result {
            Ok(row_view) => match self.predicate.matches(&row_view) {
                Ok(true) => Some(Ok(row_view)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            },
            Err(error) => Some(Err(error)),
        });
        Ok(Box::new(result))
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

impl ResultSet for ProjectResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        let inner_iterator = self.inner.iterator()?;
        Ok(Box::new(inner_iterator.map(move |row_view_result| {
            row_view_result.map(|row_view| row_view.project(&self.visible_positions))
        })))
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

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
    use super::*;
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::LogicalOperator;

    use crate::storage::table_store::TableStore;
    use crate::test_utils::{assert_row, create_schema};
    use crate::types::column_type::ColumnType;
    use crate::{row, rows};

    #[test]
    fn scan_result_set() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table));

        let mut iterator = result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("id", 1)
            .match_column("name", "relop");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn attempt_to_get_result_set_with_non_existent_column() {
        let table = Table::new("employees", create_schema(&[("id", ColumnType::Int)]));
        let table_store = TableStore::new();
        table_store.insert(row![1]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table));

        let mut iterator = result_set.iterator().unwrap();
        assert_row(iterator.as_mut()).does_not_have_column("name");
    }

    #[test]
    fn projected_result_set() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let projected_result_set = ProjectResultSet::new(result_set, &["name"]).unwrap();
        let mut iterator = projected_result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("name", "relop")
            .does_not_have_column("id");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn projected_result_set_with_filter() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let scan_result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));
        let filter_result_set = Box::new(FilterResultSet::new(
            scan_result_set,
            Predicate::comparison("id", LogicalOperator::Eq, Literal::Int(1)),
        ));
        let projected_result_set = ProjectResultSet::new(filter_result_set, &["name"]).unwrap();

        let mut iterator = projected_result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("name", "relop")
            .does_not_have_column("id");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn attempt_to_get_projected_result_set_with_non_existent_column() {
        let table = Table::new("employees", create_schema(&[("id", ColumnType::Int)]));
        let table_store = TableStore::new();
        table_store.insert(row![1]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let result = ProjectResultSet::new(result_set, &["name"]);
        assert!(
            matches!(result, Err(ExecutionError::UnknownColumn(column_name)) if column_name == "name"),
        );
    }

    #[test]
    fn filter_result_set() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let predicate = Predicate::comparison("id", LogicalOperator::Eq, Literal::Int(1));
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_row(iterator.as_mut()).match_column("id", 1);
        assert!(iterator.next().is_none());
    }

    #[test]
    fn filter_result_set_with_no_matching_rows() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let predicate = Predicate::comparison("id", LogicalOperator::Eq, Literal::Int(3));
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();
        assert!(iterator.next().is_none());
    }

    #[test]
    fn filter_result_set_with_string_comparison() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let predicate = Predicate::comparison(
            "name",
            LogicalOperator::Eq,
            Literal::Text("relop".to_string()),
        );
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_row(iterator.as_mut()).match_column("name", "relop");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_single_column_ascending() {
        let table = Table::new("employees", create_schema(&[("id", ColumnType::Int)]));
        let table_store = TableStore::new();
        table_store.insert_all(rows![[2], [1]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![OrderingKey::ascending_by("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        assert_row(iterator.as_mut()).match_column("id", 1);
        assert_row(iterator.as_mut()).match_column("id", 2);
        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_single_column_descending() {
        let table = Table::new("employees", create_schema(&[("id", ColumnType::Int)]));
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1], [2]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![OrderingKey::descending_by("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        assert_row(iterator.as_mut()).match_column("id", 2);
        assert_row(iterator.as_mut()).match_column("id", 1);
        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_multiple_columns_ascending() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("rank", ColumnType::Int)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, 20], [1, 10]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![
            OrderingKey::ascending_by("id"),
            OrderingKey::ascending_by("rank"),
        ];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("id", 1)
            .match_column("rank", 10);

        assert_row(iterator.as_mut())
            .match_column("id", 1)
            .match_column("rank", 20);

        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_with_limit() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("rank", ColumnType::Int)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[3, 30], [1, 10], [2, 20]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![OrderingKey::ascending_by("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);

        let limit_result_set = LimitResultSet::new(Box::new(ordering_result_set), 2);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_row(iterator.as_mut()).match_column("id", 1);
        assert_row(iterator.as_mut()).match_column("id", 2);
        assert!(iterator.next().is_none());
    }
    #[test]
    fn limit_result_set() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let limit_result_set = LimitResultSet::new(result_set, 1);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("id", 1)
            .match_column("name", "relop");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn limit_result_set_given_limit_higher_than_the_available_rows() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let limit_result_set = LimitResultSet::new(result_set, 4);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("id", 1)
            .match_column("name", "relop");

        assert_row(iterator.as_mut())
            .match_column("id", 2)
            .match_column("name", "query");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn limit_result_set_with_projection() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));
        let projected_result_set = ProjectResultSet::new(result_set, &["id"]).unwrap();

        let limit_result_set = LimitResultSet::new(Box::new(projected_result_set), 1);
        let mut iterator = limit_result_set.iterator().unwrap();

        assert_row(iterator.as_mut())
            .match_column("id", 1)
            .does_not_have_column("name");
        assert!(iterator.next().is_none());
    }

    #[test]
    fn schema() {
        let table = Table::new(
            "employees",
            create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]),
        );
        let table_store = TableStore::new();
        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table));

        assert_eq!(result_set.schema().column_names(), vec!["id", "name"]);
    }
}
