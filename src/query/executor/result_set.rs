use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::query::executor::error::ExecutionError;
use crate::query::parser::ordering_key::OrderingKey;
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
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowView> + '_>, ExecutionError>;

    fn schema(&self) -> &Schema;
}

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
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowView> + '_>, ExecutionError> {
        // We call .iter() on TableScan, which returns a TableIterator (the iterator).
        // We map that iterator to RowView.
        Ok(Box::new(self.table_scan.iter().map(move |row| {
            RowView::new(row, self.table.schema_ref(), &self.visible_positions)
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

impl ResultSet for ProjectResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowView> + '_>, ExecutionError> {
        let inner_iterator = self.inner.iterator()?;
        Ok(Box::new(
            inner_iterator.map(|row_view| row_view.project(&self.visible_positions)),
        ))
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
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowView> + '_>, ExecutionError> {
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
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowView> + '_>, ExecutionError> {
        let comparator = RowViewComparator::new(self.schema(), &self.ordering_keys)?;
        let mut rows: Vec<RowView> = self.inner.iterator()?.collect();

        rows.sort_by(|left, right| comparator.compare(left, right));
        Ok(Box::new(rows.into_iter()))
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schema::Schema;
    use crate::storage::row::Row;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::types::column_value::ColumnValue;

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
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table));

        let mut iterator = result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            row_view.column("name").unwrap()
        );
        assert!(iterator.next().is_none());
    }

    #[test]
    fn attempt_to_get_non_existent_column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(1)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table));

        let mut iterator = result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert!(row_view.column("name").is_none());
    }

    #[test]
    fn projected_result_set() {
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
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let projected_result_set = ProjectResultSet::new(result_set, &["name"]).unwrap();

        let mut iterator = projected_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            row_view.column("name").unwrap()
        );
        assert!(row_view.column("id").is_none());
        assert!(iterator.next().is_none());
    }

    #[test]
    fn attempt_to_get_projected_result_set_with_non_existent_column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(1)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let result = ProjectResultSet::new(result_set, &["name"]);
        assert!(
            matches!(result, Err(ExecutionError::UnknownColumn(column_name)) if column_name == "name"),
        );
    }

    #[test]
    fn limit_result_set() {
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
        table_store.insert(Row::filled(vec![
            ColumnValue::Int(2),
            ColumnValue::Text("query".to_string()),
        ]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let limit_result_set = LimitResultSet::new(result_set, 1);
        let mut iterator = limit_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            row_view.column("name").unwrap()
        );
        assert!(iterator.next().is_none());
    }

    #[test]
    fn limit_result_set_given_limit_higher_than_the_available_rows() {
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
        table_store.insert(Row::filled(vec![
            ColumnValue::Int(2),
            ColumnValue::Text("query".to_string()),
        ]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let limit_result_set = LimitResultSet::new(result_set, 4);
        let mut iterator = limit_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());
        assert_eq!(
            &ColumnValue::Text("relop".to_string()),
            row_view.column("name").unwrap()
        );

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(2), row_view.column("id").unwrap());
        assert_eq!(
            &ColumnValue::Text("query".to_string()),
            row_view.column("name").unwrap()
        );
        assert!(iterator.next().is_none());
    }

    #[test]
    fn limit_result_set_with_projection() {
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
        table_store.insert(Row::filled(vec![
            ColumnValue::Int(2),
            ColumnValue::Text("query".to_string()),
        ]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));
        let projected_result_set = ProjectResultSet::new(result_set, &["id"]).unwrap();

        let limit_result_set = LimitResultSet::new(Box::new(projected_result_set), 1);
        let mut iterator = limit_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());
        assert!(row_view.column("name").is_none());
        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_single_column_ascending() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(2)]));
        table_store.insert(Row::filled(vec![ColumnValue::Int(1)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![OrderingKey::ascending_by("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(2), row_view.column("id").unwrap());

        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_single_column_descending() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(1)]));
        table_store.insert(Row::filled(vec![ColumnValue::Int(2)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![OrderingKey::descending_by("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(2), row_view.column("id").unwrap());

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());

        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_multiple_columns_ascending() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("rank", ColumnType::Int)
            .unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(1), ColumnValue::Int(20)]));
        table_store.insert(Row::filled(vec![ColumnValue::Int(1), ColumnValue::Int(10)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![
            OrderingKey::ascending_by("id"),
            OrderingKey::ascending_by("rank"),
        ];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);
        let mut iterator = ordering_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());
        assert_eq!(&ColumnValue::Int(10), row_view.column("rank").unwrap());

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());
        assert_eq!(&ColumnValue::Int(20), row_view.column("rank").unwrap());

        assert!(iterator.next().is_none());
    }

    #[test]
    fn ordering_result_set_with_limit() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("rank", ColumnType::Int)
            .unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert(Row::filled(vec![ColumnValue::Int(3), ColumnValue::Int(30)]));
        table_store.insert(Row::filled(vec![ColumnValue::Int(1), ColumnValue::Int(10)]));
        table_store.insert(Row::filled(vec![ColumnValue::Int(2), ColumnValue::Int(20)]));

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table)));

        let ordering_keys = vec![OrderingKey::ascending_by("id")];
        let ordering_result_set = OrderingResultSet::new(result_set, ordering_keys);

        let limit_result_set = LimitResultSet::new(Box::new(ordering_result_set), 2);
        let mut iterator = limit_result_set.iterator().unwrap();

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(1), row_view.column("id").unwrap());

        let row_view = iterator.next().unwrap();
        assert_eq!(&ColumnValue::Int(2), row_view.column("id").unwrap());

        assert!(iterator.next().is_none());
    }
    #[test]
    fn schema() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(table_scan, Arc::new(table));

        assert_eq!(result_set.schema().column_names(), vec!["id", "name"]);
    }
}
