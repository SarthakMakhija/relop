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
    visible_positions: Arc<Vec<usize>>,
    prefixed_schema: Schema,
}

impl ScanResultsSet {
    /// Creates a new `ScanResultsSet` for the given table.
    ///
    /// # Arguments
    ///
    /// * `table_scan` - The owner of the table data.
    /// * `table` - The metadata of the table (schema, etc.).
    /// * `alias` - The optional alias for the table.
    pub(crate) fn new(table_scan: TableScan, table: Arc<Table>, alias: Option<String>) -> Self {
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

impl ResultSet for ScanResultsSet {
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
                    .map_err(ExecutionError::Schema)?
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

/// A `ResultSet` implementation that performs a nested loop join between two `ResultSet`s.
///
/// For multi-table joins like `(A JOIN B) JOIN C`, the execution forms a tree where each node
/// is a `NestedLoopJoinResultSet` (or a `ScanResultsSet` at the leaves).
///
/// ### Execution Flow (Recursive Iterators):
/// ```text
///            [Outer Join Iterator]
///               /            \
///      left_iterator: Pulls  right_result_set: Resets and iterates
///      rows from Inner Join  for every left row.
///             /
///     [Inner Join Iterator]
///        /            \
///   left: Pulls from A  right: Resets/Iterates for B
/// ```
///
/// 1. The **Outer Join Iterator** calls `next()` on its `left_iterator` (the Inner Join).
/// 2. The **Inner Join Iterator** pulls a row from `A`, resets `B`, and returns the first `A+B` row.
/// 3. The **Outer Join Iterator** receives `A+B`, resets `C`, and combines `A+B` with each row of `C`.
/// 4. This process repeats, effectively creating a 3-level deep nested loop without the outer
///    nodes needing to know the internal structure of their children.
pub struct NestedLoopJoinResultSet {
    left: Box<dyn ResultSet>,
    right: Box<dyn ResultSet>,
    on: Option<Predicate>,
    merged_schema: Schema,
    visible_positions: Arc<Vec<usize>>,
}

impl NestedLoopJoinResultSet {
    pub(crate) fn new(
        left: Box<dyn ResultSet>,
        right: Box<dyn ResultSet>,
        on: Option<Predicate>,
    ) -> Self {
        let merged_schema = left
            .schema()
            .merge_with_prefixes(None, right.schema(), None);
        let visible_positions = Arc::new((0..merged_schema.column_count()).collect());
        Self {
            left,
            right,
            on,
            merged_schema,
            visible_positions,
        }
    }
}

impl ResultSet for NestedLoopJoinResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        let left_iterator = self.left.iterator()?;
        Ok(Box::new(JoinIterator::new(
            left_iterator,
            self.right.as_ref(),
            self.on.as_ref(),
            &self.merged_schema,
            &self.visible_positions,
        )))
    }

    fn schema(&self) -> &Schema {
        &self.merged_schema
    }
}

/// An iterator that performs a nested loop join between two iterators.
struct JoinIterator<'a> {
    left_iterator: Box<dyn Iterator<Item = RowViewResult<'a>> + 'a>,
    right_result_set: &'a dyn ResultSet,
    on: Option<&'a Predicate>,
    merged_schema: &'a Schema,
    visible_positions: &'a [usize],
    current_left_row_view: Option<RowView<'a>>,
    current_right_iterator: Option<Box<dyn Iterator<Item = RowViewResult<'a>> + 'a>>,
}

impl<'a> JoinIterator<'a> {
    fn new(
        left_iterator: Box<dyn Iterator<Item = RowViewResult<'a>> + 'a>,
        right_result_set: &'a dyn ResultSet,
        on: Option<&'a Predicate>,
        merged_schema: &'a Schema,
        visible_positions: &'a [usize],
    ) -> Self {
        Self {
            left_iterator,
            right_result_set,
            on,
            merged_schema,
            visible_positions,
            current_left_row_view: None,
            current_right_iterator: None,
        }
    }
}

impl<'a> Iterator for JoinIterator<'a> {
    type Item = RowViewResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_left_row_view.is_none() {
                match self.left_iterator.next() {
                    Some(Ok(left_row_view)) => {
                        self.current_left_row_view = Some(left_row_view);
                        match self.right_result_set.iterator() {
                            Ok(iterator) => self.current_right_iterator = Some(iterator),
                            Err(err) => return Some(Err(err)),
                        }
                    }
                    Some(Err(err)) => return Some(Err(err)),
                    None => return None,
                }
            }

            if let Some(ref mut right_iterator) = self.current_right_iterator {
                match right_iterator.next() {
                    Some(Ok(right_row_view)) => {
                        let left_row_view = self.current_left_row_view.as_ref().unwrap();
                        let merged_row = left_row_view.merge(&right_row_view);
                        let merged_row_view =
                            RowView::new(merged_row, self.merged_schema, self.visible_positions);

                        if let Some(predicate) = self.on {
                            match predicate.matches(&merged_row_view) {
                                Ok(true) => return Some(Ok(merged_row_view)),
                                Ok(false) => continue,
                                Err(err) => return Some(Err(err)),
                            }
                        }
                        return Some(Ok(merged_row_view));
                    }
                    Some(Err(err)) => return Some(Err(err)),
                    None => {
                        self.current_left_row_view = None;
                        self.current_right_iterator = None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::LogicalOperator;

    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;

    use crate::{asc, assert_next_row, assert_no_more_rows, desc, row, rows, schema};

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
    fn projected_result_set() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let projected_result_set = ProjectResultSet::new(result_set, &["name"]).unwrap();
        let mut iterator = projected_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "name" => "relop", ! "id");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn projected_result_set_with_filter() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert(row![1, "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let scan_result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));
        let filter_result_set = Box::new(FilterResultSet::new(
            scan_result_set,
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
        ));
        let projected_result_set = ProjectResultSet::new(filter_result_set, &["name"]).unwrap();

        let mut iterator = projected_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "name" => "relop", ! "id");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn attempt_to_get_projected_result_set_with_non_existent_column() {
        let table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let table_store = TableStore::new();
        table_store.insert(row![1]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let result = ProjectResultSet::new(result_set, &["name"]);
        assert!(
            matches!(result, Err(ExecutionError::UnknownColumn(column_name)) if column_name == "name"),
        );
    }

    #[test]
    fn filter_result_set() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate = Predicate::comparison(
            Literal::ColumnReference("id".to_string()),
            LogicalOperator::Eq,
            Literal::Int(1),
        );
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 1);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn filter_result_set_with_no_matching_rows() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate = Predicate::comparison(
            Literal::ColumnReference("id".to_string()),
            LogicalOperator::Eq,
            Literal::Int(3),
        );
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn filter_result_set_with_string_comparison() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate = Predicate::comparison(
            Literal::ColumnReference("name".to_string()),
            LogicalOperator::Eq,
            Literal::Text("relop".to_string()),
        );
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn filter_result_set_with_and_predicate() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"], [3, "relop"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Greater,
                Literal::Int(1),
            ),
            Predicate::comparison(
                Literal::ColumnReference("name".to_string()),
                LogicalOperator::Eq,
                Literal::Text("relop".to_string()),
            ),
        ]);
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "id" => 3, "name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn filter_result_set_with_and_predicate_no_match() {
        let table = Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1, "relop"], [2, "query"], [3, "rust"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Greater,
                Literal::Int(1),
            ),
            Predicate::comparison(
                Literal::ColumnReference("name".to_string()),
                LogicalOperator::Eq,
                Literal::Text("relop".to_string()),
            ),
        ]);
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_no_more_rows!(iterator.as_mut());
    }

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

    #[test]
    fn filter_result_set_with_column_to_column_comparison() {
        let table = Table::new(
            "employees",
            schema!["first_name" => ColumnType::Text, "last_name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert_all(rows![["relop", "relop"], ["relop", "query"]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate = Predicate::comparison(
            Literal::ColumnReference("first_name".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("last_name".to_string()),
        );
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "first_name" => "relop", "last_name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn filter_result_set_with_literal_comparison() {
        let table = Table::new(
            "employees",
            schema!["first_name" => ColumnType::Text, "last_name" => ColumnType::Text].unwrap(),
        );
        let table_store = TableStore::new();
        table_store.insert(row!["relop", "relop"]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let predicate =
            Predicate::comparison(Literal::Int(1), LogicalOperator::Eq, Literal::Int(1));
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "first_name" => "relop", "last_name" => "relop");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn filter_result_set_with_error() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let result_set = Box::new(ErrorResultSet {
            schema: Arc::new(schema),
        });

        let predicate = Predicate::comparison(
            Literal::ColumnReference("id".to_string()),
            LogicalOperator::Eq,
            Literal::Int(1),
        );
        let filter_result_set = FilterResultSet::new(result_set, predicate);
        let mut iterator = filter_result_set.iterator().unwrap();

        assert!(matches!(
            iterator.next(),
            Some(Err(ExecutionError::TypeMismatchInComparison))
        ));
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

    #[test]
    fn project_result_set_with_ambiguous_column_fails() {
        let table_store = TableStore::new();
        let table_scan = TableScan::new(Arc::new(table_store));
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();

        let table = Table::new("combined", schema);
        let result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        let columns = vec!["id".to_string()];
        let project_result_set = ProjectResultSet::new(result_set, &columns);

        assert!(matches!(
            project_result_set,
            Err(ExecutionError::Schema(schema::error::SchemaError::AmbiguousColumnName(ref column_name))) if column_name == "id"
        ));
    }

    #[test]
    fn join_result_sets_cross_product() {
        let employees_table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let employees_store = TableStore::new();
        employees_store.insert_all(rows![[1], [2]]);

        let employees_scan = TableScan::new(Arc::new(employees_store));
        let employees_result_set = Box::new(ScanResultsSet::new(
            employees_scan,
            Arc::new(employees_table),
            None,
        ));

        let departments_table =
            Table::new("departments", schema!["name" => ColumnType::Text].unwrap());
        let departments_store = TableStore::new();
        departments_store.insert_all(rows![["Engineering"], ["Sales"]]);

        let departments_scan = TableScan::new(Arc::new(departments_store));
        let departments_result_set = Box::new(ScanResultsSet::new(
            departments_scan,
            Arc::new(departments_table),
            None,
        ));

        let join_result_set =
            NestedLoopJoinResultSet::new(employees_result_set, departments_result_set, None);
        let mut iterator = join_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "employees.id" => 1, "departments.name" => "Engineering");
        assert_next_row!(iterator.as_mut(), "employees.id" => 1, "departments.name" => "Sales");
        assert_next_row!(iterator.as_mut(), "employees.id" => 2, "departments.name" => "Engineering");
        assert_next_row!(iterator.as_mut(), "employees.id" => 2, "departments.name" => "Sales");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn join_result_sets_inner_join_with_predicate() {
        let employees_table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let employees_store = TableStore::new();
        employees_store.insert_all(rows![[1], [2]]);

        let employees_scan = TableScan::new(Arc::new(employees_store));
        let employees_result_set = Box::new(ScanResultsSet::new(
            employees_scan,
            Arc::new(employees_table),
            None,
        ));

        let departments_table = Table::new(
            "departments",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        let departments_store = TableStore::new();
        departments_store.insert_all(rows![[1, "Headquarters"], [3, "Remote"]]);

        let departments_scan = TableScan::new(Arc::new(departments_store));
        let departments_result_set = Box::new(ScanResultsSet::new(
            departments_scan,
            Arc::new(departments_table),
            None,
        ));

        let on = Predicate::comparison(
            Literal::ColumnReference("employees.id".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("departments.id".to_string()),
        );

        let join_result_set =
            NestedLoopJoinResultSet::new(employees_result_set, departments_result_set, Some(on));
        let mut iterator = join_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "employees.id" => 1, "departments.id" => 1, "departments.name" => "Headquarters");
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn multi_table_join_with_aliases() {
        // (employees JOIN departments) JOIN locations
        let employees_table = Table::new("employees", schema!["id" => ColumnType::Int].unwrap());
        let employees_store = TableStore::new();
        employees_store.insert(row![1]);

        let employees_result_set = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(employees_store)),
            Arc::new(employees_table),
            Some("emp".to_string()),
        ));

        let departments_table =
            Table::new("departments", schema!["id" => ColumnType::Int].unwrap());
        let departments_store = TableStore::new();
        departments_store.insert(row![1]);

        let departments_result_set = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(departments_store)),
            Arc::new(departments_table),
            Some("dept".to_string()),
        ));

        let inner_on = Predicate::comparison(
            Literal::ColumnReference("emp.id".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("dept.id".to_string()),
        );
        let inner_join = Box::new(NestedLoopJoinResultSet::new(
            employees_result_set,
            departments_result_set,
            Some(inner_on),
        ));

        let locations_table = Table::new("locations", schema!["id" => ColumnType::Int].unwrap());
        let locations_store = TableStore::new();
        locations_store.insert(row![1]);

        let locations_result_set = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(locations_store)),
            Arc::new(locations_table),
            Some("loc".to_string()),
        ));

        let outer_on = Predicate::comparison(
            Literal::ColumnReference("dept.id".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("loc.id".to_string()),
        );

        let join_result_set =
            NestedLoopJoinResultSet::new(inner_join, locations_result_set, Some(outer_on));
        let mut iterator = join_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "emp.id" => 1, "dept.id" => 1, "loc.id" => 1);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn self_join_with_aliases() {
        let employees_table = Arc::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        ));
        let employees_store = Arc::new(TableStore::new());
        employees_store.insert_all(rows![[101, "Alice"], [102, "Bob"]]);

        let employees1_result_set = Box::new(ScanResultsSet::new(
            TableScan::new(employees_store.clone()),
            employees_table.clone(),
            Some("emp1".to_string()),
        ));
        let employees2_result_set = Box::new(ScanResultsSet::new(
            TableScan::new(employees_store),
            employees_table.clone(),
            Some("emp2".to_string()),
        ));

        let on = Predicate::comparison(
            Literal::ColumnReference("emp1.id".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("emp2.id".to_string()),
        );

        let join_result_set =
            NestedLoopJoinResultSet::new(employees1_result_set, employees2_result_set, Some(on));
        let mut iterator = join_result_set.iterator().unwrap();

        assert_next_row!(iterator.as_mut(), "emp1.id" => 101, "emp2.id" => 101);
        assert_next_row!(iterator.as_mut(), "emp1.id" => 102, "emp2.id" => 102);
        assert_no_more_rows!(iterator.as_mut());
    }

    #[test]
    fn join_result_set_with_error_in_left_iterator() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let table = Arc::new(Table::new("right", schema));
        let schema = table.schema();

        let left = Box::new(ErrorResultSet { schema });
        let right = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(TableStore::new())),
            table,
            None,
        ));

        let join = NestedLoopJoinResultSet::new(left, right, None);
        let mut iterator = join.iterator().unwrap();

        assert!(matches!(
            iterator.next(),
            Some(Err(ExecutionError::TypeMismatchInComparison))
        ));
    }

    #[test]
    fn join_result_set_with_error_in_right_iterator_initialization() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let table = Arc::new(Table::new("left", schema));
        let schema = table.schema();

        let left_store = TableStore::new();
        left_store.insert(row![1]);
        let left = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(left_store)),
            table,
            None,
        ));
        let right = Box::new(InitErrorResultSet { schema });

        let join = NestedLoopJoinResultSet::new(left, right, None);
        let mut iterator = join.iterator().unwrap();

        assert!(matches!(
            iterator.next(),
            Some(Err(ExecutionError::TypeMismatchInComparison))
        ));
    }

    #[test]
    fn join_result_set_with_error_in_predicate() {
        let employees_table = Arc::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        let employees_store = Arc::new(TableStore::new());
        employees_store.insert(row![1]);

        let left = Box::new(ScanResultsSet::new(
            TableScan::new(employees_store.clone()),
            employees_table.clone(),
            None,
        ));

        let departments_table = Arc::new(Table::new(
            "departments",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        let departments_store = Arc::new(TableStore::new());
        departments_store.insert(row![1]);

        let right = Box::new(ScanResultsSet::new(
            TableScan::new(departments_store),
            departments_table.clone(),
            None,
        ));

        // Predicate that will error out on comparison
        let on = Predicate::comparison(
            Literal::ColumnReference("employees.id".to_string()),
            LogicalOperator::Eq,
            Literal::Text("error".to_string()),
        );

        let join = NestedLoopJoinResultSet::new(left, right, Some(on));
        let mut iterator = join.iterator().unwrap();

        assert!(matches!(
            iterator.next(),
            Some(Err(ExecutionError::TypeMismatchInComparison))
        ));
    }

    struct ErrorResultSet {
        schema: Arc<Schema>,
    }

    impl ResultSet for ErrorResultSet {
        fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
            Ok(Box::new(std::iter::once(Err(
                ExecutionError::TypeMismatchInComparison,
            ))))
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    struct InitErrorResultSet {
        schema: Arc<Schema>,
    }

    impl ResultSet for InitErrorResultSet {
        fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
            Err(ExecutionError::TypeMismatchInComparison)
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }
}
