use crate::catalog::table::Table;
use crate::catalog::table_scan::ScanTable;
use crate::query::executor::error::ExecutionError;
use crate::schema::Schema;
use crate::storage::row_view::RowView;
use std::sync::Arc;

/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` wraps a `TableIterator` and the associated `Table` metadata, allowing
/// iteration over rows and safe retrieval of column values by name.
/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` acts as a factory for iterators. It owns the underlying data source (like `ScanTable`),
/// enabling multiple iterations or consistent views.
pub trait ResultSet {
    // Return a boxed iterator that yields Result<RowView, ...>
    // The iterator is bound by the lifetime of &self
    fn iterator(&self) -> Box<dyn Iterator<Item = Result<RowView, ExecutionError>> + '_>;
    fn schema(&self) -> &Schema;
}

pub struct ScanResultsSet {
    scan_table: ScanTable,
    table: Arc<Table>,
    visible_positions: Arc<Vec<usize>>,
}

impl ScanResultsSet {
    pub(crate) fn new(scan_table: ScanTable, table: Arc<Table>) -> Self {
        let column_positions = (0..table.schema_ref().column_count()).collect();
        Self {
            scan_table,
            table,
            visible_positions: Arc::new(column_positions),
        }
    }
}

impl ResultSet for ScanResultsSet {
    fn iterator(&self) -> Box<dyn Iterator<Item = Result<RowView, ExecutionError>> + '_> {
        let table = self.table.clone();
        let visible_positions = self.visible_positions.clone();

        // We call .iter() on ScanTable, which returns a TableIterator (the iterator)
        // We map that iterator to RowView
        Box::new(
            self.scan_table
                .iter()
                .map(move |row| Ok(RowView::new(row, table.schema(), visible_positions.clone()))),
        )
    }

    fn schema(&self) -> &Schema {
        self.table.schema_ref()
    }
}

pub struct ProjectResultSet {
    inner: Box<dyn ResultSet>,
    visible_positions: Arc<Vec<usize>>,
}

impl ProjectResultSet {
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
            visible_positions: Arc::new(positions),
        })
    }
}

impl ResultSet for ProjectResultSet {
    fn iterator(&self) -> Box<dyn Iterator<Item = Result<RowView, ExecutionError>> + '_> {
        let inner_iter = self.inner.iterator();
        let visible_positions = self.visible_positions.clone();

        Box::new(
            inner_iter
                .map(move |result| result.map(|row_view| row_view.project(&visible_positions))),
        )
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

        let scan_table = ScanTable::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(scan_table, Arc::new(table));

        let mut iterator = result_set.iterator();

        let row_view = iterator.next().unwrap().unwrap();
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

        let scan_table = ScanTable::new(Arc::new(table_store));
        let result_set = ScanResultsSet::new(scan_table, Arc::new(table));

        let mut iterator = result_set.iterator();

        let row_view = iterator.next().unwrap().unwrap();
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

        let scan_table = ScanTable::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(scan_table, Arc::new(table)));

        let projected_result_set = ProjectResultSet::new(result_set, &["name"]).unwrap();

        let mut iterator = projected_result_set.iterator();

        let row_view = iterator.next().unwrap().unwrap();
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

        let scan_table = ScanTable::new(Arc::new(table_store));
        let result_set = Box::new(ScanResultsSet::new(scan_table, Arc::new(table)));

        let result = ProjectResultSet::new(result_set, &["name"]);
        assert!(
            matches!(result, Err(ExecutionError::UnknownColumn(column_name)) if column_name == "name"),
        );
    }
}
