use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::schema::Schema;

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

#[cfg(test)]
mod tests {
    use crate::catalog::table::Table;
    use crate::catalog::table_scan::TableScan;
    use crate::query::executor::scan_result_set::ScanResultsSet;
    use std::sync::Arc;

    use super::*;
    use crate::query::executor::filter_result_set::FilterResultSet;
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::{LogicalOperator, Predicate};
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, row, schema};

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
}
