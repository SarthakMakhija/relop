use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::query::plan::predicate::Predicate;
use crate::schema::Schema;

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

#[cfg(test)]
mod tests {
    use crate::catalog::table::Table;
    use crate::catalog::table_scan::TableScan;
    use crate::query::executor::scan_result_set::ScanResultsSet;
    use std::sync::Arc;

    use super::*;
    use crate::query::executor::test_utils::ErrorResultSet;
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::LogicalOperator;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, row, rows, schema};

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
    fn filter_result_set_with_predicate_error() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let table = Table::new("employees", schema);
        let table_store = TableStore::new();
        table_store.insert_all(rows![[1], [2]]);

        let table_scan = TableScan::new(Arc::new(table_store));
        let scan_result_set = Box::new(ScanResultsSet::new(table_scan, Arc::new(table), None));

        // Predicate referring to a non-existent column "age"
        let predicate = Predicate::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Eq,
            Literal::Int(30),
        );

        let filter_result_set = FilterResultSet::new(scan_result_set, predicate);
        let mut row_iterator = filter_result_set.iterator().unwrap();

        let result = row_iterator.next().unwrap();
        assert!(matches!(result, Err(ExecutionError::UnknownColumn(name)) if name == "age"));
    }
}
