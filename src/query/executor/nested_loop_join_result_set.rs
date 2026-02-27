use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::query::plan::predicate::Predicate;
use crate::schema::Schema;
use crate::storage::row_view::RowView;
use std::sync::Arc;

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
    use crate::catalog::table::Table;
    use crate::catalog::table_scan::TableScan;
    use crate::query::executor::scan_result_set::ScanResultsSet;
    use std::sync::Arc;

    use super::*;
    use crate::query::executor::test_utils::{
        ErrorResultSet, InitErrorResultSet, JoinResetErrorResultSet,
    };
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::LogicalOperator;
    use crate::storage::table_store::TableStore;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, row, rows, schema};

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
    fn join_result_set_with_error_in_right_iterator_next() {
        let left_schema = schema!["id" => ColumnType::Int].unwrap();
        let left_store = TableStore::new();
        left_store.insert(row![1]);
        let left = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(left_store)),
            Arc::new(Table::new("left", left_schema)),
            None,
        ));

        let right_schema = Arc::new(schema!["id" => ColumnType::Int].unwrap());
        let right = Box::new(ErrorResultSet {
            schema: right_schema,
        });

        let join = NestedLoopJoinResultSet::new(left, right, None);
        let mut iterator = join.iterator().unwrap();

        // Right iterator.next() returns Err
        let result = iterator.next().unwrap();
        assert!(matches!(
            result,
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }

    #[test]
    fn join_result_set_with_error_in_right_iterator_reset() {
        let left_schema = schema!["id" => ColumnType::Int].unwrap();
        let left_store = TableStore::new();
        left_store.insert_all(rows![[1], [2]]);
        let left = Box::new(ScanResultsSet::new(
            TableScan::new(Arc::new(left_store)),
            Arc::new(Table::new("left", left_schema)),
            None,
        ));

        let right_schema = Arc::new(schema!["id" => ColumnType::Int].unwrap());
        let right_positions = (0..right_schema.column_count()).collect();
        let right = Box::new(JoinResetErrorResultSet {
            schema: right_schema,
            visible_positions: Arc::new(right_positions),
            call_count: std::sync::atomic::AtomicUsize::new(0),
        });

        // Cross join (no predicate)
        let join = NestedLoopJoinResultSet::new(left, right, None);
        let mut iterator = join.iterator().unwrap();

        let first = iterator.next().unwrap();
        assert!(first.is_ok());

        let second = iterator.next().unwrap();
        assert!(matches!(
            second,
            Err(ExecutionError::TypeMismatchInComparison)
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
}
