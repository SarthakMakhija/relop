pub mod error;
pub mod result;
pub mod result_set;

use crate::catalog::Catalog;
use crate::query::executor::error::ExecutionError;
use crate::query::executor::result::QueryResult;
use crate::query::plan::LogicalPlan;
use result_set::LimitResultSet;

/// Executes logical plans against the catalog.
pub(crate) struct Executor<'a> {
    catalog: &'a Catalog,
}

impl<'a> Executor<'a> {
    /// Creates a new `Executor` with the given catalog.
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Executes the given logical plan and returns the result.
    ///
    /// Returns an `ExecutionError` if the plan cannot be executed.
    pub(crate) fn execute(&self, logical_plan: LogicalPlan) -> Result<QueryResult, ExecutionError> {
        match logical_plan {
            LogicalPlan::ShowTables => Ok(QueryResult::TableList(self.catalog.show_tables())),
            LogicalPlan::DescribeTable { table_name } => {
                let table = self
                    .catalog
                    .describe_table(&table_name)
                    .map_err(ExecutionError::Catalog)?;

                Ok(QueryResult::TableDescription(table))
            }
            _ => {
                let result_set = self.execute_select(logical_plan)?;
                Ok(QueryResult::ResultSet(result_set))
            }
        }
    }

    /// Executes the logical plan for select queries and returns the result.
    fn execute_select(
        &self,
        logical_plan: LogicalPlan,
    ) -> Result<Box<dyn result_set::ResultSet>, ExecutionError> {
        match logical_plan {
            LogicalPlan::Scan { table_name, alias } => {
                let (table_entry, table) = self
                    .catalog
                    .scan(table_name.as_ref())
                    .map_err(ExecutionError::Catalog)?;

                let table_scan = table_entry.scan();
                Ok(Box::new(result_set::ScanResultsSet::new(
                    table_scan, table, alias,
                )))
            }
            LogicalPlan::Join {
                left, right, on, ..
            } => {
                let left_result_set = self.execute_select(*left)?;
                let right_result_set = self.execute_select(*right)?;
                Ok(Box::new(result_set::NestedLoopJoinResultSet::new(
                    left_result_set,
                    right_result_set,
                    on,
                )))
            }
            LogicalPlan::Filter {
                base_plan: base,
                predicate,
            } => {
                let result_set = self.execute_select(*base)?;
                Ok(Box::new(result_set::FilterResultSet::new(
                    result_set, predicate,
                )))
            }
            LogicalPlan::Projection {
                base_plan: base,
                columns,
            } => {
                let result_set = self.execute_select(*base)?;
                let project_result_set =
                    result_set::ProjectResultSet::new(result_set, &columns[..])?;
                Ok(Box::new(project_result_set))
            }
            LogicalPlan::Sort {
                base_plan: base,
                ordering_keys,
            } => {
                let result_set = self.execute_select(*base)?;
                let ordering_result_set =
                    result_set::OrderingResultSet::new(result_set, ordering_keys);
                Ok(Box::new(ordering_result_set))
            }
            LogicalPlan::Limit {
                base_plan: base,
                count,
            } => {
                let result_set = self.execute_select(*base)?;
                Ok(Box::new(LimitResultSet::new(result_set, count)))
            }
            _ => panic!("should not be here"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::CatalogError;
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::{LogicalOperator, Predicate};
    use crate::test_utils::{create_schema_with_primary_key, insert_row, insert_rows};
    use crate::types::column_type::ColumnType;
    use crate::{asc, assert_next_row, assert_no_more_rows, desc, row, rows, schema};

    #[test]
    fn execute_show_tables() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::show_tables()).unwrap();

        assert!(query_result.all_tables().is_some());
        let table_names = query_result.all_tables().unwrap();

        assert_eq!(1, table_names.len());
        assert_eq!(&vec!["employees"], table_names);
    }

    #[test]
    fn execute_describe_table() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::describe_table("employees"))
            .unwrap();

        assert!(query_result.table_descriptor().is_some());
        let table = query_result.table_descriptor().unwrap();

        assert_eq!("employees", table.name());
        assert_eq!(vec!["id"], table.column_names());
        assert!(table.primary_key_column_names().is_none())
    }

    #[test]
    fn execute_describe_table_with_primary_key() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        );
        assert!(result.is_ok());

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::describe_table("employees"))
            .unwrap();

        assert!(query_result.table_descriptor().is_some());
        let table = query_result.table_descriptor().unwrap();

        assert_eq!("employees", table.name());
        assert_eq!(vec!["id"], table.column_names());
        assert_eq!(vec!["id"], table.primary_key_column_names().unwrap());
    }

    #[test]
    fn attempt_to_execute_describe_table_for_non_existent_table() {
        let catalog = Catalog::new();

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::describe_table("employees"));

        assert!(matches!(
            query_result,
            Err(ExecutionError::Catalog(CatalogError::TableDoesNotExist(table_name))) if table_name == "employees"
        ))
    }

    #[test]
    fn execute_select_star() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_row(&catalog, "employees", row![100]);

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::scan("employees")).unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 100);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn attempt_to_execute_select_star_for_non_existent_table() {
        let catalog = Catalog::new();

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::scan("employees"));

        assert!(matches!(
            query_result,
            Err(ExecutionError::Catalog(CatalogError::TableDoesNotExist(table_name))) if table_name == "employees"
        ));
    }

    #[test]
    fn execute_select_with_projection() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_row(&catalog, "employees", row![100, "relop"]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").project(vec!["id"]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 100, ! "name");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn attempt_to_execute_select_with_projection_for_non_existent_column() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_row(&catalog, "employees", row![100, "relop"]);

        let executor = Executor::new(&catalog);
        let query_result =
            executor.execute(LogicalPlan::scan("employees").project(vec!["unknown"]));

        assert!(matches!(
            query_result,
            Err(ExecutionError::UnknownColumn(column_name)) if column_name == "unknown"
        ))
    }

    #[test]
    fn execute_select_with_where_clause() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[1], [2]]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").filter(Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            )))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_where_and_clause() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "age" => ColumnType::Int].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[1, 30], [2, 40], [1, 25]]);

        let executor = Executor::new(&catalog);
        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(25),
            ),
        ]);

        let query_result = executor
            .execute(LogicalPlan::scan("employees").filter(predicate))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "age" => 30);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_where_and_clause_with_one_of_the_and_does_not_match() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "age" => ColumnType::Int].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[1, 20]]);

        let executor = Executor::new(&catalog);
        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(25),
            ),
        ]);

        let query_result = executor
            .execute(LogicalPlan::scan("employees").filter(predicate))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_order_by_single_column_ascending() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[200], [100]]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").order_by(vec![asc!("id")]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 100);
        assert_next_row!(row_iterator.as_mut(), "id" => 200);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_order_by_single_column_descending() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[100], [200]]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").order_by(vec![desc!("id")]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 200);
        assert_next_row!(row_iterator.as_mut(), "id" => 100);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_order_by_multiple_columns() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "age" => ColumnType::Int].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[1, 30], [1, 20]]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").order_by(vec![asc!("id"), asc!("age")]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "age" => 20);
        assert_next_row!(row_iterator.as_mut(), "id" => 1, "age" => 30);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_limit() {
        let catalog = Catalog::new();
        let result = catalog.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[100], [200]]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").limit(1))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 100);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_projection_with_limit() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&catalog, "employees", rows![[100, "relop"], [200, "query"]]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").limit(1))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 100, "name" => "relop");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_alias() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_row(&catalog, "employees", row![100, "relop"]);

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::Scan {
                table_name: "employees".to_string(),
                alias: Some("e".to_string()),
            })
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "e.id" => 100, "e.name" => "relop");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_of_two_tables() {
        let catalog = Catalog::new();
        catalog
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        catalog
            .create_table("departments", schema!["name" => ColumnType::Text].unwrap())
            .unwrap();

        insert_rows(&catalog, "employees", rows![[1], [2]]);
        insert_rows(
            &catalog,
            "departments",
            rows![["Engineering"], ["Marketing"]],
        );

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::Join {
                left: LogicalPlan::scan("employees").boxed(),
                left_name: Some("employees".to_string()),
                right: LogicalPlan::scan("departments").boxed(),
                right_name: Some("departments".to_string()),
                on: None,
            })
            .unwrap();

        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1, "departments.name" => "Engineering");
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1, "departments.name" => "Marketing");
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2, "departments.name" => "Engineering");
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2, "departments.name" => "Marketing");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_of_three_tables_with_aliases() {
        let catalog = Catalog::new();
        catalog
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        catalog
            .create_table("departments", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        catalog
            .create_table("locations", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        insert_rows(&catalog, "employees", rows![[1]]);
        insert_rows(&catalog, "departments", rows![[1]]);
        insert_rows(&catalog, "locations", rows![[1]]);

        let executor = Executor::new(&catalog);
        let inner_join = LogicalPlan::Join {
            left: LogicalPlan::Scan {
                table_name: "employees".to_string(),
                alias: Some("e".to_string()),
            }
            .boxed(),
            left_name: Some("e".to_string()),
            right: LogicalPlan::Scan {
                table_name: "departments".to_string(),
                alias: Some("d".to_string()),
            }
            .boxed(),
            right_name: Some("d".to_string()),
            on: Some(Predicate::comparison(
                Literal::ColumnReference("e.id".to_string()),
                LogicalOperator::Eq,
                Literal::ColumnReference("d.id".to_string()),
            )),
        };

        let outer_join = LogicalPlan::Join {
            left: inner_join.boxed(),
            left_name: None,
            right: LogicalPlan::Scan {
                table_name: "locations".to_string(),
                alias: Some("l".to_string()),
            }
            .boxed(),
            right_name: Some("l".to_string()),
            on: Some(Predicate::comparison(
                Literal::ColumnReference("d.id".to_string()),
                LogicalOperator::Eq,
                Literal::ColumnReference("l.id".to_string()),
            )),
        };

        let query_result = executor.execute(outer_join).unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "e.id" => 1, "d.id" => 1, "l.id" => 1);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_self_join_with_aliases() {
        let catalog = Catalog::new();
        catalog
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        insert_rows(&catalog, "employees", rows![[1], [2]]);

        let executor = Executor::new(&catalog);
        let join_plan = LogicalPlan::Join {
            left: LogicalPlan::Scan {
                table_name: "employees".to_string(),
                alias: Some("emp1".to_string()),
            }
            .boxed(),
            left_name: Some("emp1".to_string()),
            right: LogicalPlan::Scan {
                table_name: "employees".to_string(),
                alias: Some("emp2".to_string()),
            }
            .boxed(),
            right_name: Some("emp2".to_string()),
            on: Some(Predicate::comparison(
                Literal::ColumnReference("emp1.id".to_string()),
                LogicalOperator::Eq,
                Literal::ColumnReference("emp2.id".to_string()),
            )),
        };

        let query_result = executor.execute(join_plan).unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "emp1.id" => 1, "emp2.id" => 1);
        assert_next_row!(row_iterator.as_mut(), "emp1.id" => 2, "emp2.id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }
}
