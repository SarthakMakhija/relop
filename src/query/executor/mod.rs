pub mod error;
pub mod result;
pub mod result_set;

use crate::catalog::Catalog;
use crate::query::executor::error::ExecutionError;
use crate::query::executor::result::QueryResult;
use crate::query::plan::{predicate, LogicalPlan};
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
                let table_descriptor = self
                    .catalog
                    .describe_table(&table_name)
                    .map_err(ExecutionError::Catalog)?;

                Ok(QueryResult::TableDescription(table_descriptor))
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
            LogicalPlan::Scan { table_name } => {
                let (table_entry, table) = self
                    .catalog
                    .scan(table_name.as_ref())
                    .map_err(ExecutionError::Catalog)?;

                let table_scan = table_entry.scan();
                Ok(Box::new(result_set::ScanResultsSet::new(table_scan, table)))
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
    use crate::schema::primary_key::PrimaryKey;
    use crate::schema::Schema;
    use crate::storage::row::Row;
    use crate::types::column_type::ColumnType;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn execute_show_tables() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
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
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::describe_table("employees"))
            .unwrap();

        assert!(query_result.table_descriptor().is_some());
        let table_descriptor = query_result.table_descriptor().unwrap();

        assert_eq!("employees", table_descriptor.table_name());
        assert_eq!(vec!["id"], table_descriptor.column_names());
        assert!(table_descriptor.primary_key_column_names().is_none())
    }

    #[test]
    fn execute_describe_table_with_primary_key() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        );
        assert!(result.is_ok());

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::describe_table("employees"))
            .unwrap();

        assert!(query_result.table_descriptor().is_some());
        let table_descriptor = query_result.table_descriptor().unwrap();

        assert_eq!("employees", table_descriptor.table_name());
        assert_eq!(vec!["id"], table_descriptor.column_names());
        assert_eq!(
            vec!["id"],
            table_descriptor.primary_key_column_names().unwrap()
        );
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
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_into("employees", Row::single(ColumnValue::int(100)))
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::scan("employees")).unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());
        assert!(row_iterator.next().is_none());
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
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_column("name", ColumnType::Text)
                .unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_into(
                "employees",
                Row::filled(vec![ColumnValue::int(100), ColumnValue::text("relop")]),
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").project(vec!["id"]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());

        assert!(row_view.column_value_by("name").is_none());
        assert!(row_iterator.next().is_none());
    }

    #[test]
    fn attempt_to_execute_select_with_projection_for_non_existent_column() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_column("name", ColumnType::Text)
                .unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_into(
                "employees",
                Row::filled(vec![ColumnValue::int(100), ColumnValue::text("relop")]),
            )
            .unwrap();

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
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::single(ColumnValue::int(1)),
                    Row::single(ColumnValue::int(2)),
                ],
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(
                LogicalPlan::scan("employees").filter(Predicate::Comparison {
                    column_name: "id".to_string(),
                    operator: LogicalOperator::Eq,
                    literal: Literal::Int(1),
                }),
            )
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();

        assert_eq!(1, column_value.int_value().unwrap());
        assert!(row_iterator.next().is_none());
    }

    #[test]
    fn execute_select_with_order_by_single_column_ascending() {
        use crate::query::parser::ordering_key::OrderingKey;

        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::single(ColumnValue::int(200)),
                    Row::single(ColumnValue::int(100)),
                ],
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").order_by(vec![OrderingKey::ascending_by("id")]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();

        assert_eq!(200, column_value.int_value().unwrap());
        assert!(row_iterator.next().is_none());
    }

    #[test]
    fn execute_select_with_order_by_single_column_descending() {
        use crate::query::parser::ordering_key::OrderingKey;

        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::single(ColumnValue::int(100)),
                    Row::single(ColumnValue::int(200)),
                ],
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(
                LogicalPlan::scan("employees").order_by(vec![OrderingKey::descending_by("id")]),
            )
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();
        assert_eq!(200, column_value.int_value().unwrap());

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();

        assert_eq!(100, column_value.int_value().unwrap());
        assert!(row_iterator.next().is_none());
    }

    #[test]
    fn execute_select_with_order_by_multiple_columns() {
        use crate::query::parser::ordering_key::OrderingKey;

        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_column("age", ColumnType::Int)
                .unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::filled(vec![ColumnValue::int(1), ColumnValue::int(30)]),
                    Row::filled(vec![ColumnValue::int(1), ColumnValue::int(20)]),
                ],
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").order_by(vec![
                OrderingKey::ascending_by("id"),
                OrderingKey::ascending_by("age"),
            ]))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        assert_eq!(
            1,
            row_view.column_value_by("id").unwrap().int_value().unwrap()
        );
        assert_eq!(
            20,
            row_view
                .column_value_by("age")
                .unwrap()
                .int_value()
                .unwrap()
        );

        let row_view = row_iterator.next().unwrap().unwrap();
        assert_eq!(
            1,
            row_view.column_value_by("id").unwrap().int_value().unwrap()
        );
        assert_eq!(
            30,
            row_view
                .column_value_by("age")
                .unwrap()
                .int_value()
                .unwrap()
        );
        assert!(row_iterator.next().is_none());
    }

    #[test]
    fn execute_select_star_with_limit() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::single(ColumnValue::int(100)),
                    Row::single(ColumnValue::int(200)),
                ],
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").limit(1))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();

        assert_eq!(100, column_value.int_value().unwrap());
        assert!(row_iterator.next().is_none());
    }

    #[test]
    fn execute_select_with_projection_with_limit() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_column("name", ColumnType::Text)
                .unwrap(),
        );
        assert!(result.is_ok());

        let _ = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::filled(vec![ColumnValue::int(100), ColumnValue::text("relop")]),
                    Row::filled(vec![ColumnValue::int(200), ColumnValue::text("query")]),
                ],
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::scan("employees").limit(1))
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        let row_view = row_iterator.next().unwrap().unwrap();
        let column_value = row_view.column_value_by("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());

        let column_value = row_view.column_value_by("name").unwrap();

        assert_eq!("relop", column_value.text_value().unwrap());
        assert!(row_iterator.next().is_none());
    }
}
