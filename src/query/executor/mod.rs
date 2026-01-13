pub mod error;
pub mod result;

use crate::catalog::Catalog;
use crate::query::executor::error::ExecutionError;
use crate::query::executor::result::QueryResult;
use crate::query::plan::LogicalPlan;
use crate::storage::result_set::LimitResultSet;

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
                let result_set = self.execute_select(&logical_plan)?;
                Ok(QueryResult::ResultSet(result_set))
            }
        }
    }

    /// Executes the logical plan for select queries and returns the result.
    fn execute_select(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Box<dyn crate::storage::result_set::ResultSet>, ExecutionError> {
        match logical_plan {
            LogicalPlan::ScanTable { table_name } => {
                let (table_entry, table) = self
                    .catalog
                    .scan(table_name)
                    .map_err(ExecutionError::Catalog)?;

                let table_scan = table_entry.scan();
                Ok(Box::new(crate::storage::result_set::ScanResultsSet::new(
                    table_scan, table,
                )))
            }
            LogicalPlan::Projection {
                base_plan: base,
                columns,
            } => {
                let result_set = self.execute_select(base)?;
                let project_result_set =
                    crate::storage::result_set::ProjectResultSet::new(result_set, &columns[..])?;
                Ok(Box::new(project_result_set))
            }
            LogicalPlan::Limit {
                base_plan: base,
                count,
            } => {
                let result_set = self.execute_select(base)?;
                Ok(Box::new(LimitResultSet::new(result_set, *count)))
            }
            _ => panic!("should not be here"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::CatalogError;
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
        let query_result = executor.execute(LogicalPlan::ShowTables).unwrap();

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
            .execute(LogicalPlan::DescribeTable {
                table_name: "employees".to_string(),
            })
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
            .execute(LogicalPlan::DescribeTable {
                table_name: "employees".to_string(),
            })
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
        let query_result = executor.execute(LogicalPlan::DescribeTable {
            table_name: "employees".to_string(),
        });

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
            .insert_into("employees", Row::single(ColumnValue::Int(100)))
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::ScanTable {
                table_name: "employees".to_string(),
            })
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iter = result_set.iterator();

        let row_view = row_iter.next().unwrap().unwrap();
        let column_value = row_view.column("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());
        assert!(row_iter.next().is_none());
    }

    #[test]
    fn attempt_to_execute_select_star_for_non_existent_table() {
        let catalog = Catalog::new();

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::ScanTable {
            table_name: "employees".to_string(),
        });

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
                Row::filled(vec![
                    ColumnValue::Int(100),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::Projection {
                base_plan: LogicalPlan::ScanTable {
                    table_name: "employees".to_string(),
                }
                .boxed(),
                columns: vec!["id".to_string()],
            })
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iter = result_set.iterator();

        let row_view = row_iter.next().unwrap().unwrap();
        let column_value = row_view.column("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());

        assert!(row_view.column("name").is_none());
        assert!(row_iter.next().is_none());
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
                Row::filled(vec![
                    ColumnValue::Int(100),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor.execute(LogicalPlan::Projection {
            base_plan: LogicalPlan::ScanTable {
                table_name: "employees".to_string(),
            }
            .boxed(),
            columns: vec!["unknown".to_string()],
        });

        assert!(matches!(
            query_result,
            Err(ExecutionError::UnknownColumn(column_name)) if column_name == "unknown"
        ))
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
            .insert_into("employees", Row::single(ColumnValue::Int(100)))
            .unwrap();
        let _ = catalog
            .insert_into("employees", Row::single(ColumnValue::Int(200)))
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::Limit {
                base_plan: LogicalPlan::ScanTable {
                    table_name: "employees".to_string(),
                }
                .boxed(),
                count: 1,
            })
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iter = result_set.iterator();

        let row_view = row_iter.next().unwrap().unwrap();
        let column_value = row_view.column("id").unwrap();

        assert_eq!(100, column_value.int_value().unwrap());
        assert!(row_iter.next().is_none());
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
            .insert_into(
                "employees",
                Row::filled(vec![
                    ColumnValue::Int(100),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();
        let _ = catalog
            .insert_into(
                "employees",
                Row::filled(vec![
                    ColumnValue::Int(200),
                    ColumnValue::Text("query".to_string()),
                ]),
            )
            .unwrap();

        let executor = Executor::new(&catalog);
        let query_result = executor
            .execute(LogicalPlan::Limit {
                base_plan: LogicalPlan::ScanTable {
                    table_name: "employees".to_string(),
                }
                .boxed(),
                count: 1,
            })
            .unwrap();

        assert!(query_result.result_set().is_some());

        let result_set = query_result.result_set().unwrap();
        let mut row_iter = result_set.iterator();

        let row_view = row_iter.next().unwrap().unwrap();
        let column_value = row_view.column("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());

        let column_value = row_view.column("name").unwrap();
        assert_eq!("relop", column_value.text_value().unwrap());

        assert!(row_iter.next().is_none());
    }
}
