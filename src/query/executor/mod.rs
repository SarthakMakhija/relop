pub mod error;
pub mod result;

use crate::catalog::Catalog;
use crate::query::executor::error::ExecutionError;
use crate::query::executor::result::QueryResult;
use crate::query::plan::LogicalPlan;
use crate::storage::result_set::ResultSet;

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
            LogicalPlan::ScanTable { table_name } => {
                let scan_tuple = self
                    .catalog
                    .scan(&table_name)
                    .map_err(ExecutionError::Catalog)?;

                Ok(QueryResult::ResultSet(ResultSet::new(
                    scan_tuple.0,
                    scan_tuple.1,
                )))
            }
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
        let row_views: Vec<_> = result_set.iter().collect();
        assert_eq!(1, row_views.len());

        let row_view = result_set.iter().next().unwrap();
        let column_value = row_view.column("id").unwrap();
        assert_eq!(100, column_value.int_value().unwrap());
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
}
