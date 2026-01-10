pub(crate) mod result;

use crate::catalog::Catalog;
use crate::query::executor::result::QueryResult;
use crate::query::plan::LogicalPlan;

pub struct Executor<'a> {
    catalog: &'a Catalog,
}

impl<'a> Executor<'a> {
    fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    fn execute(&self, logical_plan: LogicalPlan) -> Result<QueryResult, ()> {
        match logical_plan {
            LogicalPlan::ShowTables => Ok(QueryResult::AllTables(self.catalog.show_tables())),
            LogicalPlan::DescribeTable { .. } => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

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
}
