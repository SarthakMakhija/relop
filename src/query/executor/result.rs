use crate::catalog::table_descriptor::TableDescriptor;
use crate::query::executor::result_set::ResultSet;

/// Represents the result of a query execution.
pub enum QueryResult {
    /// Result of a `SHOW TABLES` query, containing a list of table names.
    TableList(Vec<String>),
    /// Result of a `DESCRIBE TABLE` query, containing the table's schema information.
    TableDescription(TableDescriptor),
    /// Result of a `SELECT *` query without where clause.
    ResultSet(Box<dyn ResultSet>),
}

impl QueryResult {
    /// Returns the list of tables if the result is a `TableList`.
    ///
    /// # Returns
    ///
    /// * `Some(&Vec<String>)` - If the result is a `TableList`.
    /// * `None` - Otherwise.
    pub fn all_tables(&self) -> Option<&Vec<String>> {
        match self {
            QueryResult::TableList(tables) => Some(tables),
            _ => None,
        }
    }

    /// Returns the table descriptor if the result is a `TableDescription`.
    ///
    /// # Returns
    ///
    /// * `Some(&TableDescriptor)` - If the result is a `TableDescription`.
    /// * `None` - Otherwise.
    pub fn table_descriptor(&self) -> Option<&TableDescriptor> {
        match self {
            QueryResult::TableDescription(table_descriptor) => Some(table_descriptor),
            _ => None,
        }
    }

    /// Returns the table scan if the result is a `ResultSet`.
    ///
    /// # Returns
    ///
    /// * `Some(&ResultSet)` - If the result is a `ResultSet`.
    /// * `None` - Otherwise.
    pub fn result_set(&self) -> Option<&dyn ResultSet> {
        match self {
            QueryResult::ResultSet(result_set) => Some(result_set.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table_descriptor::TableDescriptor;
    use crate::query::executor::result_set::ResultSet;
    use crate::schema::error::SchemaError;
    use crate::schema::Schema;

    use crate::types::column_type::ColumnType;

    struct MockResultSet;

    impl ResultSet for MockResultSet {
        fn iterator(
            &self,
        ) -> Result<
            Box<dyn Iterator<Item = crate::storage::row_view::RowView> + '_>,
            crate::query::executor::error::ExecutionError,
        > {
            unimplemented!()
        }

        fn schema(&self) -> &Schema {
            unimplemented!()
        }
    }

    #[test]
    fn query_result_table_list() {
        let tables = vec!["table1".to_string(), "table2".to_string()];
        let result = QueryResult::TableList(tables.clone());

        assert_eq!(result.all_tables(), Some(&tables));
        assert!(result.table_descriptor().is_none());
        assert!(result.result_set().is_none());
    }

    #[test]
    fn query_result_table_description() -> Result<(), SchemaError> {
        use crate::catalog::table::Table;
        use std::sync::Arc;

        let schema = Schema::new()
            .add_column("id", ColumnType::Int)?
            .add_column("name", ColumnType::Text)?;

        let table = Table::new("table1", schema);
        let descriptor = TableDescriptor::new(Arc::new(table));

        let result = QueryResult::TableDescription(descriptor);

        let retrieved_descriptor = result.table_descriptor().unwrap();
        assert_eq!(retrieved_descriptor.table_name(), "table1");

        assert!(result.all_tables().is_none());
        assert!(result.result_set().is_none());
        Ok(())
    }

    #[test]
    fn query_result_set() {
        let result_set = Box::new(MockResultSet);
        let result = QueryResult::ResultSet(result_set);

        assert!(result.result_set().is_some());
        assert!(result.all_tables().is_none());
        assert!(result.table_descriptor().is_none());
    }
}
