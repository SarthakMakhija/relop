use crate::catalog::table_descriptor::TableDescriptor;
use crate::storage::result_set::ResultSet;

/// Represents the result of a query execution.
pub enum QueryResult {
    /// Result of a `SHOW TABLES` query, containing a list of table names.
    TableList(Vec<String>),
    /// Result of a `DESCRIBE TABLE` query, containing the table's schema information.
    TableDescription(TableDescriptor),
    /// Result of a `SELECT *` query without where clause.
    ResultSet(ResultSet),
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
    pub fn result_set(&self) -> Option<&ResultSet> {
        match self {
            QueryResult::ResultSet(result_set) => Some(result_set),
            _ => None,
        }
    }
}
