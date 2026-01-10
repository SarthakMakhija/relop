use crate::catalog::table_descriptor::TableDescriptor;

/// Represents the result of a query execution.
pub enum QueryResult {
    /// Result of a `SHOW TABLES` query, containing a list of table names.
    TableList(Vec<String>),
    /// Result of a `DESCRIBE TABLE` query, containing the table's schema information.
    TableDescription(TableDescriptor),
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
}
