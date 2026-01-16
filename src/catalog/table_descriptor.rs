use crate::catalog::table::Table;
use std::sync::Arc;

/// Describes the structure of a table, including its name, columns, and primary key.
pub struct TableDescriptor {
    table: Arc<Table>,
}

impl TableDescriptor {
    pub(crate) fn new(table: Arc<Table>) -> TableDescriptor {
        Self { table }
    }

    /// Returns the name of the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// relop.create_table("employees", Schema::new().add_column("id", ColumnType::Int).unwrap()).unwrap();
    ///
    /// // Execute `describe table` to get a TableDescriptor wrapped in QueryResult
    /// let result = relop.execute("describe table employees").unwrap();
    /// let descriptor = result.table_descriptor().unwrap();
    ///
    /// assert_eq!("employees", descriptor.table_name());
    /// ```
    pub fn table_name(&self) -> &str {
        self.table.name()
    }

    /// Returns the names of all columns in the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// relop.create_table("employees", Schema::new().add_column("id", ColumnType::Int).unwrap()).unwrap();
    ///
    /// let result = relop.execute("describe table employees").unwrap();
    /// let descriptor = result.table_descriptor().unwrap();
    ///
    /// assert_eq!(vec!["id"], descriptor.column_names());
    /// ```
    pub fn column_names(&self) -> Vec<&str> {
        self.table.schema_ref().column_names()
    }

    /// Returns the names of the primary key columns, if any.
    ///
    /// Returns `None` if the table has no primary key.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::{Schema, primary_key::PrimaryKey};
    /// use relop::types::column_type::ColumnType;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int).unwrap()
    ///     .add_primary_key(PrimaryKey::single("id")).unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    ///
    /// let result = relop.execute("describe table employees").unwrap();
    /// let descriptor = result.table_descriptor().unwrap();
    ///
    /// assert_eq!(vec!["id"], descriptor.primary_key_column_names().unwrap());
    /// ```
    pub fn primary_key_column_names(&self) -> Option<&[String]> {
        self.table.schema_ref().primary_key_column_names()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{create_schema, create_schema_with_primary_key};
    use crate::types::column_type::ColumnType;

    #[test]
    fn table_name() {
        let table = Table::new("employees", create_schema(&[("id", ColumnType::Int)]));
        let table_descriptor = TableDescriptor::new(Arc::new(table));

        assert_eq!("employees", table_descriptor.table_name());
    }

    #[test]
    fn column_names() {
        let table = Table::new("employees", create_schema(&[("id", ColumnType::Int)]));
        let table_descriptor = TableDescriptor::new(Arc::new(table));

        assert_eq!(vec!["id"], table_descriptor.column_names());
    }

    #[test]
    fn primary_key_column_names() {
        let table = Table::new(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        );
        let table_descriptor = TableDescriptor::new(Arc::new(table));

        assert_eq!(
            vec!["id"],
            table_descriptor.primary_key_column_names().unwrap()
        );
    }
}
