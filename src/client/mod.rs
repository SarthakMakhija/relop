//! Client module for the Relop relational operator library.
//!
//! This module provides the main client interface (`Relop`) for interacting with
//! the relational database system. It manages an in-memory catalog of tables and
//! provides methods for table creation, data insertion, and query execution.

pub mod error;
pub use crate::query::executor::result::QueryResult;

use crate::catalog::Catalog;
use crate::client::error::ClientError;
use crate::query::executor::Executor;
use crate::query::lexer::Lexer;
use crate::query::parser::Parser;
use crate::query::plan::LogicalPlanner;
use crate::schema::Schema;
use crate::storage::batch::Batch;
use crate::storage::row::Row;
use crate::storage::table_store::RowId;

/// The main client interface for the relational operator library.
///
/// `Relop` provides a high-level API for interacting with the relational database system.
/// It manages an in-memory catalog of tables and provides methods for:
/// - Creating tables with schemas
/// - Inserting data into tables (single rows or batches)
/// - Executing SQL queries through the full query processing pipeline
pub struct Relop {
    catalog: Catalog,
}

impl Relop {
    /// Creates a new `Relop` instance from a catalog.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The [`Catalog`] instance that will manage tables and their data.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    ///
    /// let catalog = Catalog::new();
    /// let relop = Relop::new(catalog);
    /// ```
    pub fn new(catalog: Catalog) -> Relop {
        Self { catalog }
    }

    /// Creates a new table with the given name and schema.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to create. This can be any type that implements
    ///   `Into<String>` (e.g., `&str`, `String`).
    /// * `schema` - The [`Schema`] defining the table's columns and optional primary key.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the table was created successfully, or a [`ClientError::Catalog`]
    /// if an error occurred.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - A table with the same name already exists (wrapped in [`ClientError::Catalog`])
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
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    /// ```
    ///
    /// Creating a table with a primary key:
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::{Schema, primary_key::PrimaryKey};
    /// use relop::types::column_type::ColumnType;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap()
    ///     .add_primary_key(PrimaryKey::single("id"))
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    /// ```
    pub fn create_table<N: Into<String>>(
        &self,
        table_name: N,
        schema: Schema,
    ) -> Result<(), ClientError> {
        self.catalog
            .create_table(table_name, schema)
            .map_err(ClientError::Catalog)
    }

    /// Inserts a single row into the specified table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to insert into.
    /// * `row` - The [`Row`] containing the column values to insert. The row must match
    ///   the table's schema in terms of column count and types.
    ///
    /// # Returns
    ///
    /// Returns `Ok(RowId)` containing the unique identifier assigned to the inserted row,
    /// or a [`ClientError::Insert`] if an error occurred.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The table doesn't exist (wrapped in [`ClientError::Insert`])
    /// - The row's column count doesn't match the table schema (wrapped in [`ClientError::Insert`])
    /// - The row's column types don't match the table schema (wrapped in [`ClientError::Insert`])
    /// - The row violates a primary key constraint (duplicate primary key, wrapped in [`ClientError::Insert`])
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::storage::row::Row;
    /// use relop::types::column_type::ColumnType;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    ///
    /// let row = Row::filled(vec![ColumnValue::int(1)]);
    /// let row_id = relop.insert_into("employees", row).unwrap();
    /// ```
    pub fn insert_into(&self, table_name: &str, row: Row) -> Result<RowId, ClientError> {
        self.catalog
            .insert_into(table_name, row)
            .map_err(ClientError::Insert)
    }

    /// Inserts multiple rows (batch insert) into the specified table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to insert into.
    /// * `batch` - A collection of rows that can be converted into a [`Batch`]. This accepts
    ///   any type that implements `Into<Batch>`, such as `Vec<Row>` or a `Batch` directly.
    ///   Each row must match the table's schema in terms of column count and types.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Vec<RowId>)` containing the unique identifiers assigned to each inserted row,
    /// in the same order as the input rows, or a [`ClientError::Insert`] if an error occurred.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The table doesn't exist (wrapped in [`ClientError::Insert`])
    /// - Any row's column count doesn't match the table schema (wrapped in [`ClientError::Insert`])
    /// - Any row's column types don't match the table schema (wrapped in [`ClientError::Insert`])
    /// - There are duplicate primary keys within the batch (wrapped in [`ClientError::Insert`])
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::storage::row::Row;
    /// use relop::types::column_type::ColumnType;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    ///
    /// let rows = vec![
    ///     Row::filled(vec![ColumnValue::int(1)]),
    ///     Row::filled(vec![ColumnValue::int(2)]),
    /// ];
    /// let row_ids = relop.insert_all_into("employees", rows).unwrap();
    /// assert_eq!(2, row_ids.len());
    /// ```
    pub fn insert_all_into(
        &self,
        table_name: &str,
        batch: impl Into<Batch>,
    ) -> Result<Vec<RowId>, ClientError> {
        self.catalog
            .insert_all_into(table_name, batch)
            .map_err(ClientError::Insert)
    }

    /// Executes a SQL query string through the full query processing pipeline.
    ///
    /// This method processes a SQL query through multiple stages:
    /// 1. **Lexical Analysis**: The query string is tokenized by the `Lexer`
    /// 2. **Parsing**: Tokens are parsed into an Abstract Syntax Tree (AST) by the `Parser`
    /// 3. **Logical Planning**: The AST is converted into a logical plan by the `LogicalPlanner`
    /// 4. **Execution**: The logical plan is executed by the `Executor`, which returns a [`QueryResult`]
    ///
    /// The processing pipeline follows this flow: `Lexer` → `Parser` → `LogicalPlanner` → `Executor`
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute.
    ///
    /// # Returns
    ///
    /// Returns `Ok(QueryResult)` containing the query results, or a [`ClientError`] if an error
    /// occurred during any stage of processing.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The query contains invalid characters or syntax that cannot be lexed (wrapped in [`ClientError::Lex`])
    /// - The query syntax is invalid or unsupported (wrapped in [`ClientError::Parse`])
    /// - An error occurs during query execution, such as referencing a non-existent table
    ///   (wrapped in [`ClientError::Execution`])
    ///
    /// # Supported Queries
    ///
    /// Currently, supports the following query types:
    /// - `show tables` - Lists all tables in the catalog
    /// - `describe table <name>` - Shows the schema of a specific table
    /// - `select * from table <name>` - Gets the result-set from a specific table
    ///
    /// # Examples
    ///
    /// Listing all tables:
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    ///
    /// let result = relop.execute("show tables").unwrap();
    /// let tables = result.all_tables().unwrap();
    /// assert_eq!(&vec!["employees".to_string()], tables);
    /// ```
    ///
    /// Describing a table:
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    ///
    /// let result = relop.execute("describe table employees").unwrap();
    /// let table = result.table_descriptor().unwrap();
    /// assert_eq!("employees", table.name());
    /// ```
    ///
    /// Selecting from a table
    ///
    /// ```
    /// use relop::catalog::Catalog;
    /// use relop::client::Relop;
    /// use relop::schema::Schema;
    /// use relop::storage::row::Row;
    /// use relop::types::column_type::ColumnType;
    /// use relop::types::column_value::ColumnValue;
    ///
    /// let relop = Relop::new(Catalog::new());
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int)
    ///     .unwrap();
    ///
    /// relop.create_table("employees", schema).unwrap();
    ///
    /// let _ = relop
    ///     .insert_into("employees", Row::filled(vec![ColumnValue::int(1)]))
    ///     .unwrap();
    ///
    ///  let mut query_result = relop.execute("select * from employees").unwrap();
    ///  let result_set = query_result.result_set().unwrap();
    ///  let mut iterator = result_set.iterator().unwrap();
    ///
    ///  let row_view = iterator.next().unwrap().unwrap();
    ///  assert_eq!(&ColumnValue::int(1), row_view.column_value_by("id").unwrap());
    /// ```
    pub fn execute(&self, query: &str) -> Result<QueryResult, ClientError> {
        let mut lexer = Lexer::new_with_default_keywords(query);
        let tokens = lexer.lex().map_err(ClientError::Lex)?;

        let mut parser = Parser::new(tokens);
        let ast = parser.parse().map_err(ClientError::Parse)?;

        let plan = LogicalPlanner::plan(ast).map_err(ClientError::Plan)?;

        let executor = Executor::new(&self.catalog);
        executor.execute(plan).map_err(ClientError::Execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_no_more_rows;
    use crate::catalog::error::{CatalogError, InsertError};
    use crate::query::executor::error::ExecutionError;
    use crate::query::lexer::error::LexError;
    use crate::query::parser::error::ParseError;
    use crate::row;
    use crate::rows;
    use crate::test_utils::{create_schema_with_primary_key, insert_rows};
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, schema};

    #[test]
    fn create_table() {
        let result = Relop::new(Catalog::new())
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn attempt_to_create_an_already_created_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(ClientError::Catalog(CatalogError::TableAlreadyExists(table_name))) if table_name == "employees"
        ))
    }

    #[test]
    fn insert_into_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let row_id = relop.insert_into("employees", row![1]).unwrap();

        let row = relop.catalog.get("employees", row_id).unwrap().unwrap();
        let expected_row = row![1];

        assert_eq!(expected_row, row);
    }

    #[test]
    fn attempt_to_insert_duplicate_primary_key() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        );
        assert!(result.is_ok());

        let row_id = relop.insert_into("employees", row![1]).unwrap();

        let row = relop.catalog.get("employees", row_id).unwrap().unwrap();
        let expected_row = row![1];
        assert_eq!(expected_row, row);

        let result = relop.insert_into("employees", row![1]);

        assert!(matches!(
            result,
            Err(ClientError::Insert(InsertError::DuplicatePrimaryKey))
        ));
    }

    #[test]
    fn insert_all_into_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let row_ids = relop.insert_all_into("employees", rows![[1], [2]]).unwrap();

        let row = relop
            .catalog
            .get("employees", *row_ids.first().unwrap())
            .unwrap()
            .unwrap();

        let expected_row = row![1];
        assert_eq!(expected_row, row);

        let row = relop
            .catalog
            .get("employees", *row_ids.last().unwrap())
            .unwrap()
            .unwrap();

        let expected_row = row![2];
        assert_eq!(expected_row, row);
    }

    #[test]
    fn execute_show_tables() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let query_result = relop.execute("show tables").unwrap();
        assert!(query_result.all_tables().is_some());

        let table_names = query_result.all_tables().unwrap();

        assert_eq!(1, table_names.len());
        assert_eq!(&vec!["employees"], table_names);
    }

    #[test]
    fn execute_describe_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        let query_result = relop.execute("describe table employees").unwrap();
        assert!(query_result.table_descriptor().is_some());

        let table = query_result.table_descriptor().unwrap();

        assert_eq!("employees", table.name());
        assert_eq!(vec!["id"], table.column_names());
        assert!(table.primary_key_column_names().is_none())
    }

    #[test]
    fn execute_invalid_show_tables() {
        let relop = Relop::new(Catalog::new());

        let query_result = relop.execute("show");
        assert!(matches!(
            query_result,
            Err(ClientError::Parse(ParseError::UnexpectedToken{expected, found})) if expected == "tables" && found.is_empty()
        ));
    }

    #[test]
    fn execute_show_tables_with_unsupported_characters() {
        let relop = Relop::new(Catalog::new());

        let query_result = relop.execute("show \\");
        assert!(matches!(
            query_result,
            Err(ClientError::Lex(LexError::UnexpectedCharacter(ch))) if ch == '\\'
        ));
    }

    #[test]
    fn execute_describe_table_for_non_existing_table() {
        let relop = Relop::new(Catalog::new());

        let query_result = relop.execute("describe table employees");
        assert!(matches!(
            query_result,
            Err(ClientError::Execution(ExecutionError::Catalog(CatalogError::TableDoesNotExist(table_name)))) if table_name == "employees"
        ));
    }

    #[test]
    fn execute_select_star() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&relop.catalog, "employees", rows![[1], [2]]);

        let query_result = relop.execute("select * from employees").unwrap();
        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_next_row!(row_iterator.as_mut(), "id" => 2);
    }

    #[test]
    fn execute_select_star_for_non_existing_table() {
        let relop = Relop::new(Catalog::new());

        let query_result = relop.execute("select * from employees");
        assert!(matches!(
            query_result,
            Err(ClientError::Execution(ExecutionError::Catalog(CatalogError::TableDoesNotExist(table_name)))) if table_name == "employees"
        ));
    }

    #[test]
    fn execute_select_with_projection() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&relop.catalog, "employees", rows![[1, 10], [2, 20]]);

        let query_result = relop.execute("select rank from employees").unwrap();
        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "rank" => 10);
        assert_next_row!(row_iterator.as_mut(), "rank" => 20);
    }

    #[test]
    fn attempt_to_execute_select_with_projection_for_non_existing_column() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&relop.catalog, "employees", rows![[1, 10], [2, 20]]);

        let query_result = relop.execute("select unknown from employees");
        assert!(matches!(
            query_result,
            Err(ClientError::Execution(ExecutionError::UnknownColumn(column_name))) if column_name == "unknown"
        ));
    }

    #[test]
    fn execute_select_star_with_where_clause() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"]],
        );

        let query_result = relop
            .execute("select * from employees where id = 1")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_where_clause_greater_than() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"]],
        );
        let query_result = relop
            .execute("select * from employees where id > 1")
            .unwrap();

        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "id" => 2, "name" => "query");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_projection_and_where_clause() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"]],
        );
        let query_result = relop
            .execute("select name from employees where id != 1")
            .unwrap();

        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "name" => "query", ! "id");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_like_clause_matching() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"], [3, "relational"]],
        );

        let query_result = relop
            .execute("select * from employees where name like '^rel.*' order by id")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_next_row!(row_iterator.as_mut(), "id" => 3, "name" => "relational");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_like_clause_not_matching() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"]],
        );

        let query_result = relop
            .execute("select * from employees where name like '^nomatch.*'")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_where_clause_and_match() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"]],
        );

        let query_result = relop
            .execute("select * from employees where id = 1 and name = 'relop'")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_where_clause_using_column_comparison() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["first_name" => ColumnType::Text, "last_name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![["microsoft", "microsoft"], ["relop", "query"]],
        );

        let query_result = relop
            .execute("select * from employees where first_name = last_name")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "first_name" => "microsoft", "last_name" => "microsoft");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_where_clause_and_returning_a_few_results() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"], [3, "relop"]],
        );

        let query_result = relop
            .execute("select * from employees where id >= 1 and name = 'relop' order by id")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_next_row!(row_iterator.as_mut(), "id" => 3, "name" => "relop");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_where_clause_and_no_matching_rows() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"]],
        );

        let query_result = relop
            .execute("select * from employees where id = 3 and name = 'rust'")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_order_by_single_column_ascending() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&relop.catalog, "employees", rows![[2], [1]]);

        let query_result = relop
            .execute("select * from employees order by id ASC")
            .unwrap();

        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_next_row!(row_iterator.as_mut(), "id" => 2);
    }

    #[test]
    fn execute_select_with_order_by_multiple_columns_ascending() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(&relop.catalog, "employees", rows![[1, 20], [1, 10]]);

        let query_result = relop
            .execute("select * from employees order by id ASC, rank DESC")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "rank" => 20);
        assert_next_row!(row_iterator.as_mut(), "id" => 1, "rank" => 10);
    }

    #[test]
    fn execute_select_star_with_limit() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table("employees", schema!["id" => ColumnType::Int].unwrap());
        assert!(result.is_ok());

        insert_rows(&relop.catalog, "employees", rows![[1], [2], [3]]);

        let query_result = relop.execute("select * from employees limit 2").unwrap();
        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_next_row!(row_iterator.as_mut(), "id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_projection_and_limit() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"], [3, "parsing"]],
        );

        let query_result = relop
            .execute("select name, id from employees limit 1")
            .unwrap();

        let result_set = query_result.result_set().unwrap();

        let mut row_iterator = result_set.iterator().unwrap();
        assert_next_row!(row_iterator.as_mut(), "name" => "relop", "id" => 1);
        assert_no_more_rows!(row_iterator.as_mut());
    }
}
