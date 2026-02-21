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
    ///  assert_eq!(&ColumnValue::int(1), row_view.column_value_by("id").unwrap().unwrap());
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
    use crate::catalog::error::CatalogError;
    use crate::query::executor::error::ExecutionError;
    use crate::query::lexer::error::LexError;
    use crate::query::parser::error::ParseError;
    use crate::row;
    use crate::rows;
    use crate::test_utils::insert_rows;
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
    fn execute_select_star_with_where_clause_no_results() {
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
            .execute("select * from employees where id = 100")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();
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
    fn execute_select_star_with_where_clause_using_literal_comparison() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["first_name" => ColumnType::Text, "last_name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![["microsoft", "microsoft"]],
        );

        let query_result = relop
            .execute("select * from employees where 1 = 1")
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
    fn execute_select_star_with_where_clause_or_match() {
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
            .execute("select * from employees where id = 1 or name = 'query' order by id")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_next_row!(row_iterator.as_mut(), "id" => 2, "name" => "query");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_star_with_where_clause_multiple_or_match() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
        );
        assert!(result.is_ok());

        insert_rows(
            &relop.catalog,
            "employees",
            rows![[1, "relop"], [2, "query"], [3, "rust"]],
        );

        let query_result = relop
            .execute("select * from employees where id = 1 or id = 3 or name = 'nonexistent' order by id")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "relop");
        assert_next_row!(row_iterator.as_mut(), "id" => 3, "name" => "rust");
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

    #[test]
    fn execute_select_with_table_alias() {
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
            .execute("select * from employees as emp where emp.id = 1")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "emp.id" => 1, "emp.name" => "relop");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_table_alias_and_qualified_projection() {
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
            .execute("select emp.name from employees as emp where emp.id = 2")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "emp.name" => "query");
        assert_no_more_rows!(row_iterator.as_mut());
    }
}

#[cfg(test)]
mod conjunction_tests {
    use super::*;
    use crate::rows;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, schema};

    #[test]
    fn execute_select_with_and_and_or() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text, "city" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into(
                "employees",
                rows![
                    [1, "Alice", "London"],
                    [2, "Bob", "Paris"],
                    [3, "Charlie", "London"]
                ],
            )
            .unwrap();

        let query_result = relop
            .execute("select * from employees where city = 'London' and id = 1 or city = 'Paris' order by id")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "Alice");
        assert_next_row!(row_iterator.as_mut(), "id" => 2, "name" => "Bob");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_precedence_and_or_1() {
        // A or B and C => A or (B and C)
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text, "city" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into(
                "employees",
                rows![
                    [1, "Alice", "London"],
                    [2, "Bob", "Paris"],
                    [3, "Charlie", "London"]
                ],
            )
            .unwrap();

        // id = 1 or (name = 'Bob' and city = 'Paris')
        let query_result = relop
            .execute("select * from employees where id = 1 or name = 'Bob' and city = 'Paris' order by id")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_next_row!(row_iterator.as_mut(), "id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_precedence_and_or_2() {
        // A and B or C => (A and B) or C
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text, "city" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into(
                "employees",
                rows![
                    [1, "Alice", "London"],
                    [2, "Bob", "Paris"],
                    [3, "Charlie", "London"]
                ],
            )
            .unwrap();

        // (id = 1 and city = 'London') or name = 'Bob'
        let query_result = relop
            .execute("select * from employees where id = 1 and city = 'London' or name = 'Bob' order by id")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_next_row!(row_iterator.as_mut(), "id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_trailing_or_error() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        let query_result = relop.execute("select * from employees where id = 1 or");
        assert!(matches!(
            query_result,
            Err(ClientError::Parse(
                crate::query::parser::error::ParseError::UnexpectedToken { .. }
            ))
        ));
    }

    #[test]
    fn execute_select_with_missing_clause_after_or_error() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        let query_result = relop.execute("select * from employees where id = 1 or ;");
        assert!(matches!(
            query_result,
            Err(ClientError::Parse(
                crate::query::parser::error::ParseError::UnexpectedToken { .. }
            ))
        ));
    }
}

#[cfg(test)]
mod parentheses_tests {
    use crate::catalog::Catalog;
    use crate::client::Relop;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, assert_no_more_rows, rows, schema};

    #[test]
    fn execute_select_with_parentheses_1() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text, "city" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into(
                "employees",
                rows![
                    [1, "Alice", "London"],
                    [2, "Bob", "Paris"],
                    [3, "Charlie", "London"]
                ],
            )
            .unwrap();

        let query_result = relop
            .execute("select * from employees where (name = 'Alice' or name = 'Bob') and city = 'London'")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "Alice");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_parentheses_2() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text, "city" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into(
                "employees",
                rows![
                    [1, "Alice", "London"],
                    [2, "Bob", "Paris"],
                    [3, "Charlie", "London"]
                ],
            )
            .unwrap();

        let query_result = relop
            .execute("select * from employees where (name = 'Alice' or name = 'Bob') and (city = 'London' or city = 'Paris') order by id")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1, "name" => "Alice");
        assert_next_row!(row_iterator.as_mut(), "id" => 2, "name" => "Bob");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_nested_parentheses() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        relop.insert_all_into("employees", rows![[1]]).unwrap();

        let query_result = relop
            .execute("select * from employees where ((id = 1))")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "id" => 1);
        assert_no_more_rows!(row_iterator.as_mut());
    }
}

#[cfg(test)]
mod join_tests {
    use super::*;
    use crate::assert_no_more_rows;
    use crate::row;
    use crate::rows;
    use crate::types::column_type::ColumnType;
    use crate::{assert_next_row, schema};

    #[test]
    fn execute_select_with_join() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop.insert_all_into("employees", rows![[1], [2]]).unwrap();
        relop
            .insert_all_into("departments", rows![[1, "Engineering"], [3, "Marketing"]])
            .unwrap();

        let query_result = relop
            .execute("select * from employees join departments on employees.id = departments.id")
            .unwrap();

        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1, "departments.id" => 1, "departments.name" => "Engineering");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_multi_table_join() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        relop
            .create_table("departments", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        relop
            .create_table("locations", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        relop.insert_all_into("employees", rows![[1], [2]]).unwrap();
        relop
            .insert_all_into("departments", rows![[1], [3]])
            .unwrap();
        relop.insert_all_into("locations", rows![[1], [4]]).unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.id = departments.id join locations on departments.id = locations.id")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_self_join_and_aliases() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
            )
            .unwrap();
        relop
            .insert_all_into("employees", rows![[1, "Relop"], [2, "Query"]])
            .unwrap();

        let query_result = relop
            .execute(
                "select e1.name, e2.name from employees as e1 join employees as e2 on e1.id = e2.id order by e1.id",
            )
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "e1.name" => "Relop", "e2.name" => "Relop");
        assert_next_row!(row_iterator.as_mut(), "e1.name" => "Query", "e2.name" => "Query");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_and_projection() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
            )
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "dept_name" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop.insert_into("employees", row![1, "Alice"]).unwrap();
        relop
            .insert_into("departments", row![1, "Engineering"])
            .unwrap();

        let query_result = relop
            .execute("select employees.name, departments.dept_name from employees join departments on employees.id = departments.id")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.name" => "Alice", "departments.dept_name" => "Engineering");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_and_where() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "dept_id" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 10], [2, 20]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[10, "Sales"], [20, "HR"]])
            .unwrap();

        let query_result = relop
            .execute("select departments.name from employees join departments on employees.dept_id = departments.id where employees.id = 2")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "departments.name" => "HR");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_and_order_by() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table("employees", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop.insert_all_into("employees", rows![[1], [2]]).unwrap();
        relop
            .insert_all_into("departments", rows![[1, "Dev"], [2, "Ops"]])
            .unwrap();

        let query_result = relop
            .execute("select departments.name from employees join departments on employees.id = departments.id order by departments.name DESC")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "departments.name" => "Ops");
        assert_next_row!(row_iterator.as_mut(), "departments.name" => "Dev");
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_on_with_or() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "active" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table("departments", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 0], [2, 1]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[1], [3]])
            .unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.id = departments.id OR employees.active = 1")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_and_where_or() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "active" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "location" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 1], [2, 0]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[1, "NY"], [2, "SF"]])
            .unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.id = departments.id where employees.active = 1 OR departments.location = 'SF'")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_on_mixing_and_or() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "active" => ColumnType::Int, "dept_id" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table("departments", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 1, 10], [2, 0, 20], [3, 1, 10]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[10], [20]])
            .unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.id = departments.id AND employees.active = 1 OR employees.dept_id = departments.id")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 3);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_where_mixing_and_or() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "active" => ColumnType::Int, "dept_id" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "loc" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 1, 10], [2, 0, 20], [3, 1, 10]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[10, "NY"], [20, "SF"]])
            .unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.dept_id = departments.id where employees.active = 1 AND departments.loc = 'NY' OR employees.id = 2")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2);
        assert_next_row!(row_iterator.as_mut(), "employees.id" => 3);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_on_with_and() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "active" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table("departments", schema!["id" => ColumnType::Int].unwrap())
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 1], [2, 0]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[1], [2]])
            .unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.id = departments.id and employees.active = 1")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 1);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_on_and_where() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "active" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "loc" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into("employees", rows![[1, 1], [2, 1]])
            .unwrap();
        relop
            .insert_all_into("departments", rows![[1, "NY"], [2, "SF"]])
            .unwrap();

        let query_result = relop
            .execute("select employees.id from employees join departments on employees.id = departments.id and employees.active = 1 where departments.loc = 'SF'")
            .unwrap();
        let mut row_iterator = query_result.result_set().unwrap().iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.id" => 2);
        assert_no_more_rows!(row_iterator.as_mut());
    }

    #[test]
    fn execute_select_with_join_and_parentheses_in_where() {
        let relop = Relop::new(Catalog::new());
        relop
            .create_table(
                "employees",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text, "dept_id" => ColumnType::Int].unwrap(),
            )
            .unwrap();
        relop
            .create_table(
                "departments",
                schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap(),
            )
            .unwrap();

        relop
            .insert_all_into(
                "employees",
                rows![[1, "Alice", 10], [2, "Bob", 10], [3, "Charlie", 20]],
            )
            .unwrap();
        relop
            .insert_all_into("departments", rows![[10, "Engineering"], [20, "Sales"]])
            .unwrap();

        let query_result = relop
            .execute("select employees.name from employees join departments on employees.dept_id = departments.id where (employees.name = 'Alice' or employees.name = 'Bob') and departments.name = 'Engineering' order by employees.name")
            .unwrap();
        let result_set = query_result.result_set().unwrap();
        let mut row_iterator = result_set.iterator().unwrap();

        assert_next_row!(row_iterator.as_mut(), "employees.name" => "Alice");
        assert_next_row!(row_iterator.as_mut(), "employees.name" => "Bob");
        assert_no_more_rows!(row_iterator.as_mut());
    }
}
