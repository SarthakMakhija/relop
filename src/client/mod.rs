mod error;

use crate::catalog::Catalog;
use crate::client::error::ClientError;
use crate::query::executor::result::QueryResult;
use crate::query::executor::Executor;
use crate::query::lexer::Lexer;
use crate::query::parser::Parser;
use crate::query::plan::LogicalPlanner;
use crate::schema::Schema;
use crate::storage::batch::Batch;
use crate::storage::row::Row;
use crate::storage::table_store::RowId;

pub(crate) struct Relop {
    catalog: Catalog,
}

impl Relop {
    pub(crate) fn new(catalog: Catalog) -> Relop {
        Self { catalog }
    }

    pub fn create_table(&self, table_name: &str, schema: Schema) -> Result<(), ClientError> {
        self.catalog
            .create_table(table_name, schema)
            .map_err(ClientError::Catalog)
    }

    pub fn insert_into(&self, table_name: &str, row: Row) -> Result<RowId, ClientError> {
        self.catalog
            .insert_into(table_name, row)
            .map_err(ClientError::Insert)
    }

    pub fn insert_all_into(
        &self,
        table_name: &str,
        batch: impl Into<Batch>,
    ) -> Result<Vec<RowId>, ClientError> {
        self.catalog
            .insert_all_into(table_name, batch)
            .map_err(ClientError::Insert)
    }

    pub fn execute(&self, query: &str) -> Result<QueryResult, ClientError> {
        let mut lexer = Lexer::new_with_default_keywords(query);
        let tokens = lexer.lex().map_err(ClientError::Lex)?;

        let mut parser = Parser::new(tokens);
        let ast = parser.parse().map_err(ClientError::Parse)?;

        let plan = LogicalPlanner::plan(ast);

        let executor = Executor::new(&self.catalog);
        executor.execute(plan).map_err(ClientError::Execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::{CatalogError, InsertError};
    use crate::query::executor::error::ExecutionError;
    use crate::query::lexer::error::LexError;
    use crate::query::parser::error::ParseError;
    use crate::schema::primary_key::PrimaryKey;
    use crate::types::column_type::ColumnType;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn create_table() {
        let result = Relop::new(Catalog::new()).create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn attempt_to_create_an_already_created_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let result = relop.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(ClientError::Catalog(CatalogError::TableAlreadyExists(table_name))) if table_name == "employees"
        ))
    }

    #[test]
    fn insert_into_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let row_id = relop
            .insert_into("employees", Row::filled(vec![ColumnValue::Int(1)]))
            .unwrap();

        let row = relop.catalog.get("employees", row_id).unwrap().unwrap();
        let expected_row = Row::filled(vec![ColumnValue::Int(1)]);

        assert_eq!(expected_row, row);
    }

    #[test]
    fn attempt_to_insert_duplicate_primary_key() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        );
        assert!(result.is_ok());

        let row_id = relop
            .insert_into("employees", Row::filled(vec![ColumnValue::Int(1)]))
            .unwrap();

        let row = relop.catalog.get("employees", row_id).unwrap().unwrap();
        let expected_row = Row::filled(vec![ColumnValue::Int(1)]);
        assert_eq!(expected_row, row);

        let result = relop.insert_into("employees", Row::filled(vec![ColumnValue::Int(1)]));

        assert!(matches!(
            result,
            Err(ClientError::Insert(InsertError::DuplicatePrimaryKey))
        ));
    }

    #[test]
    fn insert_all_into_table() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let row_ids = relop
            .insert_all_into(
                "employees",
                vec![
                    Row::filled(vec![ColumnValue::Int(1)]),
                    Row::filled(vec![ColumnValue::Int(2)]),
                ],
            )
            .unwrap();

        let row = relop
            .catalog
            .get("employees", *row_ids.first().unwrap())
            .unwrap()
            .unwrap();

        let expected_row = Row::filled(vec![ColumnValue::Int(1)]);
        assert_eq!(expected_row, row);

        let row = relop
            .catalog
            .get("employees", *row_ids.last().unwrap())
            .unwrap()
            .unwrap();

        let expected_row = Row::filled(vec![ColumnValue::Int(2)]);
        assert_eq!(expected_row, row);
    }

    #[test]
    fn execute_show_tables() {
        let relop = Relop::new(Catalog::new());
        let result = relop.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
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
        let result = relop.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let query_result = relop.execute("describe table employees").unwrap();
        assert!(query_result.table_descriptor().is_some());

        let table_descriptor = query_result.table_descriptor().unwrap();

        assert_eq!("employees", table_descriptor.table_name());
        assert_eq!(vec!["id"], table_descriptor.column_names());
        assert!(table_descriptor.primary_key_column_names().is_none())
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
}
