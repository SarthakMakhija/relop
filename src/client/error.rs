use crate::catalog::error::{CatalogError, InsertError};
use crate::query::executor::error::ExecutionError;
use crate::query::lexer::error::LexError;
use crate::query::parser::error::ParseError;

/// Represents the various errors that can occur when using the `Relop` client.
#[derive(Debug)]
pub enum ClientError {
    /// Errors related to catalog operations (e.g., table creation, lookup).
    Catalog(CatalogError),
    /// Errors related to data insertion (e.g., type mismatch, duplicate key).
    Insert(InsertError),
    /// Errors related to lexical analysis of the query string.
    Lex(LexError),
    /// Errors related to parsing the query tokens into an AST.
    Parse(ParseError),
    /// Errors related to executing the logical plan.
    Execution(ExecutionError),
}
