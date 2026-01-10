use crate::catalog::error::{CatalogError, InsertError};
use crate::query::executor::error::ExecutionError;
use crate::query::lexer::error::LexError;
use crate::query::parser::error::ParseError;

#[derive(Debug)]
pub enum ClientError {
    Catalog(CatalogError),
    Insert(InsertError),
    Lex(LexError),
    Parse(ParseError),
    Execution(ExecutionError),
}
