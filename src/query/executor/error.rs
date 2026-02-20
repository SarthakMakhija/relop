use crate::catalog::error::CatalogError;
use crate::storage::error::RowViewComparatorError;

/// Represents errors that can occur during query execution.
#[derive(Debug)]
pub enum ExecutionError {
    /// Errors related to catalog operations during execution (e.g., table lookup).
    Catalog(CatalogError),
    /// Error related unknown column during select query execution with projection.
    UnknownColumn(String),
    /// Error related to mismatch types during execution of comparison operations.
    TypeMismatchInComparison,
    /// Errors related to schema validation during execution.
    Schema(crate::schema::error::SchemaError),
}

impl From<RowViewComparatorError> for ExecutionError {
    fn from(error: RowViewComparatorError) -> Self {
        match error {
            RowViewComparatorError::UnknownColumn(column) => ExecutionError::UnknownColumn(column),
        }
    }
}

impl From<crate::schema::error::SchemaError> for ExecutionError {
    fn from(error: crate::schema::error::SchemaError) -> Self {
        ExecutionError::Schema(error)
    }
}