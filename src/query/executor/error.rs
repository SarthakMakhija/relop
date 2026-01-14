use crate::catalog::error::CatalogError;
use crate::storage::error::RowViewComparatorError;

/// Represents errors that can occur during query execution.
#[derive(Debug)]
pub enum ExecutionError {
    /// Errors related to catalog operations during execution (e.g., table lookup).
    Catalog(CatalogError),
    /// Error related unknown column during select query execution with projection.
    UnknownColumn(String),
}

impl From<RowViewComparatorError> for ExecutionError {
    fn from(error: RowViewComparatorError) -> Self {
        match error {
            RowViewComparatorError::UnknownColumn(column) => ExecutionError::UnknownColumn(column),
        }
    }
}
