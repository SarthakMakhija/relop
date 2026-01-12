use crate::catalog::error::CatalogError;

/// Represents errors that can occur during query execution.
#[derive(Debug)]
pub enum ExecutionError {
    /// Errors related to catalog operations during execution (e.g., table lookup).
    Catalog(CatalogError),
    /// Error related unknown column during select query execution with projection.
    UnknownColumn(String),
}
